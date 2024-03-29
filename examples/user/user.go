package user

import (
	"fmt"
	"sync"
	"time"

	"github.com/cognicraft/event"
	"github.com/cognicraft/uuid"
)

// This example will use the different concepts provided by this package in combination
// to enable an event sourced system. For this we will track a users lifecycle.

// UserID is used to identify the User entity.
type UserID string

// Created must be the very first event in a User entity lifecycle.
type Created struct {
	ID         string    `json:"id"`          // The id of the event (useful for deduplication purposes).
	OccurredOn time.Time `json:"occurred-on"` // The point in time when this event occurred.
	User       UserID    `json:"user"`        // The id of the new User entity lifecycle.
}

// NameChanged will always be generated when a Users name changes.
type NameChanged struct {
	ID         string    `json:"id"`          // The id of the envent (useful for deduplication purposes).
	OccurredOn time.Time `json:"occurred-on"` // The point in time when this event occurred.
	User       UserID    `json:"user"`        // The id of the User entity lifecycle.
	Name       string    `json:"name"`        // The new name of the User going forward.
}

// NewUser creates a User entity.
func NewUser() *User {
	return &User{
		ChangeRecorder: event.NewChangeRecorder(),
	}
}

// User is an event sourced entity.
type User struct {
	ID                    UserID // The id of the User entity.
	Version               uint64 // The current version of the User entity.
	Name                  string // The current name of the User entity.
	*event.ChangeRecorder        // The ChangeRecorder is embeded to track changes as a unit of work.
}

// Create is a command that should be called first in the lifecycle of a User entity.
func (u *User) Create(id UserID) error {
	// First check all business rules:
	// 1. If the user was already initialized we will return an error.
	if u.ID != "" {
		return fmt.Errorf("user already initialized")
	}
	// 2. If the specified id is empty we will return an error.
	if id == "" {
		return fmt.Errorf("a users id may not be empty")
	}
	// All business rules have been evaluated, we can now generate and apply the Created
	// event as a new fact to the User entity.
	u.Apply(Created{
		ID:         uuid.MakeV4(),
		OccurredOn: time.Now().UTC(),
		User:       id,
	})
	// At this point the command was successfully executed.
	return nil
}

// ChangeName is a command that should be called any time this users name should change.
func (u *User) ChangeName(name string) error {
	// First check all business rules:
	// 1. If the user has not been initialized yet we will return an error.
	if u.ID == "" {
		return fmt.Errorf("user has not been initialized")
	}
	// 2. If the new name is empty we will return an error.
	if name == "" {
		return fmt.Errorf("a users name may not be empty")
	}
	// 3. If the new name is equal to the current name there is no change needed.
	if u.Name == name {
		return nil
	}
	// All business rules have been evaluated, we can now generate and apply the NameChanged
	// event as a new fact to the User entity.
	u.Apply(NameChanged{
		ID:         uuid.MakeV4(),
		OccurredOn: time.Now().UTC(),
		User:       u.ID,
		Name:       name,
	})
	// At this point the command was successfully executed.
	return nil
}

// Apply should be called to apply a new Event to the User entity model.
func (u *User) Apply(e event.Event) {
	u.Record(e) // New Events need to be recorded to enable a unit of work for later storage in an event store.
	u.Mutate(e) // Based on the new Event the user model must mutate.
}

// Mutate should be called for each event in the users history.
func (u *User) Mutate(e event.Event) {
	u.Version++ // Any event will increase the users version number by 1.

	// Each event will have some values that are usefull for business rule validation.
	switch e := e.(type) {
	case Created:
		// Created will set the Users ID which is the first business rule of any command that is not Create(...)
		u.ID = e.User
	case NameChanged:
		// A users name is used for business rule 3. in the NameChanged(...) command to not create superfluous events.
		u.Name = e.Name
	}
}

// UserCodec is used for un-/marshaling puroposes.
func UserCodec() *event.Codec {
	c := event.NewCodec()
	c.Register("user:created", Created{})
	c.Register("user:name-changed", NameChanged{})
	return c
}

// Save can be used to dehydrate a User into an event store.
func Save(store event.Store, user *User, metadata interface{}) error {
	// If there are no new changes nothing needs to be stored. The unit of work is empty.
	if len(user.Changes()) == 0 {
		return nil
	}
	// Each user will have its own stream within the event store.
	streamID := string(user.ID)
	// Each applied event will increase the version of a user.
	// The version of the user that was originally loaded can be calculated.
	// The result is the expected version (length) of the event stream currently stored.
	ex := user.Version - uint64(len(user.Changes()))
	// We will use this codec to marshal the events as event records
	codec := UserCodec()
	// Encode all events in the unit of work.
	recs, err := codec.EncodeAll(user.Changes(), event.WithMetadata(metadata))
	if err != nil {
		return err
	}
	// Append all records to the event stream.
	err = store.Append(streamID, ex, recs)
	if err != nil {
		return err
	}
	// All events in the unit of work have been saved to the event store. We can clear all changes.
	user.ClearChanges()
	return nil
}

// Load can be used to rehydrate a User from an event store.
func Load(store event.Store, uID UserID) (*User, error) {
	// We will use this codec to unmarshal event records to domain events.
	codec := UserCodec()
	// Create an empty user
	user := NewUser()
	// Each user will have its own stream within the event store.
	streamID := string(uID)
	// Mutate the user for each event stored in history.
	// We are using the streaming version sice the number of events in all of histoy for this user
	// could be quite large (and not fit into memory).
	for rec := range store.Load(streamID) {
		// unmarshal an event record
		evt, err := codec.Decode(rec)
		if err != nil {
			return nil, err
		}
		// Mutate the user entity. Since these events have already been saved they are not changes
		// that need to be tracked by the unit of work. Do not use Apply(...) here!
		user.Mutate(evt)
	}
	// If the user ID is still empty then the stream was empty which would mean a user
	// with the given uID does not exist.
	if user.ID == "" {
		return nil, fmt.Errorf("not found")
	}
	// The user is now fully rehydrated.
	return user, nil
}

// NewProjection creates a new Projection. In a production system this should probably be something persistent.
func NewProjection() *Projection {
	return &Projection{
		userNames:                  map[UserID]string{},
		numberOfNameChangesPerUser: map[UserID]int{},
	}
}

// Projection can answer some questions about the user domain.
type Projection struct {
	mu                         sync.RWMutex
	userNames                  map[UserID]string
	numberOfNameChangesPerUser map[UserID]int
	totalNumberOfNameChanges   int
}

// On should be called for each event in history.
func (p *Projection) On(rec event.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	codec := UserCodec()          // We will use this codec to unmarshal event records to domain events.
	evt, err := codec.Decode(rec) // unmarshal an event record
	if err != nil {
		return
	}
	// extract relevant information from the domain events
	switch e := evt.(type) {
	case Created:
		// for our current projection we can ignore this event
	case NameChanged:
		// record the current name of a user
		p.userNames[e.User] = e.Name

		// increment the number of name changes per user
		n := p.numberOfNameChangesPerUser[e.User]
		n++
		p.numberOfNameChangesPerUser[e.User] = n

		// increment the total number of name changes
		p.totalNumberOfNameChanges++
	}
}

// UserName will retrieve a users current name.
func (p *Projection) UserName(id UserID) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n, _ := p.userNames[id]
	return n
}

// IsUserNameInUse will check if the provided name is currently in use by a user.
func (p *Projection) IsUserNameInUse(name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, n := range p.userNames {
		if name == n {
			return true
		}
	}
	return false
}

// NumberOfNameChangesForUser will retrieve the number of name changes in history for a given user
func (p *Projection) NumberOfNameChangesForUser(id UserID) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n, _ := p.numberOfNameChangesPerUser[id]
	return n
}

// TotalNumberOfNameChanges will retrieve the total number of name changes in history
func (p *Projection) TotalNumberOfNameChanges() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalNumberOfNameChanges
}

type Metadata struct {
	UserName    string `json:"user-name,omitempty"`
	Causation   string `json:"causation,omitempty"`
	Correlation string `json:"correlation,omitempty"`
}
