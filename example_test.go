package event

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cognicraft/uuid"
)

// UserID is used to identify the User entity.
type UserID string

// Created must be the very first event in a User entity lifecycle.
type Created struct {
	ID         string    // The id of the event (useful for deduplication purposes).
	OccurredOn time.Time // The point in time when this event occurred.
	User       UserID    // The id of the new User entity lifecycle.
}

// NameChanged will always be generated when a Users name changes.
type NameChanged struct {
	ID         string    // The id of the envent (useful for deduplication purposes).
	OccurredOn time.Time // The point in time when this event occurred.
	User       UserID    // The id of the User entity lifecycle.
	Name       string    // The new name of the User going forward.
}

// NewUser creates a User entity.
func NewUser() *User {
	return &User{
		ChangeRecorder: NewChangeRecorder(),
	}
}

// User is an event sourced entity.
type User struct {
	ID              UserID // The id of the User entity.
	Version         uint64 // The current version of the User entity.
	Name            string // The current name of the User entity.
	*ChangeRecorder        // The ChangeRecorder is embeded to track changes as a uint of work.
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
func (u *User) Apply(e Event) {
	u.Record(e) // New Events need to be recorded to enable a unit of work for later storage in an event store.
	u.Mutate(e) // Based on the new Event the user model must mutate.
}

// Mutate should be called for each event in the users history.
func (u *User) Mutate(e Event) {
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
func UserCodec() *Codec {
	c := NewCodec()
	c.Register("user:created", Created{})
	c.Register("user:name-changed", NameChanged{})
	return c
}

// Save can be used to dehydrate a User into an event store.
func Save(store *Store, user *User) error {
	// If there are no new changes nothing needs to be stored. The unit of work is empty.
	if len(user.Changes()) == 0 {
		return nil
	}
	streamID := string(user.ID) // Each user will have its own stream within the event store.
	// Each applied event will increase the version of a user.
	// The version of the user that was originally loaded can be calculated.
	// The result is the expected version (length) of the event stream currently stored.
	ex := user.Version - uint64(len(user.Changes()))
	codec := UserCodec()                         // We will use this codec to marshal the events as event records
	recs, err := codec.EncodeAll(user.Changes()) // Encode all events in the unit of work.
	if err != nil {
		return err
	}
	err = store.Append(streamID, ex, recs) // Append all records to the event stream.
	if err != nil {
		return err
	}
	user.ClearChanges() // All events in the unit of work have ben saved to the event store. We can clear all changes.
	return nil
}

// Load can be used to rehydrate a User from an event store.
func Load(store *Store, uID UserID) (*User, error) {
	codec := UserCodec()    // We will use this codec to unmarshal event records to domain events.
	user := NewUser()       // Create an empty user
	streamID := string(uID) // Each user will have its own stream within the event store.
	// Mutate the user for each event stored in history.
	// We are using the streaming version sice the number of events in all of histoy for this user
	// could be quite large (and not fit into memory).
	for rec := range store.Load(streamID) {
		evt, err := codec.Decode(rec) // unmarshal an event record
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

func TestUser(t *testing.T) {
	var err error
	u := NewUser()
	if u.ID != "" {
		t.Errorf("want: %v, got: %v", "", u.ID)
	}
	if u.Version != 0 {
		t.Errorf("want: %v, got: %v", 0, u.Version)
	}

	err = u.ChangeName("")
	if err == nil {
		t.Errorf("expected an error")
	}

	err = u.Create("user-1")
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
	if u.ID != "user-1" {
		t.Errorf("want: %v, got: %v", "user-1", u.ID)
	}
	if u.Version != 1 {
		t.Errorf("want: %v, got: %v", 1, u.Version)
	}
	if n := len(u.Changes()); n != 1 {
		t.Errorf("want: %v, got: %v", 1, n)
	}

	err = u.Create("e")
	if err == nil {
		t.Errorf("expected an error")
	}
	if u.ID != "user-1" {
		t.Errorf("want: %v, got: %v", "user-1", u.ID)
	}
	if u.Version != 1 {
		t.Errorf("want: %v, got: %v", 1, u.Version)
	}
	if n := len(u.Changes()); n != 1 {
		t.Errorf("want: %v, got: %v", 1, n)
	}

	err = u.ChangeName("")
	if err == nil {
		t.Errorf("expected an error")
	}
	err = u.ChangeName("UserA")
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
	if u.ID != "user-1" {
		t.Errorf("want: %v, got: %v", "user-1", u.ID)
	}
	if u.Version != 2 {
		t.Errorf("want: %v, got: %v", 2, u.Version)
	}
	if u.Name != "UserA" {
		t.Errorf("want: %v, got: %v", "UserA", u.Name)
	}
	if n := len(u.Changes()); n != 2 {
		t.Errorf("want: %v, got: %v", 2, n)
	}

	store, _ := NewStore(":memory:")
	err = Save(store, u)
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
	if n := len(u.Changes()); n != 0 {
		t.Errorf("want: %v, got: %v", 0, n)
	}

	var lu *User
	lu, err = Load(store, "e")
	if err == nil {
		t.Errorf("expected an error, since user e should not exist")
	}
	if lu != nil {
		t.Errorf("expected a nil user: %v", lu)
	}
	lu, err = Load(store, "user-1")
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
	if !reflect.DeepEqual(u, lu) {
		t.Errorf("expected original to be equal to loaded")
	}
}
