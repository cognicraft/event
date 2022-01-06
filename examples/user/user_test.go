package user

import (
	"reflect"
	"testing"
	"time"

	"github.com/cognicraft/event"
)

func TestEventSourcedSystem(t *testing.T) {
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
	err = u.ChangeName("User-1")
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
	if u.ID != "user-1" {
		t.Errorf("want: %v, got: %v", "user-1", u.ID)
	}
	if u.Version != 2 {
		t.Errorf("want: %v, got: %v", 2, u.Version)
	}
	if u.Name != "User-1" {
		t.Errorf("want: %v, got: %v", "User-1", u.Name)
	}
	if n := len(u.Changes()); n != 2 {
		t.Errorf("want: %v, got: %v", 2, n)
	}

	store, _ := event.NewBasicStore(":memory:")
	defer store.Close()

	projection := NewProjection()
	sub := store.SubscribeToStream(event.All)
	defer sub.Cancel()
	sub.On(projection.On)

	err = Save(store, u, Metadata{UserName: "admin", Correlation: "transaction:1"})
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

	// give the projection time to become consistent
	time.Sleep(100 * time.Millisecond)

	if projection.IsUserNameInUse("e") {
		t.Errorf("expected %v to not be in use", "e")
	}
	if !projection.IsUserNameInUse("User-1") {
		t.Errorf("expected %v to be in use", "User-1")
	}
	if n := projection.UserName("user-1"); n != "User-1" {
		t.Errorf("want: %s, got: %s", "User-1", n)
	}
	if n := projection.NumberOfNameChangesForUser("user-1"); n != 1 {
		t.Errorf("want: %v, got: %v", 1, n)
	}
	if n := projection.TotalNumberOfNameChanges(); n != 1 {
		t.Errorf("want: %v, got: %v", 1, n)
	}

	u2 := NewUser()
	u2.Create("user-2")
	u2.ChangeName("False Name")
	u2.ChangeName("User2")
	u2.ChangeName("User-2")

	err = Save(store, u2, Metadata{UserName: "admin", Correlation: "transaction:2"})
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}

	// give the projection time to become consistent
	time.Sleep(100 * time.Millisecond)

	if projection.IsUserNameInUse("False Name") {
		t.Errorf("expected %v to not be in use", "False Name")
	}
	if projection.IsUserNameInUse("User2") {
		t.Errorf("expected %v to not be in use", "User2")
	}
	if !projection.IsUserNameInUse("User-2") {
		t.Errorf("expected %v to be in use", "User-2")
	}
	if n := projection.UserName("user-2"); n != "User-2" {
		t.Errorf("want: %s, got: %s", "User-2", n)
	}
	if n := projection.NumberOfNameChangesForUser("user-2"); n != 3 {
		t.Errorf("want: %v, got: %v", 3, n)
	}
	if n := projection.TotalNumberOfNameChanges(); n != 4 {
		t.Errorf("want: %v, got: %v", 4, n)
	}
}
