package main

import (
	"net/http"

	"github.com/cognicraft/event"
)

func main() {
	store, _ := event.NewStore("test.db")
	http.Handle("/", http.HandlerFunc(event.HandleGETStream(store, event.All)))
	http.ListenAndServe(":8888", nil)
}
