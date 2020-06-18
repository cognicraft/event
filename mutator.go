package event

type Mutator interface {
	Mutate(e Event)
}
