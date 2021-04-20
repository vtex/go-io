package event

type EventSource <-chan Event

type Event struct {
	Type string
	Data interface{}
}

func NewEvent(data interface{}) Event {
	return Event{Data: data}
}

func NewTypedEvent(_type string, data interface{}) Event {
	return Event{Type: _type, Data: data}
}
