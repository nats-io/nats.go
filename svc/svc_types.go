package svc

// Collection of small interfaces that represent common behavior
// from the core NATS client.

type Subscription interface {
	Unsubscribe() error
	Drain() error
}

type MsgHandler func(Msg)

type Msg interface {
	Reply() string
	Data() []byte
	Respond([]byte) error
	RespondMsg(Msg) error
}
