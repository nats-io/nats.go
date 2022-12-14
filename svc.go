package nats

import (
	"github.com/nats-io/nats.go/svc"
)

// AddService takes a service configuration and returns a Service.
func (nc *Conn) AddService(conf svc.Config) (svc.Service, error) {
	return svc.Add(&svcClient{nc}, conf)
}

// svcClient implements the interface required by svc package to
// be able to create services.
type svcClient struct {
	nc *Conn
}

// serviceMsg implements the required interfaces for services.
type serviceMsg struct {
	m *Msg
}

func (msg *serviceMsg) Data() []byte {
	return msg.m.Data
}

func (msg *serviceMsg) Reply() string {
	return msg.m.Reply
}

func (msg *serviceMsg) Respond(payload []byte) error {
	return msg.m.Respond(payload)
}

func (msg *serviceMsg) RespondMsg(smsg svc.Msg) error {
	nmsg := &Msg{
		// Fill the rest
		Data: smsg.Data(),
	}
	return msg.m.RespondMsg(nmsg)
}

// serviceSub implements the interfaces required for service package.
type serviceSub struct {
	s *Subscription
}

func (sub *serviceSub) Drain() error {
	return sub.s.Drain()
}

func (sub *serviceSub) Unsubscribe() error {
	return sub.s.Unsubscribe()
}

// QueueSubscribe creates an queue subscription used to create services.
func (ec *svcClient) QueueSubscribe(subj, queue string, cb svc.MsgHandler) (svc.Subscription, error) {
	nsub, err := ec.nc.QueueSubscribe(subj, queue, func(m *Msg) {
		cb(&serviceMsg{m})
	})
	if err != nil {
		return nil, err
	}
	return &serviceSub{nsub}, nil
}
