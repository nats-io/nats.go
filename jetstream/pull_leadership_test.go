package jetstream

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestPullSubscriptionLeadershipChangeRequestsNextPull(t *testing.T) {
	consumeOpts, err := parseConsumeOpts(false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub := &pullSubscription{
		fetchNext:   make(chan *pullRequest, 1),
		consumeOpts: consumeOpts,
	}
	sub.fetchInProgress.Store(1)

	msg := &nats.Msg{
		Header: nats.Header{
			"Status":      []string{statusConflict},
			"Description": []string{leadershipChange},
		},
	}

	sub.Lock()
	termErr, notifyErr := sub.handleStatusMsg(msg, ErrConsumerLeadershipChanged)
	sub.Unlock()

	if termErr != nil {
		t.Fatalf("Expected non-terminal error, got %v", termErr)
	}
	if !errors.Is(notifyErr, ErrConsumerLeadershipChanged) {
		t.Fatalf("Expected %v, got %v", ErrConsumerLeadershipChanged, notifyErr)
	}
	if sub.fetchInProgress.Load() != 0 {
		t.Fatalf("Expected fetchInProgress to be cleared")
	}

	select {
	case req := <-sub.fetchNext:
		if req.Batch != consumeOpts.MaxMessages {
			t.Fatalf("Expected batch size %d, got %d", consumeOpts.MaxMessages, req.Batch)
		}
		if sub.pending.msgCount != consumeOpts.MaxMessages {
			t.Fatalf("Expected pending message count %d, got %d", consumeOpts.MaxMessages, sub.pending.msgCount)
		}
	default:
		t.Fatal("Expected a new pull request after leadership change")
	}
}

func TestPullSubscriptionRequestNextPullResetsPendingWhenRequestAlreadyQueued(t *testing.T) {
	consumeOpts, err := parseConsumeOpts(false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub := &pullSubscription{
		fetchNext:   make(chan *pullRequest, 1),
		consumeOpts: consumeOpts,
	}
	sub.fetchNext <- &pullRequest{}

	sub.Lock()
	sub.requestNextPullLocked()
	sub.Unlock()

	if sub.pending.msgCount != consumeOpts.MaxMessages {
		t.Fatalf("Expected pending message count %d, got %d", consumeOpts.MaxMessages, sub.pending.msgCount)
	}
	if len(sub.fetchNext) != 1 {
		t.Fatalf("Expected queued pull request to be preserved, got queue length %d", len(sub.fetchNext))
	}
}

func TestCheckMsgLeadershipChange(t *testing.T) {
	msg := &nats.Msg{
		Header: nats.Header{
			"Status":      []string{statusConflict},
			"Description": []string{leadershipChange},
		},
	}

	userMsg, err := checkMsg(msg)
	if userMsg {
		t.Fatal("Expected leadership change status message")
	}
	if !errors.Is(err, ErrConsumerLeadershipChanged) {
		t.Fatalf("Expected %v, got %v", ErrConsumerLeadershipChanged, err)
	}
}
