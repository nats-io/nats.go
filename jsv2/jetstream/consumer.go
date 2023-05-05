// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type (

	// Consumer contains methods for fetching/processing messages from a stream, as well as fetching consumer info
	Consumer interface {
		// Fetch is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and wait until either all messages are retreived
		// or context reaches its deadline.
		Fetch(int, ...FetchOpt) (MessageBatch, error)
		// FetchNoWait is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and immediately return up to a provided number of messages
		FetchNoWait(batch int) (MessageBatch, error)
		// Consume can be used to continuously receive messages and handle them with the provided callback function
		Consume(MessageHandler, ...PullConsumeOpt) (ConsumeContext, error)
		// Messages returns [MessagesContext], allowing continuously iterating over messages on a stream.
		Messages(...PullMessagesOpt) (MessagesContext, error)
		// Next is used to retrieve the next message from the stream.
		// This method will block until the message is retrieved or timeout is reached.
		Next(...FetchOpt) (Msg, error)

		// Info returns Consumer details
		Info(context.Context) (*ConsumerInfo, error)
		// CachedInfo returns [*ConsumerInfo] cached on a consumer struct
		CachedInfo() *ConsumerInfo
	}
)

// Info returns [ConsumerInfo] for a given consumer
func (p *pullConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	infoSubject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, p.stream, p.name))
	var resp consumerInfoResponse

	if _, err := p.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	p.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// CachedInfo returns [ConsumerInfo] fetched when initializing/updating a consumer
//
// NOTE: The returned object might not be up to date with the most recent updates on the server
// For up-to-date information, use [Info]
func (p *pullConsumer) CachedInfo() *ConsumerInfo {
	return p.info
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig) (Consumer, error) {
	req := createConsumerRequest{
		Stream: stream,
		Config: &cfg,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.Durable != "" {
		if err := validateDurableName(cfg.Durable); err != nil {
			return nil, err
		}
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiDurableCreateT, stream, cfg.Durable))
	} else {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateT, stream))
	}
	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, ccSubj, &resp, reqJSON); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		jetStream: js,
		stream:    stream,
		name:      resp.Name,
		durable:   cfg.Durable != "",
		info:      resp.ConsumerInfo,
	}, nil
}

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	if err := validateDurableName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerInfoT, stream, name))

	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		jetStream: js,
		stream:    stream,
		name:      name,
		durable:   resp.Config.Durable != "",
		info:      resp.ConsumerInfo,
	}, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	if err := validateDurableName(consumer); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerDeleteT, stream, consumer))

	var resp consumerDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return ErrConsumerNotFound
		}
		return resp.Error
	}
	return nil
}

func validateDurableName(dur string) error {
	if strings.Contains(dur, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidConsumerName, dur)
	}
	return nil
}

func compareConsumerConfig(s, u *ConsumerConfig) error {
	makeErr := func(fieldName string, usrVal, srvVal interface{}) error {
		return fmt.Errorf("configuration requests %s to be %v, but consumer's value is %v", fieldName, usrVal, srvVal)
	}

	if u.Durable != s.Durable {
		return makeErr("durable", u.Durable, s.Durable)
	}
	if u.Description != s.Description {
		return makeErr("description", u.Description, s.Description)
	}
	if u.DeliverPolicy != s.DeliverPolicy {
		return makeErr("deliver policy", u.DeliverPolicy, s.DeliverPolicy)
	}
	if u.OptStartSeq != s.OptStartSeq {
		return makeErr("optional start sequence", u.OptStartSeq, s.OptStartSeq)
	}
	if u.OptStartTime != nil && !u.OptStartTime.IsZero() && !(*u.OptStartTime).Equal(*s.OptStartTime) {
		return makeErr("optional start time", u.OptStartTime, s.OptStartTime)
	}
	if u.AckPolicy != s.AckPolicy {
		return makeErr("ack policy", u.AckPolicy, s.AckPolicy)
	}
	if u.AckWait != 0 && u.AckWait != s.AckWait {
		return makeErr("ack wait", u.AckWait.String(), s.AckWait.String())
	}
	if !(u.MaxDeliver == 0 && s.MaxDeliver == -1) && u.MaxDeliver != s.MaxDeliver {
		return makeErr("max deliver", u.MaxDeliver, s.MaxDeliver)
	}
	if len(u.BackOff) != len(s.BackOff) {
		return makeErr("backoff", u.BackOff, s.BackOff)
	}
	for i, val := range u.BackOff {
		if val != s.BackOff[i] {
			return makeErr("backoff", u.BackOff, s.BackOff)
		}
	}
	for i, val := range u.FilterSubjects {
		if val != s.FilterSubjects[i] {
			return makeErr("filter_subjects", u.FilterSubjects, s.FilterSubjects)
		}
	}
	if u.ReplayPolicy != s.ReplayPolicy {
		return makeErr("replay policy", u.ReplayPolicy, s.ReplayPolicy)
	}
	if u.RateLimit != s.RateLimit {
		return makeErr("rate limit", u.RateLimit, s.RateLimit)
	}
	if u.SampleFrequency != s.SampleFrequency {
		return makeErr("sample frequency", u.SampleFrequency, s.SampleFrequency)
	}
	if u.MaxWaiting != 0 && u.MaxWaiting != s.MaxWaiting {
		return makeErr("max waiting", u.MaxWaiting, s.MaxWaiting)
	}
	if u.MaxAckPending != 0 && u.MaxAckPending != s.MaxAckPending {
		return makeErr("max ack pending", u.MaxAckPending, s.MaxAckPending)
	}
	if u.FlowControl != s.FlowControl {
		return makeErr("flow control", u.FlowControl, s.FlowControl)
	}
	if u.Heartbeat != s.Heartbeat {
		return makeErr("heartbeat", u.Heartbeat, s.Heartbeat)
	}
	if u.HeadersOnly != s.HeadersOnly {
		return makeErr("headers only", u.HeadersOnly, s.HeadersOnly)
	}
	if u.MaxRequestBatch != s.MaxRequestBatch {
		return makeErr("max request batch", u.MaxRequestBatch, s.MaxRequestBatch)
	}
	if u.MaxRequestExpires != s.MaxRequestExpires {
		return makeErr("max request expires", u.MaxRequestExpires.String(), s.MaxRequestExpires.String())
	}
	if u.DeliverSubject != s.DeliverSubject {
		return makeErr("deliver subject", u.DeliverSubject, s.DeliverSubject)
	}
	if u.DeliverGroup != s.DeliverGroup {
		return makeErr("deliver group", u.DeliverSubject, s.DeliverSubject)
	}
	if u.InactiveThreshold != s.InactiveThreshold {
		return makeErr("inactive threshold", u.InactiveThreshold.String(), s.InactiveThreshold.String())
	}
	if u.Replicas != s.Replicas {
		return makeErr("replicas", u.Replicas, s.Replicas)
	}
	if u.MemoryStorage != s.MemoryStorage {
		return makeErr("memory storage", u.MemoryStorage, s.MemoryStorage)
	}
	return nil
}
