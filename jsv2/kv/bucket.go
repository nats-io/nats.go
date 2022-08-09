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

//nolint:all
package kv

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jsv2/jetstream"
)

type (
	Bucket interface {
		// Get returns the latest value for the key.
		Get(context.Context, string) (KeyValueEntry, error)
		// GetRevision returns a specific revision value for the key.
		GetRevision(context.Context, string, uint64) (entry KeyValueEntry, err error)
		// Put will place the new value for the key into the store.
		Put(context.Context, string, []byte) (revision uint64, err error)
		// PutString will place the string for the key into the store.
		PutString(context.Context, string, string) (revision uint64, err error)
		// Create will add the key/value pair iff it does not exist.
		Create(context.Context, string, []byte) (revision uint64, err error)
		// Update will update the value iff the latest revision matches.
		Update(context.Context, string, []byte, uint64) (revision uint64, err error)
		// Delete will place a delete marker and leave all revisions.
		Delete(context.Context, string, ...DeleteOpt) error
		// Purge will place a delete marker and remove all previous revisions.
		Purge(context.Context, string, ...DeleteOpt) error
		// Watch for any updates to keys that match the keys argument which could include wildcards.
		// Watch will send a nil entry when it has received all initial values.
		Watch(context.Context, string, ...WatchOpt) (KeyWatcher, error)
		// WatchAll will invoke the callback for all updates.
		WatchAll(context.Context, ...WatchOpt) (KeyWatcher, error)
		// Keys will return all keys.
		Keys(context.Context, ...WatchOpt) ([]string, error)
		// History will return all historical values for the key.
		History(context.Context, string, ...WatchOpt) ([]KeyValueEntry, error)
		// Bucket returns the current bucket name.
		Bucket() string
		// PurgeDeletes will remove all current delete markers.
		PurgeDeletes(context.Context, ...PurgeOpt) error
		// Status retrieves the status and configuration of a bucket
		Status(ctx context.Context) (KeyValueStatus, error)
	}

	kvs struct {
		name   string
		stream jetstream.Stream
		pre    string
		js     jetstream.JetStream
		// If true, it means that APIPrefix/Domain was set in the context
		// and we need to add something to some of our high level protocols
		// (such as Put, etc..)
		jsPrefix string
	}

	// Underlying entry.
	kve struct {
		bucket   string
		key      string
		value    []byte
		revision uint64
		delta    uint64
		created  time.Time
		op       KeyValueOp
	}

	// KeyValueStatus is run-time status about a Key-Value bucket
	KeyValueStatus interface {
		// Bucket the name of the bucket
		Bucket() string

		// Values is how many messages are in the bucket, including historical values
		Values() uint64

		// History returns the configured history kept per key
		History() int64

		// TTL is how long the bucket keeps values for
		TTL() time.Duration

		// BackingStore indicates what technology is used for storage of the bucket
		BackingStore() string
	}

	// KeyWatcher is what is returned when doing a watch.
	KeyWatcher interface {
		// Context returns watcher context optionally provided by nats.Context option.
		Context() context.Context
		// Updates returns a channel to read any updates to entries.
		Updates() <-chan KeyValueEntry
		// Stop will stop this watcher.
		Stop() error
	}

	WatchOpt interface {
		configureWatcher(opts *watchOpts) error
	}

	watchOpts struct {
		// Do not send delete markers to the update channel.
		ignoreDeletes bool
		// Include all history per subject, not just last one.
		includeHistory bool
		// retrieve only the meta data of the entry
		metaOnly bool
	}

	watchOptFn func(opts *watchOpts) error

	// KeyValueBucketStatus represents status of a Bucket, implements KeyValueStatus
	KeyValueBucketStatus struct {
		nfo    *jetstream.StreamInfo
		bucket string
	}
)

func (e *kve) Bucket() string        { return e.bucket }
func (e *kve) Key() string           { return e.key }
func (e *kve) Value() []byte         { return e.value }
func (e *kve) Revision() uint64      { return e.revision }
func (e *kve) Created() time.Time    { return e.created }
func (e *kve) Delta() uint64         { return e.delta }
func (e *kve) Operation() KeyValueOp { return e.op }

func keyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validKeyRe.MatchString(key)
}

// Get returns the latest value for the key.
func (kv *kvs) Get(ctx context.Context, key string) (KeyValueEntry, error) {
	e, err := kv.get(ctx, key, kvLatestRevision)
	if err != nil {
		if err == ErrKeyDeleted {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return e, nil
}

// GetRevision returns a specific revision value for the key.
func (kv *kvs) GetRevision(ctx context.Context, key string, revision uint64) (KeyValueEntry, error) {
	e, err := kv.get(ctx, key, revision)
	if err != nil {
		if err == ErrKeyDeleted {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	return e, nil
}

func (kv *kvs) get(ctx context.Context, key string, revision uint64) (KeyValueEntry, error) {
	if !keyValid(key) {
		return nil, ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	var m *jetstream.RawStreamMsg
	var err error
	if revision == kvLatestRevision {
		m, err = kv.stream.GetLastMsgForSubject(ctx, b.String())
	} else {
		m, err = kv.stream.GetMsg(ctx, revision)
		if err == nil && m.Subject != b.String() {
			return nil, ErrKeyNotFound
		}
	}

	if err != nil {
		if err == jetstream.ErrMsgNotFound {
			err = ErrKeyNotFound
		}
		return nil, err
	}

	entry := &kve{
		bucket:   kv.name,
		key:      key,
		value:    m.Data,
		revision: m.Sequence,
		created:  m.Time,
	}

	// Double check here that this is not a DEL Operation marker.
	if len(m.Header) > 0 {
		switch m.Header.Get(kvop) {
		case kvdel:
			entry.op = KeyValueDelete
			return entry, ErrKeyDeleted
		case kvpurge:
			entry.op = KeyValuePurge
			return entry, ErrKeyDeleted
		}
	}

	return entry, nil
}

// Put will place the new value for the key into the store.
func (kv *kvs) Put(ctx context.Context, key string, value []byte) (revision uint64, err error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	var b strings.Builder
	if kv.jsPrefix != "" {
		b.WriteString(kv.jsPrefix)
	}
	b.WriteString(kv.pre)
	b.WriteString(key)

	pa, err := kv.js.Publish(ctx, b.String(), value)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// PutString will place the string for the key into the store.
func (kv *kvs) PutString(ctx context.Context, key string, value string) (revision uint64, err error) {
	return kv.Put(ctx, key, []byte(value))
}

// Create will add the key/value pair if it does not exist.
func (kv *kvs) Create(ctx context.Context, key string, value []byte) (revision uint64, err error) {
	v, err := kv.Update(ctx, key, value, 0)
	if err == nil {
		return v, nil
	}

	// TODO(dlc) - Since we have tombstones for DEL ops for watchers, this could be from that
	// so we need to double check.
	if e, err := kv.get(ctx, key, kvLatestRevision); err == ErrKeyDeleted {
		return kv.Update(ctx, key, value, e.Revision())
	}

	return 0, err
}

// Update will update the value iff the latest revision matches.
func (kv *kvs) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	var b strings.Builder
	if kv.jsPrefix != "" {
		b.WriteString(kv.jsPrefix)
	}
	b.WriteString(kv.pre)
	b.WriteString(key)

	m := nats.Msg{Subject: b.String(), Data: value}
	m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(revision, 10))

	pa, err := kv.js.PublishMsg(ctx, &m)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Delete will place a delete marker and leave all revisions.
func (kv *kvs) Delete(ctx context.Context, key string, opts ...DeleteOpt) error {
	if !keyValid(key) {
		return ErrInvalidKey
	}

	var b strings.Builder
	if kv.jsPrefix != "" {
		b.WriteString(kv.jsPrefix)
	}
	b.WriteString(kv.pre)
	b.WriteString(key)

	// DEL op marker. For watch functionality.
	m := nats.NewMsg(b.String())

	var o deleteOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt.configureDelete(&o); err != nil {
				return err
			}
		}
	}

	if o.purge {
		m.Header.Set(kvop, kvpurge)
		m.Header.Set(jetstream.MsgRollup, jetstream.MsgRollupSubject)
	} else {
		m.Header.Set(kvop, kvdel)
	}

	if o.revision != 0 {
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(o.revision, 10))
	}

	_, err := kv.js.PublishMsg(ctx, m)
	return err
}

// Purge will remove the key and all revisions.
func (kv *kvs) Purge(ctx context.Context, key string, opts ...DeleteOpt) error {
	return kv.Delete(ctx, key, append(opts, purge())...)
}

// PurgeDeletes will remove all current delete markers.
// This is a maintenance option if there is a larger buildup of delete markers.
// See DeleteMarkersOlderThan() option for more information.
// TODO: in order for watcher to work, ordered push consumer needs to be implemented for jsv2
func (kv *kvs) PurgeDeletes(ctx context.Context, opts ...PurgeOpt) error {
	panic("not implemented")
}

// Keys() will return all keys.
// TODO: in order for watcher to work, ordered push consumer needs to be implemented for jsv2
func (kv *kvs) Keys(ctx context.Context, opts ...WatchOpt) ([]string, error) {
	panic("not implemented")
}

// History will return all values for the key.
// TODO: in order for watcher to work, ordered push consumer needs to be implemented for jsv2
func (kv *kvs) History(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error) {
	panic("not implemented")
}

// WatchAll watches all keys.
// TODO: in order for watcher to work, ordered push consumer needs to be implemented for jsv2
func (kv *kvs) WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error) {
	panic("not implemented")
}

// Watch will fire the callback when a key that matches the keys pattern is updated.
// keys needs to be a valid NATS subject.
// TODO: in order for watcher to work, ordered push consumer needs to be implemented for jsv2
func (kv *kvs) Watch(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error) {
	panic("not implemented")
}

// Bucket returns the current bucket name (JetStream stream).
func (kv *kvs) Bucket() string {
	return kv.name
}

// Bucket the name of the bucket
func (s *KeyValueBucketStatus) Bucket() string { return s.bucket }

// Values is how many messages are in the bucket, including historical values
func (s *KeyValueBucketStatus) Values() uint64 { return s.nfo.State.Msgs }

// History returns the configured history kept per key
func (s *KeyValueBucketStatus) History() int64 { return s.nfo.Config.MaxMsgsPerSubject }

// TTL is how long the bucket keeps values for
func (s *KeyValueBucketStatus) TTL() time.Duration { return s.nfo.Config.MaxAge }

// BackingStore indicates what technology is used for storage of the bucket
func (s *KeyValueBucketStatus) BackingStore() string { return "JetStream" }

// StreamInfo is the stream info retrieved to create the status
func (s *KeyValueBucketStatus) StreamInfo() *jetstream.StreamInfo { return s.nfo }

// Status retrieves the status and configuration of a bucket
func (kv *kvs) Status(ctx context.Context) (KeyValueStatus, error) {
	nfo, err := kv.stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	return &KeyValueBucketStatus{nfo: nfo, bucket: kv.name}, nil
}
