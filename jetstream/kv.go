// Copyright 2023 The NATS Authors
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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/parser"
)

// Public interfaces and structs
type (
	// KeyValueManager is used to manage KeyValue stores.
	KeyValueManager interface {
		// KeyValue will lookup and bind to an existing KeyValue store.
		KeyValue(ctx context.Context, bucket string) (KeyValue, error)
		// CreateKeyValue will create a KeyValue store with the following configuration.
		CreateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
		// DeleteKeyValue will delete this KeyValue store (JetStream stream).
		DeleteKeyValue(ctx context.Context, bucket string) error
		// KeyValueStoreNames is used to retrieve a list of key value store names
		KeyValueStoreNames(ctx context.Context) KeyValueNamesLister
		// KeyValueStores is used to retrieve a list of key value store statuses
		KeyValueStores(ctx context.Context) KeyValueLister
	}

	// KeyValue contains methods to operate on a KeyValue store.
	KeyValue interface {
		// Get returns the latest value for the key.
		Get(ctx context.Context, key string) (KeyValueEntry, error)
		// GetRevision returns a specific revision value for the key.
		GetRevision(ctx context.Context, key string, revision uint64) (KeyValueEntry, error)
		// Put will place the new value for the key into the store.
		Put(ctx context.Context, key string, value []byte) (uint64, error)
		// PutString will place the string for the key into the store.
		PutString(ctx context.Context, key string, value string) (uint64, error)
		// Create will add the key/value pair if it does not exist.
		Create(ctx context.Context, key string, value []byte) (uint64, error)
		// Update will update the value if the latest revision matches.
		Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)
		// Delete will place a delete marker and leave all revisions.
		Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error
		// Purge will place a delete marker and remove all previous revisions.
		Purge(ctx context.Context, key string, opts ...KVDeleteOpt) error
		// Watch for any updates to keys that match the keys argument which could include wildcards.
		// Watch will send a nil entry when it has received all initial values.
		Watch(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error)
		// WatchAll will invoke the callback for all updates.
		WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error)
		// Keys will return all keys.
		Keys(ctx context.Context, opts ...WatchOpt) ([]string, error)
		// History will return all historical values for the key.
		History(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error)
		// Bucket returns the current bucket name.
		Bucket() string
		// PurgeDeletes will remove all current delete markers.
		PurgeDeletes(ctx context.Context, opts ...KVPurgeOpt) error
		// Status retrieves the status and configuration of a bucket
		Status(ctx context.Context) (KeyValueStatus, error)
	}

	// KeyValueConfig is for configuring a KeyValue store.
	KeyValueConfig struct {
		Bucket       string
		Description  string
		MaxValueSize int32
		History      uint8
		TTL          time.Duration
		MaxBytes     int64
		Storage      StorageType
		Replicas     int
		Placement    *Placement
		RePublish    *RePublish
		Mirror       *StreamSource
		Sources      []*StreamSource
	}

	KeyValueLister interface {
		Status() <-chan KeyValueStatus
		Error() error
	}

	KeyValueNamesLister interface {
		Name() <-chan string
		Error() error
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

		// Bytes returns the size in bytes of the bucket
		Bytes() uint64
	}

	// KeyWatcher is what is returned when doing a watch.
	KeyWatcher interface {
		// Updates returns a channel to read any updates to entries.
		Updates() <-chan KeyValueEntry
		// Stop will stop this watcher.
		Stop() error
	}

	// KeyValueEntry is a retrieved entry for Get or List or Watch.
	KeyValueEntry interface {
		// Bucket is the bucket the data was loaded from.
		Bucket() string
		// Key is the key that was retrieved.
		Key() string
		// Value is the retrieved value.
		Value() []byte
		// Revision is a unique sequence for this value.
		Revision() uint64
		// Created is the time the data was put in the bucket.
		Created() time.Time
		// Delta is distance from the latest value.
		Delta() uint64
		// Operation returns Put or Delete or Purge.
		Operation() KeyValueOp
	}
)

// Option types

type (
	WatchOpt interface {
		configureWatcher(opts *watchOpts) error
	}

	watchOpts struct {
		// Do not send delete markers to the update channel.
		ignoreDeletes bool
		// Include all history per subject, not just last one.
		includeHistory bool
		// Include only updates for keys.
		updatesOnly bool
		// retrieve only the meta data of the entry
		metaOnly bool
	}

	KVDeleteOpt interface {
		configureDelete(opts *deleteOpts) error
	}

	deleteOpts struct {
		// Remove all previous revisions.
		purge bool

		// Delete only if the latest revision matches.
		revision uint64
	}

	KVPurgeOpt interface {
		configurePurge(opts *purgeOpts) error
	}

	purgeOpts struct {
		dmthr time.Duration // Delete markers threshold
	}
)

// kvs is the implementation of KeyValue
type kvs struct {
	name       string
	streamName string
	pre        string
	putPre     string
	pushJS     nats.JetStreamContext
	js         *jetStream
	stream     Stream
	// If true, it means that APIPrefix/Domain was set in the context
	// and we need to add something to some of our high level protocols
	// (such as Put, etc..)
	useJSPfx bool
	// To know if we can use the stream direct get API
	useDirect bool
}

// KeyValueOp represents the type of KV operation (Put, Delete, Purge)
// Returned as part of watcher entry.
type KeyValueOp uint8

const (
	KeyValuePut KeyValueOp = iota
	KeyValueDelete
	KeyValuePurge
)

func (op KeyValueOp) String() string {
	switch op {
	case KeyValuePut:
		return "KeyValuePutOp"
	case KeyValueDelete:
		return "KeyValueDeleteOp"
	case KeyValuePurge:
		return "KeyValuePurgeOp"
	default:
		return "Unknown Operation"
	}
}

const (
	kvBucketNamePre         = "KV_"
	kvBucketNameTmpl        = "KV_%s"
	kvSubjectsTmpl          = "$KV.%s.>"
	kvSubjectsPreTmpl       = "$KV.%s."
	kvSubjectsPreDomainTmpl = "%s.$KV.%s."
	kvNoPending             = "0"
)

const (
	KeyValueMaxHistory = 64
	AllKeys            = ">"
	kvLatestRevision   = 0
	kvop               = "KV-Operation"
	kvdel              = "DEL"
	kvpurge            = "PURGE"
)

// Regex for valid keys and buckets.
var (
	validBucketRe = regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	validKeyRe    = regexp.MustCompile(`\A[-/_=\.a-zA-Z0-9]+\z`)
)

func (js *jetStream) KeyValue(ctx context.Context, bucket string) (KeyValue, error) {
	if !validBucketRe.MatchString(bucket) {
		return nil, ErrInvalidBucketName
	}
	streamName := fmt.Sprintf(kvBucketNameTmpl, bucket)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		if err == ErrStreamNotFound {
			err = ErrBucketNotFound
		}
		return nil, err
	}
	// Do some quick sanity checks that this is a correctly formed stream for KV.
	// Max msgs per subject should be > 0.
	if stream.CachedInfo().Config.MaxMsgsPerSubject < 1 {
		return nil, ErrBadBucket
	}
	pushJS, err := js.legacyJetStream()
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

// CreateKeyValue will create a KeyValue store with the following configuration.
func (js *jetStream) CreateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error) {
	if !validBucketRe.MatchString(cfg.Bucket) {
		return nil, ErrInvalidBucketName
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		return nil, err
	}

	// Default to 1 for history. Max is 64 for now.
	history := int64(1)
	if cfg.History > 0 {
		if cfg.History > KeyValueMaxHistory {
			return nil, ErrHistoryTooLarge
		}
		history = int64(cfg.History)
	}

	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	// We will set explicitly some values so that we can do comparison
	// if we get an "already in use" error and need to check if it is same.
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}
	maxMsgSize := cfg.MaxValueSize
	if maxMsgSize == 0 {
		maxMsgSize = -1
	}
	// When stream's MaxAge is not set, server uses 2 minutes as the default
	// for the duplicate window. If MaxAge is set, and lower than 2 minutes,
	// then the duplicate window will be set to that. If MaxAge is greater,
	// we will cap the duplicate window to 2 minutes (to be consistent with
	// previous behavior).
	duplicateWindow := 2 * time.Minute
	if cfg.TTL > 0 && cfg.TTL < duplicateWindow {
		duplicateWindow = cfg.TTL
	}
	scfg := StreamConfig{
		Name:              fmt.Sprintf(kvBucketNameTmpl, cfg.Bucket),
		Description:       cfg.Description,
		MaxMsgsPerSubject: history,
		MaxBytes:          maxBytes,
		MaxAge:            cfg.TTL,
		MaxMsgSize:        maxMsgSize,
		Storage:           cfg.Storage,
		Replicas:          replicas,
		Placement:         cfg.Placement,
		AllowRollup:       true,
		DenyDelete:        true,
		Duplicates:        duplicateWindow,
		MaxMsgs:           -1,
		MaxConsumers:      -1,
		AllowDirect:       true,
		RePublish:         cfg.RePublish,
	}
	if cfg.Mirror != nil {
		// Copy in case we need to make changes so we do not change caller's version.
		m := cfg.Mirror.copy()
		if !strings.HasPrefix(m.Name, kvBucketNamePre) {
			m.Name = fmt.Sprintf(kvBucketNameTmpl, m.Name)
		}
		scfg.Mirror = m
		scfg.MirrorDirect = true
	} else if len(cfg.Sources) > 0 {
		// For now we do not allow direct subjects for sources. If that is desired a user could use stream API directly.
		for _, ss := range cfg.Sources {
			var sourceBucketName string
			if strings.HasPrefix(ss.Name, kvBucketNamePre) {
				sourceBucketName = ss.Name[len(kvBucketNamePre):]
			} else {
				sourceBucketName = ss.Name
				ss.Name = fmt.Sprintf(kvBucketNameTmpl, ss.Name)
			}

			if ss.External == nil || sourceBucketName != cfg.Bucket {
				ss.SubjectTransforms = []SubjectTransformConfig{{Source: fmt.Sprintf(kvSubjectsTmpl, sourceBucketName), Destination: fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}}
			}
			scfg.Sources = append(scfg.Sources, ss)
		}
		scfg.Subjects = []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}
	} else {
		scfg.Subjects = []string{fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}
	}

	stream, err := js.CreateStream(ctx, scfg)
	if err != nil {
		return nil, err
	}
	pushJS, err := js.legacyJetStream()
	if err != nil {
		return nil, err
	}

	return mapStreamToKVS(js, pushJS, stream), nil
}

// DeleteKeyValue will delete this KeyValue store (JetStream stream).
func (js *jetStream) DeleteKeyValue(ctx context.Context, bucket string) error {
	if !validBucketRe.MatchString(bucket) {
		return ErrInvalidBucketName
	}
	stream := fmt.Sprintf(kvBucketNameTmpl, bucket)
	return js.DeleteStream(ctx, stream)
}

// KeyValueStoreNames is used to retrieve a list of key value store names
func (js *jetStream) KeyValueStoreNames(ctx context.Context) KeyValueNamesLister {
	res := &kvLister{
		kvNames: make(chan string),
	}
	l := &streamLister{js: js}
	streamsReq := streamsRequest{
		Subject: fmt.Sprintf(kvSubjectsTmpl, "*"),
	}
	go func() {
		defer close(res.kvNames)
		for {
			page, err := l.streamNames(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				res.err = err
				return
			}
			for _, name := range page {
				if !strings.HasPrefix(name, kvBucketNamePre) {
					continue
				}
				res.kvNames <- name
			}
			if errors.Is(err, ErrEndOfData) {
				return
			}
		}
	}()
	return res
}

// KeyValueStores is used to retrieve a list of key value store statuses
func (js *jetStream) KeyValueStores(ctx context.Context) KeyValueLister {
	res := &kvLister{
		kvs: make(chan KeyValueStatus),
	}
	l := &streamLister{js: js}
	streamsReq := streamsRequest{
		Subject: fmt.Sprintf(kvSubjectsTmpl, "*"),
	}
	go func() {
		defer close(res.kvs)
		for {
			page, err := l.streamInfos(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				res.err = err
				return
			}
			for _, info := range page {
				if !strings.HasPrefix(info.Config.Name, kvBucketNamePre) {
					continue
				}
				res.kvs <- &KeyValueBucketStatus{nfo: info, bucket: strings.TrimPrefix(info.Config.Name, kvBucketNamePre)}
			}
			if errors.Is(err, ErrEndOfData) {
				return
			}
		}
	}()
	return res
}

// KeyValueBucketStatus represents status of a Bucket, implements KeyValueStatus
type KeyValueBucketStatus struct {
	nfo    *StreamInfo
	bucket string
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
func (s *KeyValueBucketStatus) StreamInfo() *StreamInfo { return s.nfo }

// Bytes is the size of the stream
func (s *KeyValueBucketStatus) Bytes() uint64 { return s.nfo.State.Bytes }

type kvLister struct {
	kvs     chan KeyValueStatus
	kvNames chan string
	err     error
}

func (kl *kvLister) Status() <-chan KeyValueStatus {
	return kl.kvs
}

func (kl *kvLister) Name() <-chan string {
	return kl.kvNames
}

func (kl *kvLister) Error() error {
	return kl.err
}

func (js *jetStream) legacyJetStream() (nats.JetStreamContext, error) {
	opts := make([]nats.JSOpt, 0)
	if js.apiPrefix != "" {
		opts = append(opts, nats.APIPrefix(js.apiPrefix))
	}
	if js.clientTrace != nil {
		opts = append(opts, nats.ClientTrace{
			RequestSent:      js.clientTrace.RequestSent,
			ResponseReceived: js.clientTrace.ResponseReceived,
		})
	}
	return js.conn.JetStream(opts...)
}

func keyValid(key string) bool {
	if len(key) == 0 || key[0] == '.' || key[len(key)-1] == '.' {
		return false
	}
	return validKeyRe.MatchString(key)
}

func (kv *kvs) get(ctx context.Context, key string, revision uint64) (KeyValueEntry, error) {
	if !keyValid(key) {
		return nil, ErrInvalidKey
	}

	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(key)

	var m *RawStreamMsg
	var err error

	if revision == kvLatestRevision {
		m, err = kv.stream.GetLastMsgForSubject(ctx, b.String())
	} else {
		m, err = kv.stream.GetMsg(ctx, revision)
		// If a sequence was provided, just make sure that the retrieved
		// message subject matches the request.
		if err == nil && m.Subject != b.String() {
			return nil, ErrKeyNotFound
		}
	}
	if err != nil {
		if err == ErrMsgNotFound {
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

// kve is the implementation of KeyValueEntry
type kve struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	delta    uint64
	created  time.Time
	op       KeyValueOp
}

func (e *kve) Bucket() string        { return e.bucket }
func (e *kve) Key() string           { return e.key }
func (e *kve) Value() []byte         { return e.value }
func (e *kve) Revision() uint64      { return e.revision }
func (e *kve) Created() time.Time    { return e.created }
func (e *kve) Delta() uint64         { return e.delta }
func (e *kve) Operation() KeyValueOp { return e.op }

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

// Put will place the new value for the key into the store.
func (kv *kvs) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	var b strings.Builder
	if kv.useJSPfx {
		b.WriteString(kv.js.apiPrefix)
	}
	if kv.putPre != "" {
		b.WriteString(kv.putPre)
	} else {
		b.WriteString(kv.pre)
	}
	b.WriteString(key)

	pa, err := kv.js.Publish(ctx, b.String(), value)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// PutString will place the string for the key into the store.
func (kv *kvs) PutString(ctx context.Context, key string, value string) (uint64, error) {
	return kv.Put(ctx, key, []byte(value))
}

// Create will add the key/value pair iff it does not exist.
func (kv *kvs) Create(ctx context.Context, key string, value []byte) (revision uint64, err error) {
	v, err := kv.Update(ctx, key, value, 0)
	if err == nil {
		return v, nil
	}

	if e, err := kv.get(ctx, key, kvLatestRevision); err == ErrKeyDeleted {
		return kv.Update(ctx, key, value, e.Revision())
	}

	// Check if the expected last subject sequence is not zero which implies
	// the key already exists.
	if errors.Is(err, ErrKeyExists) {
		jserr := ErrKeyExists.(*jsError)
		return 0, fmt.Errorf("%w: %s", err, jserr.message)
	}

	return 0, err
}

// Update will update the value if the latest revision matches.
func (kv *kvs) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if !keyValid(key) {
		return 0, ErrInvalidKey
	}

	var b strings.Builder
	if kv.useJSPfx {
		b.WriteString(kv.js.apiPrefix)
	}
	b.WriteString(kv.pre)
	b.WriteString(key)

	m := nats.Msg{Subject: b.String(), Header: nats.Header{}, Data: value}
	m.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(revision, 10))

	pa, err := kv.js.PublishMsg(ctx, &m)
	if err != nil {
		return 0, err
	}
	return pa.Sequence, err
}

// Delete will place a delete marker and leave all revisions.
func (kv *kvs) Delete(ctx context.Context, key string, opts ...KVDeleteOpt) error {
	if !keyValid(key) {
		return ErrInvalidKey
	}

	var b strings.Builder
	if kv.useJSPfx {
		b.WriteString(kv.js.apiPrefix)
	}
	if kv.putPre != "" {
		b.WriteString(kv.putPre)
	} else {
		b.WriteString(kv.pre)
	}
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
		m.Header.Set(MsgRollup, MsgRollupSubject)
	} else {
		m.Header.Set(kvop, kvdel)
	}

	if o.revision != 0 {
		m.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(o.revision, 10))
	}

	_, err := kv.js.PublishMsg(ctx, m)
	return err
}

// Purge will place a delete marker and remove all previous revisions.
func (kv *kvs) Purge(ctx context.Context, key string, opts ...KVDeleteOpt) error {
	return kv.Delete(ctx, key, append(opts, purge())...)
}

// purge removes all previous revisions.
func purge() KVDeleteOpt {
	return deleteOptFn(func(opts *deleteOpts) error {
		opts.purge = true
		return nil
	})
}

// Implementation for Watch
type watcher struct {
	mu          sync.Mutex
	updates     chan KeyValueEntry
	sub         *nats.Subscription
	initDone    bool
	initPending uint64
	received    uint64
}

// Updates returns the interior channel.
func (w *watcher) Updates() <-chan KeyValueEntry {
	if w == nil {
		return nil
	}
	return w.updates
}

// Stop will unsubscribe from the watcher.
func (w *watcher) Stop() error {
	if w == nil {
		return nil
	}
	return w.sub.Unsubscribe()
}

// Watch for any updates to keys that match the keys argument which could include wildcards.
// Watch will send a nil entry when it has received all initial values.
func (kv *kvs) Watch(ctx context.Context, keys string, opts ...WatchOpt) (KeyWatcher, error) {
	var o watchOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt.configureWatcher(&o); err != nil {
				return nil, err
			}
		}
	}

	// Could be a pattern so don't check for validity as we normally do.
	var b strings.Builder
	b.WriteString(kv.pre)
	b.WriteString(keys)
	keys = b.String()

	// We will block below on placing items on the chan. That is by design.
	w := &watcher{updates: make(chan KeyValueEntry, 256)}

	update := func(m *nats.Msg) {
		tokens, err := parser.GetMetadataFields(m.Reply)
		if err != nil {
			return
		}
		if len(m.Subject) <= len(kv.pre) {
			return
		}
		subj := m.Subject[len(kv.pre):]

		var op KeyValueOp
		if len(m.Header) > 0 {
			switch m.Header.Get(kvop) {
			case kvdel:
				op = KeyValueDelete
			case kvpurge:
				op = KeyValuePurge
			}
		}
		delta := parser.ParseNum(tokens[parser.AckNumPendingTokenPos])
		w.mu.Lock()
		defer w.mu.Unlock()
		if !o.ignoreDeletes || (op != KeyValueDelete && op != KeyValuePurge) {
			entry := &kve{
				bucket:   kv.name,
				key:      subj,
				value:    m.Data,
				revision: parser.ParseNum(tokens[parser.AckStreamSeqTokenPos]),
				created:  time.Unix(0, int64(parser.ParseNum(tokens[parser.AckTimestampSeqTokenPos]))),
				delta:    delta,
				op:       op,
			}
			w.updates <- entry
		}
		// Check if done and initial values.
		if !w.initDone {
			w.received++
			// We set this on the first trip through..
			if w.initPending == 0 {
				w.initPending = delta
			}
			if w.received > w.initPending || delta == 0 {
				w.initDone = true
				w.updates <- nil
			}
		}
	}

	// Used ordered consumer to deliver results.
	subOpts := []nats.SubOpt{nats.BindStream(kv.streamName), nats.OrderedConsumer()}
	if !o.includeHistory {
		subOpts = append(subOpts, nats.DeliverLastPerSubject())
	}
	if o.updatesOnly {
		subOpts = append(subOpts, nats.DeliverNew())
	}
	if o.metaOnly {
		subOpts = append(subOpts, nats.HeadersOnly())
	}
	subOpts = append(subOpts, nats.Context(ctx))
	// Create the sub and rest of initialization under the lock.
	// We want to prevent the race between this code and the
	// update() callback.
	w.mu.Lock()
	defer w.mu.Unlock()
	sub, err := kv.pushJS.Subscribe(keys, update, subOpts...)
	if err != nil {
		return nil, err
	}
	sub.SetClosedHandler(func(_ string) {
		close(w.updates)
	})
	// If there were no pending messages at the time of the creation
	// of the consumer, send the marker.
	// Skip if UpdatesOnly() is set, since there will never be updates initially.
	if !o.updatesOnly {
		initialPending, err := sub.InitialConsumerPending()
		if err == nil && initialPending == 0 {
			w.initDone = true
			w.updates <- nil
		}
	} else {
		// if UpdatesOnly was used, mark initialization as complete
		w.initDone = true
	}
	w.sub = sub
	return w, nil
}

// WatchAll will invoke the callback for all updates.
func (kv *kvs) WatchAll(ctx context.Context, opts ...WatchOpt) (KeyWatcher, error) {
	return kv.Watch(ctx, AllKeys, opts...)
}

// Keys will return all keys.
func (kv *kvs) Keys(ctx context.Context, opts ...WatchOpt) ([]string, error) {
	opts = append(opts, IgnoreDeletes(), MetaOnly())
	watcher, err := kv.WatchAll(ctx, opts...)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var keys []string
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		keys = append(keys, entry.Key())
	}
	if len(keys) == 0 {
		return nil, ErrNoKeysFound
	}
	return keys, nil
}

// History will return all historical values for the key.
func (kv *kvs) History(ctx context.Context, key string, opts ...WatchOpt) ([]KeyValueEntry, error) {
	opts = append(opts, IncludeHistory())
	watcher, err := kv.Watch(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var entries []KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, ErrKeyNotFound
	}
	return entries, nil
}

// Bucket returns the current bucket name.
func (kv *kvs) Bucket() string {
	return kv.name
}

const kvDefaultPurgeDeletesMarkerThreshold = 30 * time.Minute

// PurgeDeletes will remove all current delete markers.
func (kv *kvs) PurgeDeletes(ctx context.Context, opts ...KVPurgeOpt) error {
	var o purgeOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt.configurePurge(&o); err != nil {
				return err
			}
		}
	}
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer watcher.Stop()

	var limit time.Time
	olderThan := o.dmthr
	// Negative value is used to instruct to always remove markers, regardless
	// of age. If set to 0 (or not set), use our default value.
	if olderThan == 0 {
		olderThan = kvDefaultPurgeDeletesMarkerThreshold
	}
	if olderThan > 0 {
		limit = time.Now().Add(-olderThan)
	}

	var deleteMarkers []KeyValueEntry
	for entry := range watcher.Updates() {
		if entry == nil {
			break
		}
		if op := entry.Operation(); op == KeyValueDelete || op == KeyValuePurge {
			deleteMarkers = append(deleteMarkers, entry)
		}
	}

	var b strings.Builder
	// Do actual purges here.
	for _, entry := range deleteMarkers {
		b.WriteString(kv.pre)
		b.WriteString(entry.Key())
		purgeOpts := []StreamPurgeOpt{WithPurgeSubject(b.String())}
		if olderThan > 0 && entry.Created().After(limit) {
			purgeOpts = append(purgeOpts, WithPurgeKeep(1))
		}
		if err := kv.stream.Purge(ctx, purgeOpts...); err != nil {
			return err
		}
		b.Reset()
	}
	return nil
}

// Status retrieves the status and configuration of a bucket
func (kv *kvs) Status(ctx context.Context) (KeyValueStatus, error) {
	nfo, err := kv.stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	return &KeyValueBucketStatus{nfo: nfo, bucket: kv.name}, nil
}

func mapStreamToKVS(js *jetStream, pushJS nats.JetStreamContext, stream Stream) *kvs {
	info := stream.CachedInfo()
	bucket := strings.TrimPrefix(info.Config.Name, kvBucketNamePre)
	kv := &kvs{
		name:       bucket,
		streamName: info.Config.Name,
		pre:        fmt.Sprintf(kvSubjectsPreTmpl, bucket),
		js:         js,
		pushJS:     pushJS,
		stream:     stream,
		// Determine if we need to use the JS prefix in front of Put and Delete operations
		useJSPfx:  js.apiPrefix != DefaultAPIPrefix,
		useDirect: info.Config.AllowDirect,
	}

	// If we are mirroring, we will have mirror direct on, so just use the mirror name
	// and override use
	if m := info.Config.Mirror; m != nil {
		bucket := strings.TrimPrefix(m.Name, kvBucketNamePre)
		if m.External != nil && m.External.APIPrefix != "" {
			kv.useJSPfx = false
			kv.pre = fmt.Sprintf(kvSubjectsPreTmpl, bucket)
			kv.putPre = fmt.Sprintf(kvSubjectsPreDomainTmpl, m.External.APIPrefix, bucket)
		} else {
			kv.putPre = fmt.Sprintf(kvSubjectsPreTmpl, bucket)
		}
	}

	return kv
}
