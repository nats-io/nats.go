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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/parser"
	"github.com/nats-io/nuid"
)

type (
	// ObjectStoreManager creates, loads and deletes Object Stores
	ObjectStoreManager interface {
		// ObjectStore will look up and bind to an existing object store instance.
		ObjectStore(ctx context.Context, bucket string) (ObjectStore, error)
		// CreateObjectStore will create an object store.
		CreateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error)
		// DeleteObjectStore will delete the underlying stream for the named object.
		DeleteObjectStore(ctx context.Context, bucket string) error
		// ObjectStoreNames is used to retrieve a list of bucket names
		ObjectStoreNames(ctx context.Context) ObjectStoreNamesLister
		// ObjectStores is used to retrieve a list of bucket statuses
		ObjectStores(ctx context.Context) ObjectStoresLister
	}

	// ObjectStore is a blob store capable of storing large objects efficiently in
	// JetStream streams
	ObjectStore interface {
		// Put will place the contents from the reader into a new object.
		Put(ctx context.Context, obj ObjectMeta, reader io.Reader) (*ObjectInfo, error)
		// Get will pull the named object from the object store.
		Get(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error)

		// PutBytes is convenience function to put a byte slice into this object store.
		PutBytes(ctx context.Context, name string, data []byte) (*ObjectInfo, error)
		// GetBytes is a convenience function to pull an object from this object store and return it as a byte slice.
		GetBytes(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error)

		// PutString is convenience function to put a string into this object store.
		PutString(ctx context.Context, name string, data string) (*ObjectInfo, error)
		// GetString is a convenience function to pull an object from this object store and return it as a string.
		GetString(ctx context.Context, name string, opts ...GetObjectOpt) (string, error)

		// PutFile is convenience function to put a file into this object store.
		PutFile(ctx context.Context, file string) (*ObjectInfo, error)
		// GetFile is a convenience function to pull an object from this object store and place it in a file.
		GetFile(ctx context.Context, name, file string, opts ...GetObjectOpt) error

		// GetInfo will retrieve the current information for the object.
		GetInfo(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)
		// UpdateMeta will update the metadata for the object.
		UpdateMeta(ctx context.Context, name string, meta ObjectMeta) error

		// Delete will delete the named object.
		Delete(ctx context.Context, name string) error

		// AddLink will add a link to another object.
		AddLink(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error)

		// AddBucketLink will add a link to another object store.
		AddBucketLink(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error)

		// Seal will seal the object store, no further modifications will be allowed.
		Seal(ctx context.Context) error

		// Watch for changes in the underlying store and receive meta information updates.
		Watch(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error)

		// List will list all the objects in this store.
		List(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error)

		// Status retrieves run-time status about the backing store of the bucket.
		Status(ctx context.Context) (ObjectStoreStatus, error)
	}

	// ObjectWatcher is what is returned when doing a watch.
	ObjectWatcher interface {
		// Updates returns a channel to read any updates to entries.
		Updates() <-chan *ObjectInfo
		// Stop will stop this watcher.
		Stop() error
	}

	// ObjectStoreConfig is the config for the object store.
	ObjectStoreConfig struct {
		Bucket      string        `json:"bucket"`
		Description string        `json:"description,omitempty"`
		TTL         time.Duration `json:"max_age,omitempty"`
		MaxBytes    int64         `json:"max_bytes,omitempty"`
		Storage     StorageType   `json:"storage,omitempty"`
		Replicas    int           `json:"num_replicas,omitempty"`
		Placement   *Placement    `json:"placement,omitempty"`
		Compression bool          `json:"compression,omitempty"`

		// Bucket-specific metadata
		// NOTE: Metadata requires nats-server v2.10.0+
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	ObjectStoresLister interface {
		Status() <-chan ObjectStoreStatus
		Error() error
	}

	ObjectStoreNamesLister interface {
		Name() <-chan string
		Error() error
	}

	ObjectStoreStatus interface {
		// Bucket is the name of the bucket
		Bucket() string
		// Description is the description supplied when creating the bucket
		Description() string
		// TTL indicates how long objects are kept in the bucket
		TTL() time.Duration
		// Storage indicates the underlying JetStream storage technology used to store data
		Storage() StorageType
		// Replicas indicates how many storage replicas are kept for the data in the bucket
		Replicas() int
		// Sealed indicates the stream is sealed and cannot be modified in any way
		Sealed() bool
		// Size is the combined size of all data in the bucket including metadata, in bytes
		Size() uint64
		// BackingStore provides details about the underlying storage
		BackingStore() string
		// Metadata is the user supplied metadata for the bucket
		Metadata() map[string]string
		// IsCompressed indicates if the data is compressed on disk
		IsCompressed() bool
	}

	// ObjectMetaOptions
	ObjectMetaOptions struct {
		Link      *ObjectLink `json:"link,omitempty"`
		ChunkSize uint32      `json:"max_chunk_size,omitempty"`
	}

	// ObjectMeta is high level information about an object.
	ObjectMeta struct {
		Name        string            `json:"name"`
		Description string            `json:"description,omitempty"`
		Headers     nats.Header       `json:"headers,omitempty"`
		Metadata    map[string]string `json:"metadata,omitempty"`

		// Additional options.
		Opts *ObjectMetaOptions `json:"options,omitempty"`
	}

	// ObjectInfo is meta plus instance information.
	ObjectInfo struct {
		ObjectMeta
		Bucket  string    `json:"bucket"`
		NUID    string    `json:"nuid"`
		Size    uint64    `json:"size"`
		ModTime time.Time `json:"mtime"`
		Chunks  uint32    `json:"chunks"`
		Digest  string    `json:"digest,omitempty"`
		Deleted bool      `json:"deleted,omitempty"`
	}

	// ObjectLink is used to embed links to other buckets and objects.
	ObjectLink struct {
		// Bucket is the name of the other object store.
		Bucket string `json:"bucket"`
		// Name can be used to link to a single object.
		// If empty means this is a link to the whole store, like a directory.
		Name string `json:"name,omitempty"`
	}

	// ObjectResult will return the underlying stream info and also be an io.ReadCloser.
	ObjectResult interface {
		io.ReadCloser
		Info() (*ObjectInfo, error)
		Error() error
	}

	GetObjectOpt func(opts *getObjectOpts) error

	GetObjectInfoOpt func(opts *getObjectInfoOpts) error

	ListObjectsOpt func(opts *listObjectOpts) error

	getObjectOpts struct {
		// Include deleted object in the result.
		showDeleted bool
	}

	getObjectInfoOpts struct {
		// Include deleted object in the result.
		showDeleted bool
	}

	listObjectOpts struct {
		// Include deleted objects in the result channel.
		showDeleted bool
	}

	obs struct {
		name       string
		streamName string
		stream     Stream
		pushJS     nats.JetStreamContext
		js         *jetStream
	}

	// ObjectResult impl.
	objResult struct {
		sync.Mutex
		info   *ObjectInfo
		r      io.ReadCloser
		err    error
		ctx    context.Context
		digest hash.Hash
	}
)

const (
	objNameTmpl         = "OBJ_%s"     // OBJ_<bucket> // stream name
	objAllChunksPreTmpl = "$O.%s.C.>"  // $O.<bucket>.C.> // chunk stream subject
	objAllMetaPreTmpl   = "$O.%s.M.>"  // $O.<bucket>.M.> // meta stream subject
	objChunksPreTmpl    = "$O.%s.C.%s" // $O.<bucket>.C.<object-nuid> // chunk message subject
	objMetaPreTmpl      = "$O.%s.M.%s" // $O.<bucket>.M.<name-encoded> // meta message subject
	objNoPending        = "0"
	objDefaultChunkSize = uint32(128 * 1024) // 128k
	objDigestType       = "SHA-256="
	objDigestTmpl       = objDigestType + "%s"
)

// CreateObjectStore will create an object store.
func (js *jetStream) CreateObjectStore(ctx context.Context, cfg ObjectStoreConfig) (ObjectStore, error) {
	if !validBucketRe.MatchString(cfg.Bucket) {
		return nil, ErrInvalidStoreName
	}

	name := cfg.Bucket
	chunks := fmt.Sprintf(objAllChunksPreTmpl, name)
	meta := fmt.Sprintf(objAllMetaPreTmpl, name)

	// We will set explicitly some values so that we can do comparison
	// if we get an "already in use" error and need to check if it is same.
	// See kv
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1
	}
	var compression StoreCompression
	if cfg.Compression {
		compression = S2Compression
	}
	scfg := StreamConfig{
		Name:        fmt.Sprintf(objNameTmpl, name),
		Description: cfg.Description,
		Subjects:    []string{chunks, meta},
		MaxAge:      cfg.TTL,
		MaxBytes:    maxBytes,
		Storage:     cfg.Storage,
		Replicas:    replicas,
		Placement:   cfg.Placement,
		Discard:     DiscardNew,
		AllowRollup: true,
		AllowDirect: true,
		Metadata:    cfg.Metadata,
		Compression: compression,
	}

	// Create our stream.
	stream, err := js.CreateStream(ctx, scfg)
	if err != nil {
		return nil, err
	}
	pushJS, err := js.legacyJetStream()
	if err != nil {
		return nil, err
	}

	return mapStreamToObjectStore(js, pushJS, name, stream), nil
}

// ObjectStore will look up and bind to an existing object store instance.
func (js *jetStream) ObjectStore(ctx context.Context, bucket string) (ObjectStore, error) {
	if !validBucketRe.MatchString(bucket) {
		return nil, ErrInvalidStoreName
	}

	streamName := fmt.Sprintf(objNameTmpl, bucket)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(err, ErrStreamNotFound) {
			err = ErrBucketNotFound
		}
		return nil, err
	}
	pushJS, err := js.legacyJetStream()
	if err != nil {
		return nil, err
	}
	return mapStreamToObjectStore(js, pushJS, bucket, stream), nil
}

// DeleteObjectStore will delete the underlying stream for the named object.
func (js *jetStream) DeleteObjectStore(ctx context.Context, bucket string) error {
	stream := fmt.Sprintf(objNameTmpl, bucket)
	return js.DeleteStream(ctx, stream)
}

func encodeName(name string) string {
	return base64.URLEncoding.EncodeToString([]byte(name))
}

// Put will place the contents from the reader into this object-store.
func (obs *obs) Put(ctx context.Context, meta ObjectMeta, r io.Reader) (*ObjectInfo, error) {
	if meta.Name == "" {
		return nil, ErrBadObjectMeta
	}

	if meta.Opts == nil {
		meta.Opts = &ObjectMetaOptions{ChunkSize: objDefaultChunkSize}
	} else if meta.Opts.Link != nil {
		return nil, ErrLinkNotAllowed
	} else if meta.Opts.ChunkSize == 0 {
		meta.Opts.ChunkSize = objDefaultChunkSize
	}

	// Create the new nuid so chunks go on a new subject if the name is re-used
	newnuid := nuid.Next()

	// These will be used in more than one place
	chunkSubj := fmt.Sprintf(objChunksPreTmpl, obs.name, newnuid)

	// Grab existing meta info (einfo). Ok to be found or not found, any other error is a problem
	// Chunks on the old nuid can be cleaned up at the end
	einfo, err := obs.GetInfo(ctx, meta.Name, GetObjectInfoShowDeleted()) // GetInfo will encode the name
	if err != nil && err != ErrObjectNotFound {
		return nil, err
	}

	// For async error handling
	var perr error
	var mu sync.Mutex
	setErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		perr = err
	}
	getErr := func() error {
		mu.Lock()
		defer mu.Unlock()
		return perr
	}

	// Create our own JS context to handle errors etc.
	pubJS, err := New(obs.js.conn, WithPublishAsyncErrHandler(func(js JetStream, _ *nats.Msg, err error) { setErr(err) }))
	if err != nil {
		return nil, err
	}

	defer pubJS.(*jetStream).cleanupReplySub()

	purgePartial := func() {
		// wait until all pubs are complete or up to default timeout before attempting purge
		select {
		case <-pubJS.PublishAsyncComplete():
		case <-ctx.Done():
		}
		_ = obs.stream.Purge(ctx, WithPurgeSubject(chunkSubj))
	}

	m, h := nats.NewMsg(chunkSubj), sha256.New()
	chunk, sent, total := make([]byte, meta.Opts.ChunkSize), 0, uint64(0)

	// set up the info object. The chunk upload sets the size and digest
	info := &ObjectInfo{Bucket: obs.name, NUID: newnuid, ObjectMeta: meta}

	for r != nil {
		if ctx != nil {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.Canceled {
					err = ctx.Err()
				} else {
					err = nats.ErrTimeout
				}
			default:
			}
			if err != nil {
				purgePartial()
				return nil, err
			}
		}

		// Actual read.
		// TODO(dlc) - Deadline?
		n, readErr := r.Read(chunk)

		// Handle all non EOF errors
		if readErr != nil && readErr != io.EOF {
			purgePartial()
			return nil, readErr
		}

		// Add chunk only if we received data
		if n > 0 {
			// Chunk processing.
			m.Data = chunk[:n]
			h.Write(m.Data)

			// Send msg itself.
			if _, err := pubJS.PublishMsgAsync(m); err != nil {
				purgePartial()
				return nil, err
			}
			if err := getErr(); err != nil {
				purgePartial()
				return nil, err
			}
			// Update totals.
			sent++
			total += uint64(n)
		}

		// EOF Processing.
		if readErr == io.EOF {
			// Place meta info.
			info.Size, info.Chunks = uint64(total), uint32(sent)
			info.Digest = GetObjectDigestValue(h)
			break
		}
	}

	// Prepare the meta message
	metaSubj := fmt.Sprintf(objMetaPreTmpl, obs.name, encodeName(meta.Name))
	mm := nats.NewMsg(metaSubj)
	mm.Header.Set(MsgRollup, MsgRollupSubject)
	mm.Data, err = json.Marshal(info)
	if err != nil {
		if r != nil {
			purgePartial()
		}
		return nil, err
	}

	// Publish the meta message.
	_, err = pubJS.PublishMsgAsync(mm)
	if err != nil {
		if r != nil {
			purgePartial()
		}
		return nil, err
	}

	// Wait for all to be processed.
	select {
	case <-pubJS.PublishAsyncComplete():
		if err := getErr(); err != nil {
			if r != nil {
				purgePartial()
			}
			return nil, err
		}
	case <-ctx.Done():
		return nil, nats.ErrTimeout
	}

	info.ModTime = time.Now().UTC() // This time is not actually the correct time

	// Delete any original chunks.
	if einfo != nil && !einfo.Deleted {
		echunkSubj := fmt.Sprintf(objChunksPreTmpl, obs.name, einfo.NUID)
		_ = obs.stream.Purge(ctx, WithPurgeSubject(echunkSubj))
	}

	// TODO would it be okay to do this to return the info with the correct time?
	// With the understanding that it is an extra call to the server.
	// Otherwise the time the user gets back is the client time, not the server time.
	// return obs.GetInfo(info.Name)

	return info, nil
}

// GetObjectDigestValue calculates the base64 value of hashed data
func GetObjectDigestValue(data hash.Hash) string {
	sha := data.Sum(nil)
	return fmt.Sprintf(objDigestTmpl, base64.URLEncoding.EncodeToString(sha[:]))
}

// DecodeObjectDigest decodes base64 hash
func DecodeObjectDigest(data string) ([]byte, error) {
	digest := strings.SplitN(data, "=", 2)
	if len(digest) != 2 {
		return nil, ErrInvalidDigestFormat
	}
	return base64.URLEncoding.DecodeString(digest[1])
}

func (info *ObjectInfo) isLink() bool {
	return info.ObjectMeta.Opts != nil && info.ObjectMeta.Opts.Link != nil
}

// Get will pull the object from the underlying stream.
func (obs *obs) Get(ctx context.Context, name string, opts ...GetObjectOpt) (ObjectResult, error) {
	var o getObjectOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}
	infoOpts := make([]GetObjectInfoOpt, 0)
	if o.showDeleted {
		infoOpts = append(infoOpts, GetObjectInfoShowDeleted())
	}

	// Grab meta info.
	info, err := obs.GetInfo(ctx, name, infoOpts...)
	if err != nil {
		return nil, err
	}
	if info.NUID == "" {
		return nil, ErrBadObjectMeta
	}

	// Check for object links. If single objects we do a pass through.
	if info.isLink() {
		if info.ObjectMeta.Opts.Link.Name == "" {
			return nil, ErrCantGetBucket
		}

		// is the link in the same bucket?
		lbuck := info.ObjectMeta.Opts.Link.Bucket
		if lbuck == obs.name {
			return obs.Get(ctx, info.ObjectMeta.Opts.Link.Name)
		}

		// different bucket
		lobs, err := obs.js.ObjectStore(ctx, lbuck)
		if err != nil {
			return nil, err
		}
		return lobs.Get(ctx, info.ObjectMeta.Opts.Link.Name)
	}

	result := &objResult{info: info, ctx: ctx}
	if info.Size == 0 {
		return result, nil
	}

	pr, pw := net.Pipe()
	result.r = pr

	gotErr := func(m *nats.Msg, err error) {
		pw.Close()
		m.Sub.Unsubscribe()
		result.setErr(err)
	}

	// For calculating sum256
	result.digest = sha256.New()

	processChunk := func(m *nats.Msg) {
		var err error
		if ctx != nil {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.Canceled {
					err = ctx.Err()
				} else {
					err = nats.ErrTimeout
				}
			default:
			}
			if err != nil {
				gotErr(m, err)
				return
			}
		}

		tokens, err := parser.GetMetadataFields(m.Reply)
		if err != nil {
			gotErr(m, err)
			return
		}

		// Write to our pipe.
		for b := m.Data; len(b) > 0; {
			n, err := pw.Write(b)
			if err != nil {
				gotErr(m, err)
				return
			}
			b = b[n:]
		}
		// Update sha256
		result.digest.Write(m.Data)

		// Check if we are done.
		if tokens[parser.AckNumPendingTokenPos] == objNoPending {
			pw.Close()
			m.Sub.Unsubscribe()
		}
	}

	chunkSubj := fmt.Sprintf(objChunksPreTmpl, obs.name, info.NUID)
	_, err = obs.pushJS.Subscribe(chunkSubj, processChunk, nats.OrderedConsumer(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Delete will delete the object.
func (obs *obs) Delete(ctx context.Context, name string) error {
	// Grab meta info.
	info, err := obs.GetInfo(ctx, name, GetObjectInfoShowDeleted())
	if err != nil {
		return err
	}
	if info.NUID == "" {
		return ErrBadObjectMeta
	}

	// Place a rollup delete marker and publish the info
	info.Deleted = true
	info.Size, info.Chunks, info.Digest = 0, 0, ""

	if err = publishMeta(ctx, info, obs.js); err != nil {
		return err
	}

	// Purge chunks for the object.
	chunkSubj := fmt.Sprintf(objChunksPreTmpl, obs.name, info.NUID)
	return obs.stream.Purge(ctx, WithPurgeSubject(chunkSubj))
}

func publishMeta(ctx context.Context, info *ObjectInfo, js *jetStream) error {
	// marshal the object into json, don't store an actual time
	info.ModTime = time.Time{}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	// Prepare and publish the message.
	mm := nats.NewMsg(fmt.Sprintf(objMetaPreTmpl, info.Bucket, encodeName(info.ObjectMeta.Name)))
	mm.Header.Set(MsgRollup, MsgRollupSubject)
	mm.Data = data
	if _, err := js.PublishMsg(ctx, mm); err != nil {
		return err
	}

	// set the ModTime in case it's returned to the user, even though it's not the correct time.
	info.ModTime = time.Now().UTC()
	return nil
}

// AddLink will add a link to another object if it's not deleted and not another link
// name is the name of this link object
// obj is what is being linked too
func (obs *obs) AddLink(ctx context.Context, name string, obj *ObjectInfo) (*ObjectInfo, error) {
	if name == "" {
		return nil, ErrNameRequired
	}

	// TODO Handle stale info

	if obj == nil || obj.Name == "" {
		return nil, ErrObjectRequired
	}
	if obj.Deleted {
		return nil, ErrNoLinkToDeleted
	}
	if obj.isLink() {
		return nil, ErrNoLinkToLink
	}

	// If object with link's name is found, error.
	// If link with link's name is found, that's okay to overwrite.
	// If there was an error that was not ErrObjectNotFound, error.
	einfo, err := obs.GetInfo(ctx, name, GetObjectInfoShowDeleted())
	if einfo != nil {
		if !einfo.isLink() {
			return nil, ErrObjectAlreadyExists
		}
	} else if err != ErrObjectNotFound {
		return nil, err
	}

	// create the meta for the link
	meta := &ObjectMeta{
		Name: name,
		Opts: &ObjectMetaOptions{Link: &ObjectLink{Bucket: obj.Bucket, Name: obj.Name}},
	}
	info := &ObjectInfo{Bucket: obs.name, NUID: nuid.Next(), ModTime: time.Now().UTC(), ObjectMeta: *meta}

	// put the link object
	if err = publishMeta(ctx, info, obs.js); err != nil {
		return nil, err
	}

	return info, nil
}

// AddBucketLink will add a link to another object store.
func (ob *obs) AddBucketLink(ctx context.Context, name string, bucket ObjectStore) (*ObjectInfo, error) {
	if name == "" {
		return nil, ErrNameRequired
	}
	if bucket == nil {
		return nil, ErrBucketRequired
	}
	bos, ok := bucket.(*obs)
	if !ok {
		return nil, ErrBucketMalformed
	}

	// If object with link's name is found, error.
	// If link with link's name is found, that's okay to overwrite.
	// If there was an error that was not ErrObjectNotFound, error.
	einfo, err := ob.GetInfo(ctx, name, GetObjectInfoShowDeleted())
	if einfo != nil {
		if !einfo.isLink() {
			return nil, ErrObjectAlreadyExists
		}
	} else if err != ErrObjectNotFound {
		return nil, err
	}

	// create the meta for the link
	meta := &ObjectMeta{
		Name: name,
		Opts: &ObjectMetaOptions{Link: &ObjectLink{Bucket: bos.name}},
	}
	info := &ObjectInfo{Bucket: ob.name, NUID: nuid.Next(), ObjectMeta: *meta}

	// put the link object
	err = publishMeta(ctx, info, ob.js)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// PutBytes is convenience function to put a byte slice into this object store.
func (obs *obs) PutBytes(ctx context.Context, name string, data []byte) (*ObjectInfo, error) {
	return obs.Put(ctx, ObjectMeta{Name: name}, bytes.NewReader(data))
}

// GetBytes is a convenience function to pull an object from this object store and return it as a byte slice.
func (obs *obs) GetBytes(ctx context.Context, name string, opts ...GetObjectOpt) ([]byte, error) {
	result, err := obs.Get(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var b bytes.Buffer
	if _, err := b.ReadFrom(result); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// PutString is convenience function to put a string into this object store.
func (obs *obs) PutString(ctx context.Context, name string, data string) (*ObjectInfo, error) {
	return obs.Put(ctx, ObjectMeta{Name: name}, strings.NewReader(data))
}

// GetString is a convenience function to pull an object from this object store and return it as a string.
func (obs *obs) GetString(ctx context.Context, name string, opts ...GetObjectOpt) (string, error) {
	result, err := obs.Get(ctx, name, opts...)
	if err != nil {
		return "", err
	}
	defer result.Close()

	var b bytes.Buffer
	if _, err := b.ReadFrom(result); err != nil {
		return "", err
	}
	return b.String(), nil
}

// PutFile is convenience function to put a file into an object store.
func (obs *obs) PutFile(ctx context.Context, file string) (*ObjectInfo, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return obs.Put(ctx, ObjectMeta{Name: file}, f)
}

// GetFile is a convenience function to pull and object and place in a file.
func (obs *obs) GetFile(ctx context.Context, name, file string, opts ...GetObjectOpt) error {
	// Expect file to be new.
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	result, err := obs.Get(ctx, name, opts...)
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	defer result.Close()

	// Stream copy to the file.
	_, err = io.Copy(f, result)
	return err
}

// GetInfo will retrieve the current information for the object.
func (obs *obs) GetInfo(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error) {
	// Grab last meta value we have.
	if name == "" {
		return nil, ErrNameRequired
	}
	var o getObjectInfoOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}

	metaSubj := fmt.Sprintf(objMetaPreTmpl, obs.name, encodeName(name)) // used as data in a JS API call

	m, err := obs.stream.GetLastMsgForSubject(ctx, metaSubj)
	if err != nil {
		if errors.Is(err, ErrMsgNotFound) {
			err = ErrObjectNotFound
		}
		if errors.Is(err, ErrStreamNotFound) {
			err = ErrBucketNotFound
		}
		return nil, err
	}
	var info ObjectInfo
	if err := json.Unmarshal(m.Data, &info); err != nil {
		return nil, ErrBadObjectMeta
	}
	if !o.showDeleted && info.Deleted {
		return nil, ErrObjectNotFound
	}
	info.ModTime = m.Time
	return &info, nil
}

// UpdateMeta will update the meta for the object.
func (obs *obs) UpdateMeta(ctx context.Context, name string, meta ObjectMeta) error {
	// Grab the current meta.
	info, err := obs.GetInfo(ctx, name)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return ErrUpdateMetaDeleted
		}
		return err
	}

	// If the new name is different from the old, and it exists, error
	// If there was an error that was not ErrObjectNotFound, error.
	if name != meta.Name {
		existingInfo, err := obs.GetInfo(ctx, meta.Name, GetObjectInfoShowDeleted())
		if err != nil && !errors.Is(err, ErrObjectNotFound) {
			return err
		}
		if err == nil && !existingInfo.Deleted {
			return ErrObjectAlreadyExists
		}
	}

	// Update Meta prevents update of ObjectMetaOptions (Link, ChunkSize)
	// These should only be updated internally when appropriate.
	info.Name = meta.Name
	info.Description = meta.Description
	info.Headers = meta.Headers
	info.Metadata = meta.Metadata

	// Prepare the meta message
	if err = publishMeta(ctx, info, obs.js); err != nil {
		return err
	}

	// did the name of this object change? We just stored the meta under the new name
	// so delete the meta from the old name via purge stream for subject
	if name != meta.Name {
		metaSubj := fmt.Sprintf(objMetaPreTmpl, obs.name, encodeName(name))
		return obs.stream.Purge(ctx, WithPurgeSubject(metaSubj))
	}

	return nil
}

// Seal will seal the object store, no further modifications will be allowed.
func (obs *obs) Seal(ctx context.Context) error {
	si, err := obs.stream.Info(ctx)
	if err != nil {
		return err
	}
	// Seal the stream from being able to take on more messages.
	cfg := si.Config
	cfg.Sealed = true
	_, err = obs.js.UpdateStream(ctx, cfg)
	return err
}

// Implementation for Watch
type objWatcher struct {
	updates chan *ObjectInfo
	sub     *nats.Subscription
}

// Updates returns the interior channel.
func (w *objWatcher) Updates() <-chan *ObjectInfo {
	if w == nil {
		return nil
	}
	return w.updates
}

// Stop will unsubscribe from the watcher.
func (w *objWatcher) Stop() error {
	if w == nil {
		return nil
	}
	return w.sub.Unsubscribe()
}

// Watch for changes in the underlying store and receive meta information updates.
func (obs *obs) Watch(ctx context.Context, opts ...WatchOpt) (ObjectWatcher, error) {
	var o watchOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt.configureWatcher(&o); err != nil {
				return nil, err
			}
		}
	}

	var initDoneMarker bool

	w := &objWatcher{updates: make(chan *ObjectInfo, 32)}

	update := func(m *nats.Msg) {
		var info ObjectInfo
		if err := json.Unmarshal(m.Data, &info); err != nil {
			return // TODO(dlc) - Communicate this upwards?
		}
		meta, err := m.Metadata()
		if err != nil {
			return
		}

		if !o.ignoreDeletes || !info.Deleted {
			info.ModTime = meta.Timestamp
			w.updates <- &info
		}

		// if UpdatesOnly is set, no not send nil to the channel
		// as it would always be triggered after initializing the watcher
		if !initDoneMarker && meta.NumPending == 0 {
			initDoneMarker = true
			w.updates <- nil
		}
	}

	allMeta := fmt.Sprintf(objAllMetaPreTmpl, obs.name)
	_, err := obs.stream.GetLastMsgForSubject(ctx, allMeta)
	// if there are no messages on the stream and we are not watching
	// updates only, send nil to the channel to indicate that the initial
	// watch is done
	if !o.updatesOnly {
		if errors.Is(err, ErrMsgNotFound) {
			initDoneMarker = true
			w.updates <- nil
		}
	} else {
		// if UpdatesOnly was used, mark initialization as complete
		initDoneMarker = true
	}

	// Used ordered consumer to deliver results.
	subOpts := []nats.SubOpt{nats.OrderedConsumer()}
	if !o.includeHistory {
		subOpts = append(subOpts, nats.DeliverLastPerSubject())
	}
	if o.updatesOnly {
		subOpts = append(subOpts, nats.DeliverNew())
	}
	subOpts = append(subOpts, nats.Context(ctx))
	sub, err := obs.pushJS.Subscribe(allMeta, update, subOpts...)
	if err != nil {
		return nil, err
	}
	w.sub = sub
	return w, nil
}

// List will list all the objects in this store.
func (obs *obs) List(ctx context.Context, opts ...ListObjectsOpt) ([]*ObjectInfo, error) {
	var o listObjectOpts
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}
	watchOpts := make([]WatchOpt, 0)
	if !o.showDeleted {
		watchOpts = append(watchOpts, IgnoreDeletes())
	}
	watcher, err := obs.Watch(ctx, watchOpts...)
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var objs []*ObjectInfo
	updates := watcher.Updates()
Updates:
	for {
		select {
		case entry := <-updates:
			if entry == nil {
				break Updates
			}
			objs = append(objs, entry)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if len(objs) == 0 {
		return nil, ErrNoObjectsFound
	}
	return objs, nil
}

// ObjectBucketStatus  represents status of a Bucket, implements ObjectStoreStatus
type ObjectBucketStatus struct {
	nfo    *StreamInfo
	bucket string
}

// Bucket is the name of the bucket
func (s *ObjectBucketStatus) Bucket() string { return s.bucket }

// Description is the description supplied when creating the bucket
func (s *ObjectBucketStatus) Description() string { return s.nfo.Config.Description }

// TTL indicates how long objects are kept in the bucket
func (s *ObjectBucketStatus) TTL() time.Duration { return s.nfo.Config.MaxAge }

// Storage indicates the underlying JetStream storage technology used to store data
func (s *ObjectBucketStatus) Storage() StorageType { return s.nfo.Config.Storage }

// Replicas indicates how many storage replicas are kept for the data in the bucket
func (s *ObjectBucketStatus) Replicas() int { return s.nfo.Config.Replicas }

// Sealed indicates the stream is sealed and cannot be modified in any way
func (s *ObjectBucketStatus) Sealed() bool { return s.nfo.Config.Sealed }

// Size is the combined size of all data in the bucket including metadata, in bytes
func (s *ObjectBucketStatus) Size() uint64 { return s.nfo.State.Bytes }

// BackingStore indicates what technology is used for storage of the bucket
func (s *ObjectBucketStatus) BackingStore() string { return "JetStream" }

// Metadata is the metadata supplied when creating the bucket
func (s *ObjectBucketStatus) Metadata() map[string]string { return s.nfo.Config.Metadata }

// StreamInfo is the stream info retrieved to create the status
func (s *ObjectBucketStatus) StreamInfo() *StreamInfo { return s.nfo }

// IsCompressed indicates if the data is compressed on disk
func (s *ObjectBucketStatus) IsCompressed() bool { return s.nfo.Config.Compression != NoCompression }

// Status retrieves run-time status about a bucket
func (obs *obs) Status(ctx context.Context) (ObjectStoreStatus, error) {
	nfo, err := obs.stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	status := &ObjectBucketStatus{
		nfo:    nfo,
		bucket: obs.name,
	}

	return status, nil
}

// Read impl.
func (o *objResult) Read(p []byte) (n int, err error) {
	o.Lock()
	defer o.Unlock()
	readDeadline := time.Now().Add(defaultAPITimeout)
	if ctx := o.ctx; ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			readDeadline = deadline
		}
		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				o.err = ctx.Err()
			} else {
				o.err = nats.ErrTimeout
			}
		default:
		}
	}
	if o.err != nil {
		return 0, o.err
	}
	if o.r == nil {
		return 0, io.EOF
	}

	r := o.r.(net.Conn)
	_ = r.SetReadDeadline(readDeadline)
	n, err = r.Read(p)
	if err, ok := err.(net.Error); ok && err.Timeout() {
		if ctx := o.ctx; ctx != nil {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.Canceled {
					return 0, ctx.Err()
				} else {
					return 0, nats.ErrTimeout
				}
			default:
				err = nil
			}
		}
	}
	if err == io.EOF {
		// Make sure the digest matches.
		sha := o.digest.Sum(nil)
		rsha, decodeErr := DecodeObjectDigest(o.info.Digest)
		if decodeErr != nil {
			o.err = decodeErr
			return 0, o.err
		}
		if !bytes.Equal(sha[:], rsha) {
			o.err = ErrDigestMismatch
			return 0, o.err
		}
	}
	return n, err
}

// Close impl.
func (o *objResult) Close() error {
	o.Lock()
	defer o.Unlock()
	if o.r == nil {
		return nil
	}
	return o.r.Close()
}

func (o *objResult) setErr(err error) {
	o.Lock()
	defer o.Unlock()
	o.err = err
}

func (o *objResult) Info() (*ObjectInfo, error) {
	o.Lock()
	defer o.Unlock()
	return o.info, o.err
}

func (o *objResult) Error() error {
	o.Lock()
	defer o.Unlock()
	return o.err
}

// ObjectStoreNames is used to retrieve a list of bucket names
func (js *jetStream) ObjectStoreNames(ctx context.Context) ObjectStoreNamesLister {
	res := &obsLister{
		obsNames: make(chan string),
	}
	l := &streamLister{js: js}
	streamsReq := streamsRequest{
		Subject: fmt.Sprintf(objAllChunksPreTmpl, "*"),
	}

	go func() {
		defer close(res.obsNames)
		for {
			page, err := l.streamNames(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				res.err = err
				return
			}
			for _, name := range page {
				if !strings.HasPrefix(name, "OBJ_") {
					continue
				}
				res.obsNames <- strings.TrimPrefix(name, "OBJ_")
			}
			if errors.Is(err, ErrEndOfData) {
				return
			}
		}
	}()

	return res
}

// ObjectStores is used to retrieve a list of bucket statuses
func (js *jetStream) ObjectStores(ctx context.Context) ObjectStoresLister {
	res := &obsLister{
		obs: make(chan ObjectStoreStatus),
	}
	l := &streamLister{js: js}
	streamsReq := streamsRequest{
		Subject: fmt.Sprintf(objAllChunksPreTmpl, "*"),
	}
	go func() {
		defer close(res.obs)
		for {
			page, err := l.streamInfos(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				res.err = err
				return
			}
			for _, info := range page {
				if !strings.HasPrefix(info.Config.Name, "OBJ_") {
					continue
				}
				res.obs <- &ObjectBucketStatus{
					nfo:    info,
					bucket: strings.TrimPrefix(info.Config.Name, "OBJ_"),
				}
			}
			if errors.Is(err, ErrEndOfData) {
				return
			}
		}
	}()

	return res
}

type obsLister struct {
	obs      chan ObjectStoreStatus
	obsNames chan string
	err      error
}

func (ol *obsLister) Status() <-chan ObjectStoreStatus {
	return ol.obs
}

func (ol *obsLister) Name() <-chan string {
	return ol.obsNames
}

func (ol *obsLister) Error() error {
	return ol.err
}

func mapStreamToObjectStore(js *jetStream, pushJS nats.JetStreamContext, bucket string, stream Stream) *obs {
	info := stream.CachedInfo()

	obs := &obs{
		name:       bucket,
		js:         js,
		pushJS:     pushJS,
		streamName: info.Config.Name,
		stream:     stream,
	}

	return obs
}
