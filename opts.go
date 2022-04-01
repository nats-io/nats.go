// Copyright 2012-2022 The NATS Authors
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

package nats

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// GetDefaultOptions returns default configuration options for the client.
func GetDefaultOptions() Options {
	return Options{
		AllowReconnect:     true,
		MaxReconnect:       DefaultMaxReconnect,
		ReconnectWait:      DefaultReconnectWait,
		ReconnectJitter:    DefaultReconnectJitter,
		ReconnectJitterTLS: DefaultReconnectJitterTLS,
		Timeout:            DefaultTimeout,
		PingInterval:       DefaultPingInterval,
		MaxPingsOut:        DefaultMaxPingOut,
		SubChanLen:         DefaultMaxChanLen,
		ReconnectBufSize:   DefaultReconnectBufSize,
		DrainTimeout:       DefaultDrainTimeout,
	}
}

// DEPRECATED: Use GetDefaultOptions() instead.
// DefaultOptions is not safe for use by multiple clients.
// For details see #308.
var DefaultOptions = GetDefaultOptions()

// Options can be used to create a customized connection.
type Options struct {

	// Url represents a single NATS server url to which the client
	// will be connecting. If the Servers option is also set, it
	// then becomes the first server in the Servers array.
	Url string

	// Servers is a configured set of servers which this client
	// will use when attempting to connect.
	Servers []string

	// NoRandomize configures whether we will randomize the
	// server pool.
	NoRandomize bool `json:"noRandomize"`

	// NoEcho configures whether the server will echo back messages
	// that are sent on this connection if we also have matching subscriptions.
	// Note this is supported on servers >= version 1.2. Proto 1 or greater.
	NoEcho bool `json:"noEcho"`

	// Name is an optional name label which will be sent to the server
	// on CONNECT to identify the client.
	Name string `json:"name"`

	// Verbose signals the server to send an OK ack for commands
	// successfully processed by the server.
	Verbose bool

	// Pedantic signals the server whether it should be doing further
	// validation of subjects.
	Pedantic bool

	// Secure enables TLS secure connections that skip server
	// verification by default. NOT RECOMMENDED.
	Secure bool

	// TLSConfig is a custom TLS configuration to use for secure
	// transports.
	TLSConfig *tls.Config

	// AllowReconnect enables reconnection logic to be used when we
	// encounter a disconnect from the current server.
	AllowReconnect bool `json:"allowReconnect"`

	// MaxReconnect sets the number of reconnect attempts that will be
	// tried before giving up. If negative, then it will never give up
	// trying to reconnect.
	MaxReconnect int `json:"maxReconnect"`

	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	ReconnectWait time.Duration `json:"reconnectWait"`

	// CustomReconnectDelayCB is invoked after the library tried every
	// URL in the server list and failed to reconnect. It passes to the
	// user the current number of attempts. This function returns the
	// amount of time the library will sleep before attempting to reconnect
	// again. It is strongly recommended that this value contains some
	// jitter to prevent all connections to attempt reconnecting at the same time.
	CustomReconnectDelayCB ReconnectDelayHandler

	// ReconnectJitter sets the upper bound for a random delay added to
	// ReconnectWait during a reconnect when no TLS is used.
	ReconnectJitter time.Duration `json:"reconnectJitter"`

	// ReconnectJitterTLS sets the upper bound for a random delay added to
	// ReconnectWait during a reconnect when TLS is used.
	ReconnectJitterTLS time.Duration `json:"reconnectJitterTLS"`

	// Timeout sets the timeout for a Dial operation on a connection.
	Timeout time.Duration `json:"timeout"`

	// DrainTimeout sets the timeout for a Drain Operation to complete.
	DrainTimeout time.Duration `json:"drainTimeout"`

	// FlusherTimeout is the maximum time to wait for write operations
	// to the underlying connection to complete (including the flusher loop).
	FlusherTimeout time.Duration `json:"flusherTimeout"`

	// PingInterval is the period at which the client will be sending ping
	// commands to the server, disabled if 0 or negative.
	PingInterval time.Duration `json:"pingInterval"`

	// MaxPingsOut is the maximum number of pending ping commands that can
	// be awaiting a response before raising an ErrStaleConnection error.
	MaxPingsOut int `json:"maxPingsOut"`

	// ClosedCB sets the closed handler that is called when a client will
	// no longer be connected.
	ClosedCB ConnHandler

	// DisconnectedCB sets the disconnected handler that is called
	// whenever the connection is disconnected.
	// Will not be called if DisconnectedErrCB is set
	// DEPRECATED. Use DisconnectedErrCB which passes error that caused
	// the disconnect event.
	DisconnectedCB ConnHandler

	// DisconnectedErrCB sets the disconnected error handler that is called
	// whenever the connection is disconnected.
	// Disconnected error could be nil, for instance when user explicitly closes the connection.
	// DisconnectedCB will not be called if DisconnectedErrCB is set
	DisconnectedErrCB ConnErrHandler

	// ReconnectedCB sets the reconnected handler called whenever
	// the connection is successfully reconnected.
	ReconnectedCB ConnHandler

	// DiscoveredServersCB sets the callback that is invoked whenever a new
	// server has joined the cluster.
	DiscoveredServersCB ConnHandler

	// AsyncErrorCB sets the async error handler (e.g. slow consumer errors)
	AsyncErrorCB ErrHandler

	// ReconnectBufSize is the size of the backing bufio during reconnect.
	// Once this has been exhausted publish operations will return an error.
	ReconnectBufSize int `json:"reconnectBufSize"`

	// SubChanLen is the size of the buffered channel used between the socket
	// Go routine and the message delivery for SyncSubscriptions.
	// NOTE: This does not affect AsyncSubscriptions which are
	// dictated by PendingLimits()
	SubChanLen int

	// UserJWT sets the callback handler that will fetch a user's JWT.
	UserJWT UserJWTHandler

	// Nkey sets the public nkey that will be used to authenticate
	// when connecting to the server. UserJWT and Nkey are mutually exclusive
	// and if defined, UserJWT will take precedence.
	Nkey string

	// SignatureCB designates the function used to sign the nonce
	// presented from the server.
	SignatureCB SignatureHandler

	// User sets the username to be used when connecting to the server.
	User string

	// Password sets the password to be used when connecting to a server.
	Password string

	// Token sets the token to be used when connecting to a server.
	Token string

	// TokenHandler designates the function used to generate the token to be used when connecting to a server.
	TokenHandler AuthTokenHandler

	// Dialer allows a custom net.Dialer when forming connections.
	// DEPRECATED: should use CustomDialer instead.
	Dialer *net.Dialer

	// CustomDialer allows to specify a custom dialer (not necessarily
	// a *net.Dialer).
	CustomDialer CustomDialer

	// UseOldRequestStyle forces the old method of Requests that utilize
	// a new Inbox and a new Subscription for each request.
	UseOldRequestStyle bool `json:"useOldRequestStyle"`

	// NoCallbacksAfterClientClose allows preventing the invocation of
	// callbacks after Close() is called. Client won't receive notifications
	// when Close is invoked by user code. Default is to invoke the callbacks.
	NoCallbacksAfterClientClose bool `json:"noCallbacksAfterClientClose"`

	// LameDuckModeHandler sets the callback to invoke when the server notifies
	// the connection that it entered lame duck mode, that is, going to
	// gradually disconnect all its connections before shutting down. This is
	// often used in deployments when upgrading NATS Servers.
	LameDuckModeHandler ConnHandler

	// RetryOnFailedConnect sets the connection in reconnecting state right
	// away if it can't connect to a server in the initial set. The
	// MaxReconnect and ReconnectWait options are used for this process,
	// similarly to when an established connection is disconnected.
	// If a ReconnectHandler is set, it will be invoked when the connection
	// is established, and if a ClosedHandler is set, it will be invoked if
	// it fails to connect (after exhausting the MaxReconnect attempts).
	RetryOnFailedConnect bool `json:"retryOnFailedConnect"`

	// For websocket connections, indicates to the server that the connection
	// supports compression. If the server does too, then data will be compressed.
	Compression bool `json:"compression"`

	// InboxPrefix allows the default _INBOX prefix to be customized
	InboxPrefix string `json:"inboxPrefix"`
}

// Option is a function on the options for a connection.
type Option func(*Options) error

// Process the url string argument to Connect.  Returns an array of
// urls, even if only one.
func processUrlString(url string) []string {
	urls := strings.Split(url, ",")
	for i, s := range urls {
		urls[i] = strings.TrimSpace(s)
	}
	return urls
}

// processOptions takes an URI string and a variadic number of options.
func processOptions(u string, options ...Option) (*Options, error) {
	urls := processUrlString(u)
	opts := GetDefaultOptions()
	servers := make([]string, len(urls))

	// Durations have to be in seconds:
	//
	// nats://demo.nats.io?timeout=10
	//
	parseDurationInSecs := func(opt, qs string) (time.Duration, error) {
		duration, err := strconv.ParseFloat(qs, 64)
		if err != nil {
			return 0, fmt.Errorf("nats: invalid duration %v for %q option", opt, qs)
		}
		return time.Duration(duration) * time.Second, nil
	}

	// Boolean values are the same supported by strconv.ParseBool
	// so values like 1, true, 0, false all should work.
	//
	// nats://demo.nats.io?allowReconnect=true
	// nats://demo.nats.io?allowReconnect=0
	//
	// parseBool := func(opt, qs string) (bool, error) {
	// 	b, err := strconv.ParseBool(qs)
	// 	if err != nil {
	// 		return false, fmt.Errorf("nats: invalid value %v for %q option", opt, qs)
	// 	}
	// 	return b, nil
	// }

	// First apply URI based query parameters, these can then be
	// overriden by the functional options below.
	for i, u := range urls {
		purl, err := url.Parse(u)
		if err != nil {
			return nil, fmt.Errorf("nats: invalid url %v", u)
		}
		for k, v := range purl.Query() {
			if len(v) > 1 {
				return nil, fmt.Errorf("nats: multiple uri options for %q", k)
			}

			field := strings.ToLower(k)
			switch field {
			case "timeout":
				duration, err := parseDurationInSecs(k, v[0])
				if err != nil {
					return nil, err
				}
				opts.Timeout = duration
			}
		}

		// Normalize a bit the resulting URL for servers. Add default scheme if missing,
		// and clear out any query values before capturing the actual strings used to
		// connect.
		if purl.IsAbs() {
			purl.Scheme = "nats"
		}
		purl.Path = ""
		purl.RawPath = ""
		purl.RawQuery = ""
		purl.Fragment = ""
		purl.RawFragment = ""
		servers[i] = purl.String()
	}
	opts.Servers = servers

	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	return &opts, nil
}

// Options that can be passed to Connect.

// Name is an Option to set the client name.
func Name(name string) Option {
	return func(o *Options) error {
		o.Name = name
		return nil
	}
}

// Secure is an Option to enable TLS secure connections that skip server verification by default.
// Pass a TLS Configuration for proper TLS.
// NOTE: This should NOT be used in a production setting.
func Secure(tls ...*tls.Config) Option {
	return func(o *Options) error {
		o.Secure = true
		// Use of variadic just simplifies testing scenarios. We only take the first one.
		if len(tls) > 1 {
			return ErrMultipleTLSConfigs
		}
		if len(tls) == 1 {
			o.TLSConfig = tls[0]
		}
		return nil
	}
}

// RootCAs is a helper option to provide the RootCAs pool from a list of filenames.
// If Secure is not already set this will set it as well.
func RootCAs(file ...string) Option {
	return func(o *Options) error {
		pool := x509.NewCertPool()
		for _, f := range file {
			rootPEM, err := ioutil.ReadFile(f)
			if err != nil || rootPEM == nil {
				return fmt.Errorf("nats: error loading or parsing rootCA file: %v", err)
			}
			ok := pool.AppendCertsFromPEM(rootPEM)
			if !ok {
				return fmt.Errorf("nats: failed to parse root certificate from %q", f)
			}
		}
		if o.TLSConfig == nil {
			o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		o.TLSConfig.RootCAs = pool
		o.Secure = true
		return nil
	}
}

// ClientCert is a helper option to provide the client certificate from a file.
// If Secure is not already set this will set it as well.
func ClientCert(certFile, keyFile string) Option {
	return func(o *Options) error {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("nats: error loading client certificate: %v", err)
		}
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return fmt.Errorf("nats: error parsing client certificate: %v", err)
		}
		if o.TLSConfig == nil {
			o.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		o.TLSConfig.Certificates = []tls.Certificate{cert}
		o.Secure = true
		return nil
	}
}

// NoReconnect is an Option to turn off reconnect behavior.
func NoReconnect() Option {
	return func(o *Options) error {
		o.AllowReconnect = false
		return nil
	}
}

// DontRandomize is an Option to turn off randomizing the server pool.
func DontRandomize() Option {
	return func(o *Options) error {
		o.NoRandomize = true
		return nil
	}
}

// NoEcho is an Option to turn off messages echoing back from a server.
// Note this is supported on servers >= version 1.2. Proto 1 or greater.
func NoEcho() Option {
	return func(o *Options) error {
		o.NoEcho = true
		return nil
	}
}

// ReconnectWait is an Option to set the wait time between reconnect attempts.
func ReconnectWait(t time.Duration) Option {
	return func(o *Options) error {
		o.ReconnectWait = t
		return nil
	}
}

// MaxReconnects is an Option to set the maximum number of reconnect attempts.
func MaxReconnects(max int) Option {
	return func(o *Options) error {
		o.MaxReconnect = max
		return nil
	}
}

// ReconnectJitter is an Option to set the upper bound of a random delay added ReconnectWait.
func ReconnectJitter(jitter, jitterForTLS time.Duration) Option {
	return func(o *Options) error {
		o.ReconnectJitter = jitter
		o.ReconnectJitterTLS = jitterForTLS
		return nil
	}
}

// CustomReconnectDelay is an Option to set the CustomReconnectDelayCB option.
// See CustomReconnectDelayCB Option for more details.
func CustomReconnectDelay(cb ReconnectDelayHandler) Option {
	return func(o *Options) error {
		o.CustomReconnectDelayCB = cb
		return nil
	}
}

// PingInterval is an Option to set the period for client ping commands.
func PingInterval(t time.Duration) Option {
	return func(o *Options) error {
		o.PingInterval = t
		return nil
	}
}

// MaxPingsOutstanding is an Option to set the maximum number of ping requests
// that can go unanswered by the server before closing the connection.
func MaxPingsOutstanding(max int) Option {
	return func(o *Options) error {
		o.MaxPingsOut = max
		return nil
	}
}

// ReconnectBufSize sets the buffer size of messages kept while busy reconnecting.
func ReconnectBufSize(size int) Option {
	return func(o *Options) error {
		o.ReconnectBufSize = size
		return nil
	}
}

// Timeout is an Option to set the timeout for Dial on a connection.
func Timeout(t time.Duration) Option {
	return func(o *Options) error {
		o.Timeout = t
		return nil
	}
}

// FlusherTimeout is an Option to set the write (and flush) timeout on a connection.
func FlusherTimeout(t time.Duration) Option {
	return func(o *Options) error {
		o.FlusherTimeout = t
		return nil
	}
}

// DrainTimeout is an Option to set the timeout for draining a connection.
func DrainTimeout(t time.Duration) Option {
	return func(o *Options) error {
		o.DrainTimeout = t
		return nil
	}
}

// DisconnectErrHandler is an Option to set the disconnected error handler.
func DisconnectErrHandler(cb ConnErrHandler) Option {
	return func(o *Options) error {
		o.DisconnectedErrCB = cb
		return nil
	}
}

// DisconnectHandler is an Option to set the disconnected handler.
// DEPRECATED: Use DisconnectErrHandler.
func DisconnectHandler(cb ConnHandler) Option {
	return func(o *Options) error {
		o.DisconnectedCB = cb
		return nil
	}
}

// ReconnectHandler is an Option to set the reconnected handler.
func ReconnectHandler(cb ConnHandler) Option {
	return func(o *Options) error {
		o.ReconnectedCB = cb
		return nil
	}
}

// ClosedHandler is an Option to set the closed handler.
func ClosedHandler(cb ConnHandler) Option {
	return func(o *Options) error {
		o.ClosedCB = cb
		return nil
	}
}

// DiscoveredServersHandler is an Option to set the new servers handler.
func DiscoveredServersHandler(cb ConnHandler) Option {
	return func(o *Options) error {
		o.DiscoveredServersCB = cb
		return nil
	}
}

// ErrorHandler is an Option to set the async error handler.
func ErrorHandler(cb ErrHandler) Option {
	return func(o *Options) error {
		o.AsyncErrorCB = cb
		return nil
	}
}

// UserInfo is an Option to set the username and password to
// use when not included directly in the URLs.
func UserInfo(user, password string) Option {
	return func(o *Options) error {
		o.User = user
		o.Password = password
		return nil
	}
}

// Token is an Option to set the token to use
// when a token is not included directly in the URLs
// and when a token handler is not provided.
func Token(token string) Option {
	return func(o *Options) error {
		if o.TokenHandler != nil {
			return ErrTokenAlreadySet
		}
		o.Token = token
		return nil
	}
}

// TokenHandler is an Option to set the token handler to use
// when a token is not included directly in the URLs
// and when a token is not set.
func TokenHandler(cb AuthTokenHandler) Option {
	return func(o *Options) error {
		if o.Token != "" {
			return ErrTokenAlreadySet
		}
		o.TokenHandler = cb
		return nil
	}
}

// UserCredentials is a convenience function that takes a filename
// for a user's JWT and a filename for the user's private Nkey seed.
func UserCredentials(userOrChainedFile string, seedFiles ...string) Option {
	userCB := func() (string, error) {
		return userFromFile(userOrChainedFile)
	}
	var keyFile string
	if len(seedFiles) > 0 {
		keyFile = seedFiles[0]
	} else {
		keyFile = userOrChainedFile
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		return sigHandler(nonce, keyFile)
	}
	return UserJWT(userCB, sigCB)
}

// UserJWT will set the callbacks to retrieve the user's JWT and
// the signature callback to sign the server nonce. This an the Nkey
// option are mutually exclusive.
func UserJWT(userCB UserJWTHandler, sigCB SignatureHandler) Option {
	return func(o *Options) error {
		if userCB == nil {
			return ErrNoUserCB
		}
		if sigCB == nil {
			return ErrUserButNoSigCB
		}
		o.UserJWT = userCB
		o.SignatureCB = sigCB
		return nil
	}
}

// Nkey will set the public Nkey and the signature callback to
// sign the server nonce.
func Nkey(pubKey string, sigCB SignatureHandler) Option {
	return func(o *Options) error {
		o.Nkey = pubKey
		o.SignatureCB = sigCB
		if pubKey != "" && sigCB == nil {
			return ErrNkeyButNoSigCB
		}
		return nil
	}
}

// SyncQueueLen will set the maximum queue len for the internal
// channel used for SubscribeSync().
func SyncQueueLen(max int) Option {
	return func(o *Options) error {
		o.SubChanLen = max
		return nil
	}
}

// Dialer is an Option to set the dialer whicxh will be used when
// attempting to establish a connection.
// DEPRECATED: Should use CustomDialer instead.
func Dialer(dialer *net.Dialer) Option {
	return func(o *Options) error {
		o.Dialer = dialer
		return nil
	}
}

// CustomDialer can be used to specify any dialer, not necessarily
// a *net.Dialer.
type CustomDialer interface {
	Dial(network, address string) (net.Conn, error)
}

// SetCustomDialer is an Option to set a custom dialer which will be
// used when attempting to establish a connection. If both Dialer
// and CustomDialer are specified, CustomDialer takes precedence.
func SetCustomDialer(dialer CustomDialer) Option {
	return func(o *Options) error {
		o.CustomDialer = dialer
		return nil
	}
}

// UseOldRequestStyle is an Option to force usage of the old Request style.
func UseOldRequestStyle() Option {
	return func(o *Options) error {
		o.UseOldRequestStyle = true
		return nil
	}
}

// NoCallbacksAfterClientClose is an Option to disable callbacks when user code
// calls Close(). If close is initiated by any other condition, callbacks
// if any will be invoked.
func NoCallbacksAfterClientClose() Option {
	return func(o *Options) error {
		o.NoCallbacksAfterClientClose = true
		return nil
	}
}

// LameDuckModeHandler sets the callback to invoke when the server notifies
// the connection that it entered lame duck mode, that is, going to
// gradually disconnect all its connections before shutting down. This is
// often used in deployments when upgrading NATS Servers.
func LameDuckModeHandler(cb ConnHandler) Option {
	return func(o *Options) error {
		o.LameDuckModeHandler = cb
		return nil
	}
}

// RetryOnFailedConnect sets the connection in reconnecting state right away
// if it can't connect to a server in the initial set.
// See RetryOnFailedConnect option for more details.
func RetryOnFailedConnect(retry bool) Option {
	return func(o *Options) error {
		o.RetryOnFailedConnect = retry
		return nil
	}
}

// Compression is an Option to indicate if this connection supports
// compression. Currently only supported for Websocket connections.
func Compression(enabled bool) Option {
	return func(o *Options) error {
		o.Compression = enabled
		return nil
	}
}

// CustomInboxPrefix configures the request + reply inbox prefix
func CustomInboxPrefix(p string) Option {
	return func(o *Options) error {
		if p == "" || strings.Contains(p, ">") || strings.Contains(p, "*") || strings.HasSuffix(p, ".") {
			return fmt.Errorf("nats: invald custom prefix")
		}
		o.InboxPrefix = p
		return nil
	}
}
