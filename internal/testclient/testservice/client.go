// Copyright 2026 The NATS Authors
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

package testservice

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/api"
)

type Client struct {
	address string
	nc      *nats.Conn
}

// Instance is a client-side handle to a single managed instance (server,
// cluster, or super-cluster) hosted by the management service. Stop/Start
// /Status/Destroy are scoped to this instance, which makes parallel tests
// safe.
type Instance struct {
	ID          string
	Description string
	Kind        string
	Servers     []*api.ManagedServer
	// TLS carries the CA cert and (for mutual TLS) client cert/key minted
	// by the service when WithGeneratedTLS was set on the create call.
	// Nil otherwise. Use TLSConfig(inst) to build a *tls.Config from it.
	TLS *api.TLSMaterial

	c *Client
}

// CreateOption customizes a Create* call.
type CreateOption interface{ applyCreate(*createOptions) }

// UpdateOption customizes an UpdateServer call.
type UpdateOption interface{ applyUpdate(*updateOptions) }

type createOptions struct {
	description    string
	snippets       map[string]string
	template       string
	connectOptions []nats.Option
	tls            *api.TLSOptions
}

// TLSOpt customizes a WithGeneratedTLS call.
type TLSOpt func(*api.TLSOptions)

// WithGeneratedTLS asks the service to mint a CA, server cert, and (for
// mutual TLS) client cert, and to listen with TLS on every server's client
// port. Gateways and routes stay plaintext. The CA cert plus client cert/key
// are returned on Instance.TLS; the convenience helpers (WithServer,
// WithCluster, WithSuperCluster, JetStream variants) auto-wire the returned
// material into their nats.Connect, so a TLS test is a one-liner.
//
// Defaults to mutual TLS and SANs ["localhost","127.0.0.1","::1"]. Override
// with TLSServerOnly, TLSMutual, TLSSANs, TLSHandshakeFirst.
//
// Mutually exclusive with WithTLSSnippet — the service rejects requests that
// set both.
func WithGeneratedTLS(opts ...TLSOpt) CreateOption {
	return createOpt(func(o *createOptions) {
		t := &api.TLSOptions{Mode: api.TLSModeMutual}
		for _, opt := range opts {
			opt(t)
		}
		o.tls = t
	})
}

// TLSMutual selects mutual TLS — the server requires the issued client cert.
// This is the default for WithGeneratedTLS; the helper exists for symmetry
// with TLSServerOnly.
func TLSMutual() TLSOpt {
	return func(t *api.TLSOptions) { t.Mode = api.TLSModeMutual }
}

// TLSServerOnly selects one-way TLS — the server presents its cert but does
// not require a client cert.
func TLSServerOnly() TLSOpt {
	return func(t *api.TLSOptions) { t.Mode = api.TLSModeServer }
}

// TLSSANs overrides the default Subject Alternative Names for the generated
// server cert. Strings that parse as IPs are placed in IPAddresses; the rest
// become DNSNames. Use when tests dial a hostname other than localhost (CI
// runner names, container hostnames, etc.).
func TLSSANs(sans ...string) TLSOpt {
	return func(t *api.TLSOptions) { t.SANs = append([]string(nil), sans...) }
}

// TLSHandshakeFirst makes the server complete the TLS handshake before sending
// the INFO protocol. The convenience helpers dial with nats.TLSHandshakeFirst
// automatically; if you dial the returned servers yourself, pass that option
// to your own nats.Connect alongside nats.Secure or the connection will hang.
func TLSHandshakeFirst() TLSOpt {
	return func(t *api.TLSOptions) { t.HandshakeFirst = true }
}

// TLSConfig builds a *tls.Config from an Instance's returned TLS material:
// RootCAs from the CA cert, and (when mutual TLS was requested) a single
// X509KeyPair from the client cert and key. Returns an error if inst.TLS
// is nil or the PEM material is malformed.
func TLSConfig(inst *Instance) (*tls.Config, error) {
	if inst == nil || inst.TLS == nil {
		return nil, fmt.Errorf("instance has no TLS material; was WithGeneratedTLS set on the create call?")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(inst.TLS.CAPEM)) {
		return nil, fmt.Errorf("could not load CA PEM into trust pool")
	}
	cfg := &tls.Config{RootCAs: pool}
	if inst.TLS.ClientCertPEM != "" {
		cert, err := tls.X509KeyPair([]byte(inst.TLS.ClientCertPEM), []byte(inst.TLS.ClientKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("could not load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

type updateOptions struct {
	snippets map[string]string
	template string
}

// snippetOpt is the underlying type of every helper that customizes the
// rendered config. It satisfies both CreateOption and UpdateOption so the
// same With* helper can be passed to either CreateServer or UpdateServer.
type snippetOpt struct {
	key    string // snippet key when isTmpl is false; ignored when isTmpl is true
	body   string
	isTmpl bool // if true, body replaces the main template
}

func (s snippetOpt) applyCreate(o *createOptions) {
	if s.isTmpl {
		o.template = s.body
		return
	}
	if o.snippets == nil {
		o.snippets = map[string]string{}
	}
	o.snippets[s.key] = s.body
}

func (s snippetOpt) applyUpdate(o *updateOptions) {
	if s.isTmpl {
		o.template = s.body
		return
	}
	if o.snippets == nil {
		o.snippets = map[string]string{}
	}
	o.snippets[s.key] = s.body
}

// createOpt is the underlying type for helpers that only customize Create*
// calls.
type createOpt func(*createOptions)

func (f createOpt) applyCreate(o *createOptions) { f(o) }

// WithDescription attaches a human-readable label to a managed instance. It
// round-trips on the wire and surfaces in tester.list / tester.status — useful
// for spotting which test owns which instance in service logs.
func WithDescription(d string) createOpt {
	return func(o *createOptions) { o.description = d }
}

// WithConnectOptions ferries nats.Option values through to the nats.Connect
// call that the convenience helpers (WithServer, WithCluster,
// WithSuperCluster, and their JetStream variants) make against the managed
// instance. Use it when the instance configuration requires creds, TLS, or
// any other client-side option to connect — e.g. paired with WithAccounts +
// WithAuthorization to authenticate as a custom user. Has no effect when
// you call CreateServer / CreateCluster / CreateSuperCluster directly and
// dial the returned servers yourself.
func WithConnectOptions(opts ...nats.Option) createOpt {
	return func(o *createOptions) { o.connectOptions = append(o.connectOptions, opts...) }
}

// WithAccounts replaces the built-in USERS1..USERS5 / $SYS accounts block
// with the caller's accounts configuration. Keep a $SYS account if you don't
// override system_account, and keep the user named by no_auth_user (default
// user1) — or override authorization too with WithAuthorization.
func WithAccounts(body string) snippetOpt { return snippetOpt{key: "accounts", body: body} }

// WithSystemAccount replaces the built-in `system_account: "$SYS"` line with
// the caller's directive. Pair with WithAccounts when the new accounts block
// uses a different system-account name.
func WithSystemAccount(body string) snippetOpt {
	return snippetOpt{key: "system_account", body: body}
}

// WithAuthorization replaces the built-in `no_auth_user: user1` line with the
// caller's authorization block.
func WithAuthorization(body string) snippetOpt {
	return snippetOpt{key: "authorization", body: body}
}

// WithTLSSnippet adds a top-level TLS block (client TLS) from a caller-
// supplied snippet body. This is the escape hatch for tests that need to
// pin specific certs or ciphers; for the common "just turn TLS on" case
// use WithGeneratedTLS, which has the service mint a CA and certificates
// and auto-wires the convenience helpers' nats.Connect.
//
// WithTLSSnippet and WithGeneratedTLS are mutually exclusive — the service
// rejects requests that combine them.
//
// Like the other snippet helpers it satisfies both CreateOption and
// UpdateOption, so it can also be passed to UpdateServer to swap the TLS block
// of an existing instance (pair with ReloadServer to apply it live).
func WithTLSSnippet(body string) snippetOpt { return snippetOpt{key: "tls", body: body} }

// WithWebSocket adds a top-level websocket block. The service automatically
// reserves a TCP port named "websocket" on every server in the instance and
// exposes it as .Ports.websocket in the template env, so the caller's body
// typically renders `port: {{ .Ports.websocket }}`. The reserved port surfaces
// on ManagedServer.Ports["websocket"] for tests to dial via ws://.
//
// The websocket listener's TLS is independent of the client-port TLS that
// WithGeneratedTLS configures. For wss, pair this with WithGeneratedTLS and
// give the body its own tls{} block referencing the generated cert paths via
// .TLS.CertFile/.TLS.KeyFile (and .TLS.CAFile for client-cert verification),
// then dial wss:// with nats.Secure(TLSConfig(inst)).
func WithWebSocket(body string) snippetOpt { return snippetOpt{key: "websocket", body: body} }

// WithMQTT adds a top-level mqtt block. The service automatically reserves a
// TCP port named "mqtt" and exposes it as .Ports.mqtt in the template env.
// The reserved port surfaces on ManagedServer.Ports["mqtt"].
func WithMQTT(body string) snippetOpt { return snippetOpt{key: "mqtt", body: body} }

// WithLeafNode adds a top-level leafnode block. The service automatically
// reserves a TCP port named "leafnode" and exposes it as .Ports.leafnode in
// the template env. The reserved port surfaces on
// ManagedServer.Ports["leafnode"].
func WithLeafNode(body string) snippetOpt { return snippetOpt{key: "leafnode", body: body} }

// WithJetStream supplies extra keys for the server's jetstream { } block, such
// as domain or JetStream limits. The body is merged inside the block the
// service renders, so the service keeps owning `enabled` and `store_dir` (the
// store dir is available as .StoreDir if you need to reference it) — the body
// carries only the extras. Requires a JetStream-enabled instance.
func WithJetStream(body string) snippetOpt { return snippetOpt{key: "jetstream", body: body} }

// WithTopLevel adds free-form top-level lines to the rendered config (limits,
// debug, max_payload, …). Rendered above server_name in the merged config so
// settings that must appear before the rest of the config are honored.
func WithTopLevel(body string) snippetOpt { return snippetOpt{key: "top", body: body} }

// WithTemplate replaces the built-in main config template with the caller's
// body. Rendered through text/template against the same env exposed to
// snippets (.ClusterName, .Routes, .Gateways, .Ports.<name>, .Snippets.<name>,
// …), so the user's template has full access to topology data — but it
// becomes responsible for emitting correct cluster {…} / gateway {…} blocks;
// the service no longer guarantees topology correctness when this is set.
//
// Reach for this only when no typed snippet helper expresses the change.
// Composes with the typed helpers: snippet files are still rendered to disk
// and their paths exposed via .Snippets.<name>, so the custom template can
// include them.
func WithTemplate(body string) snippetOpt { return snippetOpt{body: body, isTmpl: true} }

func resolveCreateOptions(t testing.TB, opts []CreateOption) createOptions {
	co := createOptions{description: t.Name()}
	for _, o := range opts {
		o.applyCreate(&co)
	}
	return co
}

func resolveUpdateOptions(opts []UpdateOption) updateOptions {
	uo := updateOptions{}
	for _, o := range opts {
		o.applyUpdate(&uo)
	}
	return uo
}

// helperConnectOptions assembles the nats.Option slice used by the
// convenience helpers. nats.Secure(cfg) is prepended when co.tls is set so
// the post-create Connect uses TLS; user-supplied co.connectOptions compose
// last and win on overlap.
func helperConnectOptions(t *testing.T, co createOptions, inst *Instance) []nats.Option {
	t.Helper()
	opts := []nats.Option{nats.MaxReconnects(-1)}
	if co.tls != nil {
		cfg, err := TLSConfig(inst)
		if err != nil {
			t.Fatalf("could not build TLS config: %v", err)
		}
		opts = append(opts, nats.Secure(cfg))
		if co.tls.HandshakeFirst {
			opts = append(opts, nats.TLSHandshakeFirst())
		}
	}
	opts = append(opts, co.connectOptions...)
	return opts
}

// New connects to the management service of the test cluster manager
func New(t testing.TB, server string, opts ...nats.Option) *Client {
	t.Helper()

	u, err := url.Parse(server)
	if err != nil {
		t.Fatalf("could not parse server URL: %v", err)
	}

	nopts := []nats.Option{
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1),
	}

	nc, err := nats.Connect(server, append(nopts, opts...)...)
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	return &Client{nc: nc, address: u.Hostname()}
}

// WithJetStreamServer creates a server running JetStream and connects to it.
// Pass CreateOptions to customize the rendered config (e.g. WithAccounts) and
// the post-create nats.Connect (e.g. WithConnectOptions(nats.UserInfo(...))).
func (c *Client) WithJetStreamServer(t *testing.T, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withServer(t, true, h, opts...)
}

// WithServer creates a non JetStream server and connects to it. Pass
// CreateOptions to customize the rendered config and the post-create
// nats.Connect.
func (c *Client) WithServer(t *testing.T, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withServer(t, false, h, opts...)
}

func (c *Client) withServer(t *testing.T, js bool, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	inst := c.CreateServer(t, js, opts...)
	defer inst.Destroy(t)

	nc, err := nats.Connect(inst.Servers[0].URL, helperConnectOptions(t, co, inst)...)
	if err != nil {
		t.Fatal("failed to connect to NATS:", err)
	}
	defer nc.Close()

	h(t, nc, inst)
}

// WithJetStreamCluster creates a cluster with the given server count running
// JetStream and connects to a random server. Pass CreateOptions to customize
// the rendered config and the post-create nats.Connect.
func (c *Client) WithJetStreamCluster(t *testing.T, servers int, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withCluster(t, servers, true, h, opts...)
}

// WithCluster creates a non JetStream cluster with the given server count and
// connects to a random server. Pass CreateOptions to customize the rendered
// config and the post-create nats.Connect.
func (c *Client) WithCluster(t *testing.T, servers int, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withCluster(t, servers, false, h, opts...)
}

func (c *Client) withCluster(t *testing.T, servers int, js bool, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	inst := c.CreateCluster(t, servers, js, opts...)
	defer inst.Destroy(t)

	if len(inst.Servers) != servers {
		t.Fatalf("expected number of servers to be %d got %d", servers, len(inst.Servers))
	}

	nc, err := nats.Connect(inst.RandomServer().URL, helperConnectOptions(t, co, inst)...)
	if err != nil {
		t.Fatal("failed to connect to NATS:", err)
	}
	defer nc.Close()

	if js {
		c.WaitForJetStream(t, nc)
	}

	h(t, nc, inst)
}

// WaitForJetStream polls '$JS.API.INFO' regularly waiting for Jetstream to be ready, fails after 5 seconds
func (c *Client) WaitForJetStream(t testing.TB, nc *nats.Conn) {
	t.Helper()

	for i := range 10 {
		_, err := nc.Request("$JS.API.INFO", nil, time.Second)
		if err == nil {
			return
		}

		time.Sleep(500 * time.Millisecond)

		if i == 9 {
			t.Fatalf("jetstream did not become ready")
		}
	}
}

// WithJetStreamSuperCluster creates a super-cluster with the given server and
// cluster counts running JetStream and connects to a random server. Pass
// CreateOptions to customize the rendered config and the post-create
// nats.Connect.
func (c *Client) WithJetStreamSuperCluster(t *testing.T, clusters int, servers int, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withSuperCluster(t, clusters, servers, true, h, opts...)
}

// WithSuperCluster creates a non JetStream super-cluster with the given server
// and cluster counts and connects to a random server. Pass CreateOptions to
// customize the rendered config and the post-create nats.Connect.
func (c *Client) WithSuperCluster(t *testing.T, clusters int, servers int, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	c.withSuperCluster(t, clusters, servers, false, h, opts...)
}

func (c *Client) withSuperCluster(t *testing.T, clusters int, servers int, js bool, h func(*testing.T, *nats.Conn, *Instance), opts ...CreateOption) {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	inst := c.CreateSuperCluster(t, clusters, servers, js, opts...)
	defer inst.Destroy(t)

	if len(inst.Servers) != servers*clusters {
		t.Fatalf("expected number of servers to be %d got %d", servers*clusters, len(inst.Servers))
	}

	nc, err := nats.Connect(inst.RandomServer().URL, helperConnectOptions(t, co, inst)...)
	if err != nil {
		t.Fatal("failed to connect to NATS:", err)
	}
	defer nc.Close()

	if js {
		c.WaitForJetStream(t, nc)
	}

	h(t, nc, inst)
}

// CreateSuperCluster creates a super cluster
func (c *Client) CreateSuperCluster(t testing.TB, clusters int, servers int, js bool, opts ...CreateOption) *Instance {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	jreq, err := json.Marshal(api.CreateSuperClusterRequest{
		JetStream:   js,
		Clusters:    clusters,
		Servers:     servers,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
	})
	if err != nil {
		t.Fatalf("could not marshal CreateSuperClusterRequest: %v", err)
	}

	return c.doCreate(t, "tester.create.super-cluster", jreq)
}

// CreateCluster creates a cluster
func (c *Client) CreateCluster(t testing.TB, servers int, js bool, opts ...CreateOption) *Instance {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	jreq, err := json.Marshal(api.CreateClusterRequest{
		JetStream:   js,
		Servers:     servers,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
	})
	if err != nil {
		t.Fatalf("could not marshal CreateClusterRequest: %v", err)
	}

	return c.doCreate(t, "tester.create.cluster", jreq)
}

// CreateServer creates a server
func (c *Client) CreateServer(t testing.TB, js bool, opts ...CreateOption) *Instance {
	t.Helper()

	co := resolveCreateOptions(t, opts)
	jreq, err := json.Marshal(api.CreateServerRequest{
		JetStream:   js,
		Description: co.description,
		Snippets:    co.snippets,
		Template:    co.template,
		TLS:         co.tls,
	})
	if err != nil {
		t.Fatalf("could not marshal CreateServerRequest: %v", err)
	}

	return c.doCreate(t, "tester.create.server", jreq)
}

func (c *Client) doCreate(t testing.TB, subject string, jreq []byte) *Instance {
	t.Helper()

	msg, err := c.nc.Request(subject, jreq, 30*time.Second)
	if err != nil {
		t.Fatalf("could not send create request to %s: %v", subject, err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request to %s failed: %v", subject, e)
	}

	resp := api.CreateResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal CreateResponse: %v: %v", string(msg.Data), err)
	}

	for _, srv := range resp.Servers {
		srv.URL = fmt.Sprintf("nats://%s:%d", c.address, srv.Port)
	}

	return &Instance{
		ID:          resp.ID,
		Description: resp.Description,
		Kind:        resp.Kind,
		Servers:     resp.Servers,
		TLS:         resp.TLS,
		c:           c,
	}
}

// List returns a lightweight summary of every instance currently held by the
// management service.
func (c *Client) List(t testing.TB) *api.ListResponse {
	t.Helper()

	msg, err := c.nc.Request("tester.list", nil, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send ListRequest: %v", err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request failed: %v", e)
	}

	resp := api.ListResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal ListResponse: %v: %v", string(msg.Data), err)
	}
	return &resp
}

// Reset shuts down and removes all servers across every instance. Use sparingly
// — most tests should call inst.Destroy() to scope cleanup to the instance they
// own. Reset remains for CI safety nets between job stages.
func (c *Client) Reset(t testing.TB) api.ResetResponse {
	t.Helper()

	msg, err := c.nc.Request("tester.reset", nil, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send ResetRequest: %v", err)
	}

	if err := msg.Header.Get("Nats-Service-Error"); err != "" {
		t.Fatalf("Request failed: %v", err)
	}

	resp := api.ResetResponse{}
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		t.Fatalf("could not unmarshal ResetResponse: %v: %v", string(msg.Data), err)
	}

	return resp
}

// Status returns status of all instances managed by the tester. Use
// inst.Status() if you only care about a single instance.
func (c *Client) Status(t testing.TB) *api.StatusResponse {
	t.Helper()

	msg, err := c.nc.Request("tester.status", nil, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send StatusRequest: %v", err)
	}

	if err := msg.Header.Get("Nats-Service-Error"); err != "" {
		t.Fatalf("Request failed: %v", err)
	}

	resp := api.StatusResponse{}
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		t.Fatalf("could not unmarshal StatusResponse: %v: %v", string(msg.Data), err)
	}

	return &resp
}

// Close closes the connection to the management service
func (c *Client) Close(t testing.TB) {
	t.Helper()

	c.nc.Close()
}

// Destroy tears down this instance — shuts down its servers and removes its
// storage dir. Other instances on the same management service are unaffected.
func (i *Instance) Destroy(t testing.TB) *api.DestroyResponse {
	t.Helper()

	jreq, err := json.Marshal(api.DestroyRequest{InstanceID: i.ID})
	if err != nil {
		t.Fatalf("could not marshal DestroyRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.destroy", jreq, 30*time.Second)
	if err != nil {
		t.Fatalf("could not send DestroyRequest: %v", err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request failed: %v", e)
	}

	resp := api.DestroyResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal DestroyResponse: %v: %v", string(msg.Data), err)
	}
	return &resp
}

// StopServer stops a single server within this instance.
func (i *Instance) StopServer(t testing.TB, server *api.ManagedServer) *api.StopServerResponse {
	t.Helper()

	if server == nil || server.Name == "" {
		t.Fatal("server is required")
	}

	req, err := json.Marshal(api.StopServerRequest{Name: server.Name})
	if err != nil {
		t.Fatalf("could not marshal StopServerRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.stop.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send StopServerRequest: %v", err)
	}

	if err := msg.Header.Get("Nats-Service-Error"); err != "" {
		t.Fatalf("Request failed: %v", err)
	}

	resp := api.StopServerResponse{}
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		t.Fatalf("could not unmarshal StopServerResponse: %v: %v", string(msg.Data), err)
	}

	return &resp
}

// StartServer starts a single server within this instance that was previously
// stopped.
func (i *Instance) StartServer(t testing.TB, server *api.ManagedServer) *api.StartServerResponse {
	t.Helper()

	if server == nil || server.Name == "" {
		t.Fatal("server is required")
	}

	req, err := json.Marshal(api.StartServerRequest{Name: server.Name})
	if err != nil {
		t.Fatalf("could not marshal StartServerRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.start.server", req, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send StartServerRequest: %v", err)
	}

	if err := msg.Header.Get("Nats-Service-Error"); err != "" {
		t.Fatalf("Request failed: %v", err)
	}

	resp := api.StartServerResponse{}
	err = json.Unmarshal(msg.Data, &resp)
	if err != nil {
		t.Fatalf("could not unmarshal StartServerResponse: %v: %v", string(msg.Data), err)
	}

	return &resp
}

// UpdateServer re-renders the server's config from the supplied snippets /
// template (full-replace — the payload is the new config) and writes it to
// disk. It does not reload the config: call ReloadServer or restart the
// server to apply changes. Works whether the server is running or stopped.
//
// The set of port-bearing snippets present at create time (websocket, mqtt,
// leafnode) is fixed for the server's lifetime; an update that adds or drops
// one is rejected.
func (i *Instance) UpdateServer(t testing.TB, server *api.ManagedServer, opts ...UpdateOption) *api.UpdateServerResponse {
	t.Helper()

	if server == nil || server.Name == "" {
		t.Fatal("server is required")
	}

	uo := resolveUpdateOptions(opts)

	req, err := json.Marshal(api.UpdateServerRequest{
		Name:     server.Name,
		Snippets: uo.snippets,
		Template: uo.template,
	})
	if err != nil {
		t.Fatalf("could not marshal UpdateServerRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.update.server", req, 30*time.Second)
	if err != nil {
		t.Fatalf("could not send UpdateServerRequest: %v", err)
	}

	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request failed: %v", e)
	}

	resp := api.UpdateServerResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal UpdateServerResponse: %v: %v", string(msg.Data), err)
	}

	return &resp
}

// ReloadServer signals the running server to re-read its on-disk config
// (nats-server Reload()). Pair it with a prior UpdateServer to apply a staged
// change without restarting.
func (i *Instance) ReloadServer(t testing.TB, server *api.ManagedServer) *api.ReloadServerResponse {
	t.Helper()

	if server == nil || server.Name == "" {
		t.Fatal("server is required")
	}

	req, err := json.Marshal(api.ReloadServerRequest{Name: server.Name})
	if err != nil {
		t.Fatalf("could not marshal ReloadServerRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.reload.server", req, 30*time.Second)
	if err != nil {
		t.Fatalf("could not send ReloadServerRequest: %v", err)
	}

	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request failed: %v", e)
	}

	resp := api.ReloadServerResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal ReloadServerResponse: %v: %v", string(msg.Data), err)
	}

	return &resp
}

// Status returns the current status of just this instance.
func (i *Instance) Status(t testing.TB) *api.InstanceStatus {
	t.Helper()

	jreq, err := json.Marshal(api.StatusRequest{InstanceID: i.ID})
	if err != nil {
		t.Fatalf("could not marshal StatusRequest: %v", err)
	}

	msg, err := i.c.nc.Request("tester.status", jreq, 10*time.Second)
	if err != nil {
		t.Fatalf("could not send StatusRequest: %v", err)
	}
	if e := msg.Header.Get("Nats-Service-Error"); e != "" {
		t.Fatalf("Request failed: %v", e)
	}

	resp := api.StatusResponse{}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("could not unmarshal StatusResponse: %v: %v", string(msg.Data), err)
	}
	if len(resp.Instances) == 0 {
		t.Fatalf("instance %q not found in status response", i.ID)
	}
	return &resp.Instances[0]
}

// RandomServer picks a random server from this instance.
func (i *Instance) RandomServer() *api.ManagedServer {
	return i.Servers[rand.Intn(len(i.Servers))]
}

// RandomClusterServer picks a random server in a cluster from the list of running ones
func RandomClusterServer(cluster string, servers []*api.ManagedServer) (*api.ManagedServer, error) {
	matched := []*api.ManagedServer{}
	for _, srv := range servers {
		if srv.Cluster == cluster {
			matched = append(matched, srv)
		}
	}

	if len(matched) == 0 {
		return nil, fmt.Errorf("cluster %q not found", cluster)
	}

	return RandomServer(matched), nil
}

// RandomServer picks a random server
func RandomServer(servers []*api.ManagedServer) *api.ManagedServer {
	return servers[rand.Intn(len(servers))]
}
