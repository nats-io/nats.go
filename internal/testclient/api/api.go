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

package api

import "time"

// TLSMode selects the verification posture for managed TLS. Absence of a
// TLSOptions value disables TLS entirely; the modes only apply when TLS is
// requested.
type TLSMode string

const (
	// TLSModeServer is one-way TLS: the server presents a cert signed by the
	// generated CA but does not require a client cert.
	TLSModeServer TLSMode = "server"
	// TLSModeMutual requires the client to present the issued client cert.
	TLSModeMutual TLSMode = "mutual"
)

// TLSOptions opts a managed instance into generated TLS on its client ports.
// Gateways and routes remain plaintext.
type TLSOptions struct {
	// Mode defaults to TLSModeMutual when empty.
	Mode TLSMode `json:"mode,omitempty"`
	// SANs are the Subject Alternative Names to embed in the server leaf.
	// Strings that parse as IPs become IP SANs; everything else is treated
	// as a DNS name. Empty means ["localhost","127.0.0.1","::1"].
	SANs []string `json:"sans,omitempty"`
	// HandshakeFirst makes the server perform the TLS handshake before sending
	// the INFO protocol. Clients must dial with the matching handshake-first
	// option; the convenience helpers wire it automatically.
	HandshakeFirst bool `json:"handshake_first,omitempty"`
}

// TLSMaterial carries the cert material a caller needs to dial a managed
// instance over TLS. ClientCertPEM/ClientKeyPEM are populated only for
// TLSModeMutual; the server's private key never leaves the service.
type TLSMaterial struct {
	CAPEM         string `json:"ca_pem"`
	ClientCertPEM string `json:"client_cert_pem,omitempty"`
	ClientKeyPEM  string `json:"client_key_pem,omitempty"`
}

type CreateServerRequest struct {
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
}

type CreateClusterRequest struct {
	Servers     int               `json:"servers"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
}

type CreateSuperClusterRequest struct {
	Servers     int               `json:"servers"`
	Clusters    int               `json:"clusters"`
	JetStream   bool              `json:"jetstream"`
	Description string            `json:"description,omitempty"`
	Snippets    map[string]string `json:"snippets,omitempty"`
	Template    string            `json:"template,omitempty"`
	TLS         *TLSOptions       `json:"tls,omitempty"`
}

type ManagedServer struct {
	Name    string         `json:"name"`
	Cluster string         `json:"cluster"`
	Port    int            `json:"port"`
	Ports   map[string]int `json:"ports,omitempty"`
	URL     string         `json:"url,omitempty"`
	Running bool           `json:"running"`
}

type CreateResponse struct {
	ID          string           `json:"id"`
	Description string           `json:"description,omitempty"`
	Kind        string           `json:"kind"`
	Servers     []*ManagedServer `json:"servers"`
	TLS         *TLSMaterial     `json:"tls,omitempty"`
}

type DestroyRequest struct {
	InstanceID string `json:"instance_id"`
}

type DestroyResponse struct {
	Destroyed bool `json:"destroyed"`
}

type InstanceSummary struct {
	ID          string    `json:"id"`
	Description string    `json:"description,omitempty"`
	Kind        string    `json:"kind"`
	Cluster     string    `json:"cluster,omitempty"`
	Servers     int       `json:"servers"`
	Created     time.Time `json:"created"`
}

type ListResponse struct {
	Instances []InstanceSummary `json:"instances"`
}

type ResetResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerRequest struct {
	Name string `json:"name"`
}

type StopServerRequest struct {
	Name string `json:"name"`
}

type StopServerResponse struct {
	Shutdown bool `json:"shutdown"`
}

type StartServerResponse struct {
	Started bool `json:"started"`
}

type StatusRequest struct {
	InstanceID string `json:"instance_id,omitempty"`
}

type InstanceStatus struct {
	ID          string          `json:"id"`
	Description string          `json:"description,omitempty"`
	Kind        string          `json:"kind"`
	Servers     []ManagedServer `json:"servers"`
}

type StatusResponse struct {
	Instances []InstanceStatus `json:"instances"`
}

type UpdateServerRequest struct {
	Name     string            `json:"name"`
	Snippets map[string]string `json:"snippets,omitempty"`
	Template string            `json:"template,omitempty"`
}

type UpdateServerResponse struct {
	Updated bool `json:"updated"`
}

type ReloadServerRequest struct {
	Name string `json:"name"`
}

type ReloadServerResponse struct {
	Reloaded bool `json:"reloaded"`
}
