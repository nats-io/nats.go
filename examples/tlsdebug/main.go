// Copyright 2025 The NATS Authors
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

package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Usage:
//
//	tlsdebug -s tls://connect.ngs.global \
//	  -creds /tmp/root.creds \
//	  -tlscert my.crt \
//	  -tlskey my.key \
//	  -tlscacert ca-cert.pem \
//	  -timeout 5s
func usage() {
	log.Printf("Usage: tlsdebug [-s server] [-creds file] [-tlscert file] [-tlskey file] [-tlscacert file] [-tlsfirst] [-timeout duration]\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var tlsClientCert = flag.String("tlscert", "", "TLS client certificate file")
	var tlsClientKey = flag.String("tlskey", "", "TLS client private key file")
	var tlsCACert = flag.String("tlscacert", "", "CA certificate to verify peer against")
	var tlsFirst = flag.Bool("tlsfirst", false, "Perform TLS handshake before receiving INFO protocol")
	var timeout = flag.Duration("timeout", 5*time.Second, "Connection timeout")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	// Connect Options.
	opts := []nats.Option{
		nats.Name("NATS TLS Debug"),
		nats.Timeout(*timeout),
		nats.SetCustomDialer(&debugDialer{}),
	}

	// Build a TLS config with debug callbacks.
	tc := debugTLSConfig()

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -creds or -nkey, not both")
	}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use Nkey authentication.
	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	// Use TLS client certificate for mutual TLS.
	if *tlsClientCert != "" && *tlsClientKey != "" {
		cert, err := tls.LoadX509KeyPair(*tlsClientCert, *tlsClientKey)
		if err != nil {
			log.Fatalf("Failed to load client certificate: %v", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}

	// Use specific CA certificate.
	if *tlsCACert != "" {
		caCert, err := os.ReadFile(*tlsCACert)
		if err != nil {
			log.Fatalf("Failed to read CA certificate: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			log.Fatal("Failed to parse CA certificate")
		}
		tc.RootCAs = pool
	}

	opts = append(opts, nats.Secure(tc))

	if *tlsFirst {
		opts = append(opts, nats.TLSHandshakeFirst())
	}

	fmt.Println("======================================")
	fmt.Println(" NATS TLS Connection Debug")
	fmt.Println("======================================")
	fmt.Printf("Server:       %s\n", *urls)
	fmt.Printf("Timeout:      %s\n", *timeout)
	if *userCreds != "" {
		fmt.Printf("Credentials:  %s\n", *userCreds)
	}
	if *nkeyFile != "" {
		fmt.Printf("NKey:         %s\n", *nkeyFile)
	}
	if *tlsClientCert != "" {
		fmt.Printf("TLS Cert:     %s\n", *tlsClientCert)
	}
	if *tlsClientKey != "" {
		fmt.Printf("TLS Key:      %s\n", *tlsClientKey)
	}
	if *tlsCACert != "" {
		fmt.Printf("TLS CA:       %s\n", *tlsCACert)
	}
	if *tlsFirst {
		fmt.Printf("TLS First:    true\n")
	}
	fmt.Println()

	// Connect to NATS
	fmt.Println("Connecting...")
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatalf("Connection failed: %v\n", err)
	}
	defer nc.Close()

	fmt.Printf("Connected to: %s\n", nc.ConnectedUrlRedacted())
	fmt.Printf("Server Name:  %s\n", nc.ConnectedServerName())
	fmt.Printf("Server ID:    %s\n", nc.ConnectedServerId())
	fmt.Printf("Version:      %s\n", nc.ConnectedServerVersion())
	fmt.Println()

	// TLS Connection State
	tlsState, err := nc.TLSConnectionState()
	if err != nil {
		fmt.Printf("TLS State: not available (%v)\n", err)
	} else {
		fmt.Println("--- TLS Connection Info ---")
		fmt.Printf("TLS Version:        %s\n", tlsVersionName(tlsState.Version))
		fmt.Printf("Cipher Suite:       %s\n", tlsCipherSuiteName(tlsState.CipherSuite))
		fmt.Printf("Server Name (SNI):  %s\n", tlsState.ServerName)
		fmt.Printf("Negotiated Protocol: %s\n", tlsState.NegotiatedProtocol)
		fmt.Println()

		if len(tlsState.PeerCertificates) > 0 {
			fmt.Println("--- Server Certificate Chain ---")
			for i, cert := range tlsState.PeerCertificates {
				fmt.Printf("\nCertificate [%d]:\n", i)
				fmt.Printf("  Subject:      %s\n", cert.Subject)
				fmt.Printf("  Issuer:       %s\n", cert.Issuer)
				fmt.Printf("  Serial:       %s\n", cert.SerialNumber)
				fmt.Printf("  Not Before:   %s\n", cert.NotBefore.Format(time.RFC3339))
				fmt.Printf("  Not After:    %s\n", cert.NotAfter.Format(time.RFC3339))
				if time.Now().After(cert.NotAfter) {
					fmt.Printf("  ** EXPIRED **\n")
				} else {
					remaining := time.Until(cert.NotAfter)
					fmt.Printf("  Expires In:   %s\n", formatDuration(remaining))
				}
				if len(cert.DNSNames) > 0 {
					fmt.Printf("  DNS Names:    %s\n", strings.Join(cert.DNSNames, ", "))
				}
				if len(cert.IPAddresses) > 0 {
					ips := make([]string, len(cert.IPAddresses))
					for j, ip := range cert.IPAddresses {
						ips[j] = ip.String()
					}
					fmt.Printf("  IP Addresses: %s\n", strings.Join(ips, ", "))
				}
				if len(cert.URIs) > 0 {
					uris := make([]string, len(cert.URIs))
					for j, u := range cert.URIs {
						uris[j] = u.String()
					}
					fmt.Printf("  URIs:         %s\n", strings.Join(uris, ", "))
				}
				fmt.Printf("  Signature:    %s\n", cert.SignatureAlgorithm)
				fmt.Printf("  Public Key:   %s\n", publicKeyInfo(cert))
				if cert.IsCA {
					fmt.Printf("  Is CA:        true\n")
				}
				printKeyUsage(cert)
			}
		}

		if len(tlsState.VerifiedChains) > 0 {
			fmt.Printf("\n--- Verified Chains: %d ---\n", len(tlsState.VerifiedChains))
			for i, chain := range tlsState.VerifiedChains {
				fmt.Printf("Chain [%d]: ", i)
				subjects := make([]string, len(chain))
				for j, cert := range chain {
					subjects[j] = cert.Subject.CommonName
				}
				fmt.Println(strings.Join(subjects, " -> "))
			}
		}
	}

	fmt.Println()
	fmt.Println("======================================")
	fmt.Println(" Connection successful!")
	fmt.Println("======================================")
}

// debugDialer implements nats.CustomDialer and logs TCP connection details.
type debugDialer struct{}

func (d *debugDialer) Dial(network, address string) (net.Conn, error) {
	host, port, _ := net.SplitHostPort(address)

	fmt.Printf("[debug] DNS lookup for %s...\n", host)
	dnsStart := time.Now()
	addrs, err := net.LookupHost(host)
	dnsElapsed := time.Since(dnsStart)
	if err != nil {
		fmt.Printf("[debug] DNS lookup failed: %v (%s)\n", err, dnsElapsed)
		return nil, err
	}
	fmt.Printf("[debug] DNS resolved %s -> %s (%s)\n", host, strings.Join(addrs, ", "), dnsElapsed)

	fmt.Printf("[debug] TCP connecting to %s %s...\n", network, address)
	tcpStart := time.Now()
	conn, err := net.DialTimeout(network, net.JoinHostPort(addrs[0], port), 5*time.Second)
	tcpElapsed := time.Since(tcpStart)
	if err != nil {
		fmt.Printf("[debug] TCP connect failed: %v (%s)\n", err, tcpElapsed)
		return nil, err
	}
	fmt.Printf("[debug] TCP connected to %s (%s)\n", conn.RemoteAddr(), tcpElapsed)
	return conn, nil
}

// debugTLSConfig returns a *tls.Config with callbacks that log TLS handshake details.
func debugTLSConfig() *tls.Config {
	handshakeStart := time.Now()
	return &tls.Config{
		VerifyConnection: func(state tls.ConnectionState) error {
			elapsed := time.Since(handshakeStart)
			fmt.Printf("[debug] TLS handshake complete (%s)\n", elapsed)
			fmt.Printf("[debug]   Version: %s\n", tlsVersionName(state.Version))
			fmt.Printf("[debug]   Cipher:  %s\n", tlsCipherSuiteName(state.CipherSuite))
			fmt.Printf("[debug]   SNI:     %s\n", state.ServerName)
			for i, c := range state.PeerCertificates {
				fmt.Printf("[debug]   Peer Cert[%d]: %s (issuer: %s)\n", i, c.Subject, c.Issuer)
			}
			return nil
		},
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Capture the handshake start time on first callback.
			handshakeStart = time.Now()
			fmt.Printf("[debug] TLS handshake verifying %d raw certificate(s)...\n", len(rawCerts))
			for i, rawCert := range rawCerts {
				block := &pem.Block{Type: "CERTIFICATE", Bytes: rawCert}
				cert, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					fmt.Printf("[debug]   Raw Cert[%d]: failed to parse: %v\n", i, err)
					continue
				}
				fmt.Printf("[debug]   Raw Cert[%d]: %s (issuer: %s, serial: %s)\n",
					i, cert.Subject, cert.Issuer, cert.SerialNumber)
			}
			if len(verifiedChains) > 0 {
				fmt.Printf("[debug]   Verified chains: %d\n", len(verifiedChains))
			} else {
				fmt.Printf("[debug]   (verified chains built after peer cert verification)\n")
			}
			return nil
		},
	}
}

func tlsVersionName(v uint16) string {
	switch v {
	case 0x0300:
		return "SSL 3.0"
	case 0x0301:
		return "TLS 1.0"
	case 0x0302:
		return "TLS 1.1"
	case 0x0303:
		return "TLS 1.2"
	case 0x0304:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown (0x%04x)", v)
	}
}

func tlsCipherSuiteName(id uint16) string {
	// Go 1.14+ has tls.CipherSuiteName but we construct manually for clarity.
	switch id {
	case 0x1301:
		return "TLS_AES_128_GCM_SHA256"
	case 0x1302:
		return "TLS_AES_256_GCM_SHA384"
	case 0x1303:
		return "TLS_CHACHA20_POLY1305_SHA256"
	case 0xc02f:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case 0xc030:
		return "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	case 0xcca8:
		return "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305"
	case 0xc02b:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	case 0xc02c:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
	case 0xcca9:
		return "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305"
	default:
		return fmt.Sprintf("0x%04x", id)
	}
}

func publicKeyInfo(cert *x509.Certificate) string {
	switch cert.PublicKeyAlgorithm {
	case x509.RSA:
		if cert.PublicKey != nil {
			return fmt.Sprintf("RSA %d-bit", cert.PublicKey.(interface{ Size() int }).Size()*8)
		}
		return "RSA"
	case x509.ECDSA:
		return fmt.Sprintf("ECDSA %s", cert.PublicKeyAlgorithm)
	case x509.Ed25519:
		return "Ed25519"
	default:
		return cert.PublicKeyAlgorithm.String()
	}
}

func printKeyUsage(cert *x509.Certificate) {
	var usages []string
	if cert.KeyUsage&x509.KeyUsageDigitalSignature != 0 {
		usages = append(usages, "Digital Signature")
	}
	if cert.KeyUsage&x509.KeyUsageKeyEncipherment != 0 {
		usages = append(usages, "Key Encipherment")
	}
	if cert.KeyUsage&x509.KeyUsageCertSign != 0 {
		usages = append(usages, "Certificate Sign")
	}
	if cert.KeyUsage&x509.KeyUsageCRLSign != 0 {
		usages = append(usages, "CRL Sign")
	}
	if len(usages) > 0 {
		fmt.Printf("  Key Usage:    %s\n", strings.Join(usages, ", "))
	}

	if len(cert.ExtKeyUsage) > 0 {
		var extUsages []string
		for _, u := range cert.ExtKeyUsage {
			switch u {
			case x509.ExtKeyUsageServerAuth:
				extUsages = append(extUsages, "Server Auth")
			case x509.ExtKeyUsageClientAuth:
				extUsages = append(extUsages, "Client Auth")
			default:
				extUsages = append(extUsages, fmt.Sprintf("%d", u))
			}
		}
		fmt.Printf("  Ext Key Usage: %s\n", strings.Join(extUsages, ", "))
	}
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 365 {
		years := days / 365
		remaining := days % 365
		return fmt.Sprintf("%d years, %d days", years, remaining)
	}
	if days > 0 {
		return fmt.Sprintf("%d days", days)
	}
	return d.Round(time.Second).String()
}
