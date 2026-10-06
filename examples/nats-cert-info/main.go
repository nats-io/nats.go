// Copyright 2012-2026 The NATS Authors
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
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: nats-cert-info [-s server] [-nkey file] [-tlscacert file] [-skip-tls-verify]\n")
	fmt.Fprintf(os.Stderr, "  -s             NATS server URL(s) separated by comma (default: nats://127.0.0.1:4222)\n")
	fmt.Fprintf(os.Stderr, "  -nkey          NKey seed file for authentication\n")
	fmt.Fprintf(os.Stderr, "  -tlscacert     Root CA certificate file to verify server certificate\n")
	fmt.Fprintf(os.Stderr, "  -skip-tls-verify Skip server certificate verification (NOT for production)\n")
	flag.PrintDefaults()
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var nkeyFile = flag.String("nkey", "", "NKey seed file for authentication")
	var tlsCACert = flag.String("tlscacert", "", "Root CA certificate file to verify server certificate")
	var skipTLSVerify = flag.Bool("skip-tls-verify", false, "Skip server certificate verification")
	var showHelp = flag.Bool("h", false, "Show this help message")
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		usage()
		os.Exit(0)
	}

	opts := []nats.Option{nats.MaxReconnects(1)}

	// Enable TLS
	if *skipTLSVerify {
		opts = append(opts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	} else if *tlsCACert != "" {
		opts = append(opts, nats.RootCAs(*tlsCACert))
		opts = append(opts, nats.Secure(nil))
	} else {
		opts = append(opts, nats.Secure(nil))
	}

	// NKey authentication
	if *nkeyFile != "" {
		nkeyOpt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatalf("Failed to load NKey seed: %v", err)
		}
		opts = append(opts, nkeyOpt)
	}

	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	tlsState, err := nc.TLSConnectionState()
	if err != nil {
		log.Fatalf("Failed to get TLS connection state: %v", err)
	}

	if len(tlsState.PeerCertificates) == 0 {
		log.Fatal("No server certificates presented")
	}

	// Use the first (leaf) certificate presented by the server
	cert := tlsState.PeerCertificates[0]

	fmt.Println("Server Certificate Info:")
	fmt.Println("========================")

	// Subject
	fmt.Printf("Subject: %s\n", formatSubject(cert.Subject))

	// Validity
	fmt.Printf("Valid From: %s\n", cert.NotBefore.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("Valid Until: %s\n", cert.NotAfter.Format("2006-01-02 15:04:05 MST"))

	// Additional useful info
	duration := time.Until(cert.NotAfter)
	if duration < 0 {
		fmt.Printf("Status: EXPIRED %s ago\n", (-duration).String())
	} else if duration < 720*time.Hour {
		fmt.Printf("Status: Expires in %s\n", duration.String())
	} else {
		fmt.Printf("Status: Valid\n")
	}

	// Issuer
	fmt.Printf("Issuer: %s\n", formatSubject(cert.Issuer))

	// DNS SANs
	if len(cert.DNSNames) > 0 {
		fmt.Printf("DNS Names: %s\n", strings.Join(cert.DNSNames, ", "))
	}

	// IP SANs
	if len(cert.IPAddresses) > 0 {
		var ips []string
		for _, ip := range cert.IPAddresses {
			ips = append(ips, ip.String())
		}
		fmt.Printf("IP Addresses: %s\n", strings.Join(ips, ", "))
	}

	// Email SANs
	if len(cert.EmailAddresses) > 0 {
		fmt.Printf("Email Addresses: %s\n", strings.Join(cert.EmailAddresses, ", "))
	}

	// URI SANs
	if len(cert.URIs) > 0 {
		var uris []string
		for _, u := range cert.URIs {
			uris = append(uris, u.String())
		}
		fmt.Printf("URIs: %s\n", strings.Join(uris, ", "))
	}

	// Serial Number
	fmt.Printf("Serial Number: %s\n", cert.SerialNumber.String())
}

func formatSubject(subject pkix.Name) string {
	var parts []string
	if subject.CommonName != "" {
		parts = append(parts, fmt.Sprintf("CN=%s", subject.CommonName))
	}
	if subject.Country != nil {
		parts = append(parts, fmt.Sprintf("C=%s", subject.Country[0]))
	}
	if subject.Organization != nil {
		parts = append(parts, fmt.Sprintf("O=%s", subject.Organization[0]))
	}
	if len(subject.OrganizationalUnit) > 0 {
		parts = append(parts, fmt.Sprintf("OU=%s", subject.OrganizationalUnit[0]))
	}
	if subject.Locality != nil {
		parts = append(parts, fmt.Sprintf("L=%s", subject.Locality[0]))
	}
	if subject.Province != nil {
		parts = append(parts, fmt.Sprintf("ST=%s", subject.Province[0]))
	}
	return strings.Join(parts, ", ")
}
