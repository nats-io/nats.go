// Copyright 2012-2025 The NATS Authors
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

//go:build tinygo

package nats

import (
	"errors"
	"strconv"
	"strings"
)

func marshalConnectInfo(c connectInfo) ([]byte, error) {
	var b strings.Builder
	b.WriteByte('{')
	writeKVBool(&b, "verbose", c.Verbose)
	b.WriteByte(',')
	writeKVBool(&b, "pedantic", c.Pedantic)
	if c.UserJWT != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "jwt", c.UserJWT)
	}
	if c.Nkey != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "nkey", c.Nkey)
	}
	if c.Signature != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "sig", c.Signature)
	}
	if c.User != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "user", c.User)
	}
	if c.Pass != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "pass", c.Pass)
	}
	if c.Token != _EMPTY_ {
		b.WriteByte(',')
		writeKVString(&b, "auth_token", c.Token)
	}
	b.WriteByte(',')
	writeKVBool(&b, "tls_required", c.TLS)
	b.WriteByte(',')
	writeKVString(&b, "name", c.Name)
	b.WriteByte(',')
	writeKVString(&b, "lang", c.Lang)
	b.WriteByte(',')
	writeKVString(&b, "version", c.Version)
	b.WriteString(`,"protocol":`)
	b.WriteString(strconv.Itoa(c.Protocol))
	b.WriteByte(',')
	writeKVBool(&b, "echo", c.Echo)
	b.WriteByte(',')
	writeKVBool(&b, "headers", c.Headers)
	b.WriteByte(',')
	writeKVBool(&b, "no_responders", c.NoResponders)
	b.WriteByte('}')
	return []byte(b.String()), nil
}

func writeKVBool(b *strings.Builder, key string, val bool) {
	b.WriteByte('"')
	b.WriteString(key)
	b.WriteString(`":`)
	if val {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}
}

func writeKVString(b *strings.Builder, key, val string) {
	b.WriteByte('"')
	b.WriteString(key)
	b.WriteString(`":"`)
	for i := range len(val) {
		switch val[i] {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(val[i])
		}
	}
	b.WriteByte('"')
}

func unmarshalServerInfo(data string, info *ServerInfo) error {
	data = strings.TrimSpace(data)
	if len(data) < 2 || data[0] != '{' || data[len(data)-1] != '}' {
		return errors.New("nats: invalid server info")
	}
	data = data[1 : len(data)-1]

	for len(data) > 0 {
		data = strings.TrimSpace(data)
		if len(data) == 0 {
			break
		}
		if data[0] == ',' {
			data = data[1:]
			continue
		}
		if data[0] != '"' {
			return errors.New("nats: invalid server info")
		}
		end := strings.IndexByte(data[1:], '"')
		if end < 0 {
			return errors.New("nats: invalid server info key")
		}
		key := data[1 : end+1]
		data = strings.TrimSpace(data[end+2:])
		if len(data) == 0 || data[0] != ':' {
			return errors.New("nats: invalid server info")
		}
		data = strings.TrimSpace(data[1:])
		if len(data) == 0 {
			return errors.New("nats: invalid server info value")
		}

		var val string
		var advance int
		switch data[0] {
		case '"':
			val, advance = jsonReadString(data)
		case '[':
			val, advance = jsonReadArray(data)
		default:
			end := strings.IndexAny(data, ",}")
			if end < 0 {
				end = len(data)
			}
			val = strings.TrimSpace(data[:end])
			advance = end
		}
		data = data[advance:]

		switch key {
		case "server_id":
			info.ID = val
		case "server_name":
			info.Name = val
		case "proto":
			info.Proto, _ = strconv.Atoi(val)
		case "version":
			info.Version = val
		case "host":
			info.Host = val
		case "port":
			info.Port, _ = strconv.Atoi(val)
		case "headers":
			info.Headers = val == "true"
		case "auth_required":
			info.AuthRequired = val == "true"
		case "tls_required":
			info.TLSRequired = val == "true"
		case "tls_available":
			info.TLSAvailable = val == "true"
		case "max_payload":
			info.MaxPayload, _ = strconv.ParseInt(val, 10, 64)
		case "client_id":
			info.CID, _ = strconv.ParseUint(val, 10, 64)
		case "client_ip":
			info.ClientIP = val
		case "nonce":
			info.Nonce = val
		case "cluster":
			info.Cluster = val
		case "connect_urls":
			info.ConnectURLs = jsonParseStringArray(val)
		case "ldm":
			info.LameDuckMode = val == "true"
		case "jetstream":
			info.JetStream = val == "true"
		case "acc_is_sys":
			info.IsSystemAccount = val == "true"
		case "api_lvl":
			info.JSApiLevel, _ = strconv.Atoi(val)
		}
	}
	return nil
}

// jsonReadString reads a JSON string value starting at data[0] == '"'.
// Returns the unescaped value and bytes consumed.
func jsonReadString(data string) (string, int) {
	var b strings.Builder
	i := 1
	for i < len(data) {
		c := data[i]
		if c == '\\' && i+1 < len(data) {
			i++
			switch data[i] {
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			default:
				b.WriteByte(data[i])
			}
		} else if c == '"' {
			return b.String(), i + 1
		} else {
			b.WriteByte(c)
		}
		i++
	}
	return b.String(), i
}

// jsonReadArray reads a JSON array starting at data[0] == '['.
// Returns the raw inner content and bytes consumed.
func jsonReadArray(data string) (string, int) {
	depth := 0
	for i, c := range data {
		switch c {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return data[1:i], i + 1
			}
		}
	}
	return _EMPTY_, len(data)
}

// jsonParseStringArray parses the inner content of a JSON string array.
func jsonParseStringArray(raw string) []string {
	var result []string
	for len(raw) > 0 {
		raw = strings.TrimSpace(raw)
		if len(raw) == 0 {
			break
		}
		if raw[0] == ',' {
			raw = raw[1:]
			continue
		}
		if raw[0] != '"' {
			break
		}
		val, advance := jsonReadString(raw)
		result = append(result, val)
		raw = raw[advance:]
	}
	return result
}
