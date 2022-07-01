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
	"time"
)

type (
	apiResponse struct {
		Type  string    `json:"type"`
		Error *apiError `json:"error,omitempty"`
	}

	// apiError is included in all API responses if there was an error.
	apiError struct {
		Code        int    `json:"code"`
		ErrorCode   int    `json:"err_code"`
		Description string `json:"description,omitempty"`
	}
)

// Request API subjects for JetStream.
const (
	// DefaultAPIPrefix is the default prefix for the JetStream API.
	DefaultAPIPrefix = "$JS.API."

	// jsDomainT is used to create JetStream API prefix by specifying only Domain
	jsDomainT = "$JS.%s.API."

	// apiAccountInfo is for obtaining general information about JetStream.
	apiAccountInfo = "INFO"

	// apiConsumerCreateT is used to create consumers.
	apiConsumerCreateT = "CONSUMER.CREATE.%s"

	// apiDurableCreateT is used to create durable consumers.
	apiDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"

	// apiConsumerInfoT is used to create consumers.
	apiConsumerInfoT = "CONSUMER.INFO.%s.%s"

	// apiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
	apiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"

	// apiConsumerDeleteT is used to delete consumers.
	apiConsumerDeleteT = "CONSUMER.DELETE.%s.%s"

	// apiConsumerListT is used to return all detailed consumer information
	apiConsumerListT = "CONSUMER.LIST.%s"

	// apiConsumerNamesT is used to return a list with all consumer names for the stream.
	apiConsumerNamesT = "CONSUMER.NAMES.%s"

	// apiStreams can lookup a stream by subject.
	apiStreams = "STREAM.NAMES"

	// apiStreamCreateT is the endpoint to create new streams.
	apiStreamCreateT = "STREAM.CREATE.%s"

	// apiStreamInfoT is the endpoint to get information on a stream.
	apiStreamInfoT = "STREAM.INFO.%s"

	// apiStreamUpdateT is the endpoint to update existing streams.
	apiStreamUpdateT = "STREAM.UPDATE.%s"

	// apiStreamDeleteT is the endpoint to delete streams.
	apiStreamDeleteT = "STREAM.DELETE.%s"

	// apiStreamPurgeT is the endpoint to purge streams.
	apiStreamPurgeT = "STREAM.PURGE.%s"

	// apiStreamListT is the endpoint that will return all detailed stream information
	apiStreamListT = "STREAM.LIST"

	// apiMsgGetT is the endpoint to get a message.
	apiMsgGetT = "STREAM.MSG.GET.%s"

	// apiMsgDeleteT is the endpoint to remove a message.
	apiMsgDeleteT = "STREAM.MSG.DELETE.%s"

	// Default time wait between retries on Publish iff err is NoResponders.
	DefaultPubRetryWait = 250 * time.Millisecond

	// Default number of retries
	DefaultPubRetryAttempts = 2

	// defaultAsyncPubAckInflight is the number of async pub acks inflight.
	defaultAsyncPubAckInflight = 4000
)

func (js *jetStream) apiRequestJSON(ctx context.Context, subject string, resp interface{}, data ...[]byte) (*jetStreamMsg, error) {
	jsMsg, err := js.apiRequest(ctx, subject, data...)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(jsMsg.Data(), resp); err != nil {
		return nil, err
	}
	return jsMsg, err
}

// a RequestWithContext with tracing via TraceCB
func (js *jetStream) apiRequest(ctx context.Context, subj string, data ...[]byte) (*jetStreamMsg, error) {
	var req []byte
	if len(data) > 0 {
		req = data[0]
	}
	if js.clientTrace != nil {
		ctrace := js.clientTrace
		if ctrace.RequestSent != nil {
			ctrace.RequestSent(subj, req)
		}
	}
	resp, err := js.conn.RequestWithContext(ctx, subj, req)
	if err != nil {
		return nil, err
	}
	if js.clientTrace != nil {
		ctrace := js.clientTrace
		if ctrace.ResponseReceived != nil {
			ctrace.ResponseReceived(subj, resp.Data, resp.Header)
		}
	}

	return js.toJSMsg(resp), nil
}

func apiSubj(prefix, subject string) string {
	if prefix == "" {
		return subject
	}
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteString(subject)
	return b.String()
}

// Error prints the API error description
func (e *apiError) Error() string {
	return fmt.Sprintf("API error: %s", e.Description)
}
