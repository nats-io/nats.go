package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type ConsumerDetail struct {
	ServerID             string
	StreamName           string
	ConsumerName         string
	Account              string
	RaftGroup            string
	State                nats.StreamState
	Cluster              *nats.ClusterInfo
	StreamCluster        *nats.ClusterInfo
	DeliveredStreamSeq   uint64
	DeliveredConsumerSeq uint64
	AckFloorStreamSeq    uint64
	AckFloorConsumerSeq  uint64
	NumAckPending        int
	NumRedelivered       int
	NumWaiting           int
	NumPending           uint64
	HealthStatus         string
}

func main() {
	log.SetFlags(0)
	var (
		urls, sname, cname string
		creds              string
		timeout            int
		health             bool
		unsyncedFilter     bool
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	flag.StringVar(&creds, "creds", "", "The NATS credentials")
	flag.StringVar(&sname, "stream", "", "Select a single stream")
	flag.StringVar(&cname, "consumer", "", "Select a single consumer")
	flag.BoolVar(&health, "health", false, "Check health from consumers")
	flag.IntVar(&timeout, "timeout", 30, "Connect timeout")
	flag.BoolVar(&unsyncedFilter, "unsynced", false, "Filter by streams that are out of sync")
	flag.Parse()

	start := time.Now()

	opts := []nats.Option{
		nats.Timeout(time.Duration(timeout) * time.Second),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}

	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected in %.3fs", time.Since(start).Seconds())
	sys := NewSysClient(nc)

	start = time.Now()
	servers, err := sys.JszPing(JszEventOptions{
		JszOptions: JszOptions{
			Streams:    true,
			Consumer:   true,
			RaftGroups: true,
		},
	})
	log.Printf("Response took %.3fs", time.Since(start).Seconds())
	if err != nil {
		log.Fatal(err)
	}
	header := fmt.Sprintf("Servers: %d", len(servers))
	fmt.Println(header)

	consumers := make(map[string]map[string]*ConsumerDetail)
	// Collect all info from servers.
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				for _, consumer := range stream.Consumer {
					var ok bool
					var m map[string]*ConsumerDetail
					if m, ok = consumers[consumer.Name]; !ok {
						m = make(map[string]*ConsumerDetail)
						consumers[consumer.Name] = m
					}
					var raftGroup string
					for _, cr := range stream.ConsumerRaftGroups {
						if cr.Name == consumer.Name {
							raftGroup = cr.RaftGroup
							break
						}
					}

					m[server.Name] = &ConsumerDetail{
						ServerID:             server.ID,
						StreamName:           consumer.Stream,
						ConsumerName:         consumer.Name,
						Account:              acc.Name,
						RaftGroup:            raftGroup,
						State:                stream.State,
						DeliveredStreamSeq:   consumer.Delivered.Stream,
						DeliveredConsumerSeq: consumer.Delivered.Consumer,
						AckFloorStreamSeq:    consumer.AckFloor.Stream,
						AckFloorConsumerSeq:  consumer.AckFloor.Consumer,
						Cluster:              consumer.Cluster,
						StreamCluster:        stream.Cluster,
						NumAckPending:        consumer.NumAckPending,
						NumRedelivered:       consumer.NumRedelivered,
						NumWaiting:           consumer.NumWaiting,
						NumPending:           consumer.NumPending,
					}
				}
			}
		}
	}
	keys := make([]string, 0)
	for k := range consumers {
		for kk := range consumers[k] {
			key := fmt.Sprintf("%s/%s", k, kk)
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	line := strings.Repeat("-", 170)
	fmt.Printf("Consumers: %d\n", len(keys))
	fmt.Println()

	fields := []any{"CONSUMER", "STREAM", "RAFT", "ACCOUNT", "NODE", "DELIVERED", "ACK_FLOOR", "COUNTERS", "STATUS"}
	fmt.Printf("%-10s %-15s %-15s %-10s %-15s | %-20s | %-20s | %-25s | %-30s\n", fields...)

	var prev string
	for i, k := range keys {
		var unsynced bool
		v := strings.Split(k, "/")
		consumerName, serverName := v[0], v[1]
		if cname != "" && consumerName != cname {
			continue
		}
		consumer := consumers[consumerName]
		replica := consumer[serverName]
		var status string
		statuses := make(map[string]bool)

		// Make comparisons against other peers.
		for _, peer := range consumer {
			if peer.DeliveredStreamSeq != replica.DeliveredStreamSeq &&
				peer.DeliveredConsumerSeq != replica.DeliveredConsumerSeq {
				statuses["UNSYNCED:DELIVERED"] = true
				unsynced = true
			}
			if peer.AckFloorStreamSeq != replica.AckFloorStreamSeq &&
				peer.AckFloorConsumerSeq != replica.AckFloorConsumerSeq {
				statuses["UNSYNCED:ACK_FLOOR"] = true
				unsynced = true
			}
			if peer.Cluster == nil {
				statuses["NO_CLUSTER"] = true
				unsynced = true
			} else {
				if replica.Cluster == nil {
					statuses["NO_CLUSTER_R"] = true
					unsynced = true
				}
				if peer.Cluster.Leader != replica.Cluster.Leader {
					statuses["MULTILEADER"] = true
					unsynced = true
				}
			}
		}
		if replica.AckFloorStreamSeq == 0 || replica.AckFloorConsumerSeq == 0 ||
			replica.DeliveredConsumerSeq == 0 || replica.DeliveredStreamSeq == 0 {
			statuses["LOST STATE"] = true
			unsynced = true
		}
		if len(statuses) > 0 {
			for k, _ := range statuses {
				status = fmt.Sprintf("%s%s,", status, k)
			}
		} else {
			status = "IN SYNC"
		}

		if unsyncedFilter && !unsynced {
			continue
		}
		if i > 0 && prev != consumerName {
			fmt.Println(line)
		}

		sf := make([]any, 0)
		sf = append(sf, replica.ConsumerName)
		sf = append(sf, replica.StreamName)
		sf = append(sf, replica.RaftGroup)
		sf = append(sf, replica.Account)

		// Mark it in case it is a leader.
		var suffix string
		if serverName == replica.Cluster.Leader {
			suffix = "*"
		} else if replica.Cluster.Leader == "" {
			status = "LEADERLESS"
			unsynced = true
		}
		s := fmt.Sprintf("%s%s", serverName, suffix)
		sf = append(sf, s)
		sf = append(sf, fmt.Sprintf("%d | %d", replica.DeliveredStreamSeq, replica.DeliveredConsumerSeq))
		sf = append(sf, fmt.Sprintf("%d | %d", replica.AckFloorStreamSeq, replica.AckFloorConsumerSeq))
		sf = append(sf, fmt.Sprintf("(ap:%d, nr:%d, nw:%d, np:%d)",
			replica.NumAckPending,
			replica.NumRedelivered,
			replica.NumWaiting,
			replica.NumPending,
		))

		var replicasInfo string
		replicasInfo = fmt.Sprintf("leader: %q", replica.Cluster.Leader)

		// Include Healthz if option added.
		var healthStatus string
		if health {
			hstatus, err := sys.Healthz(replica.ServerID, HealthzOptions{
				Account:  replica.Account,
				Stream:   replica.StreamName,
				Consumer: replica.ConsumerName,
			})
			if err != nil {
				healthStatus = err.Error()
			} else {
				healthStatus = fmt.Sprintf(":%s:%s", hstatus.Healthz.Status, hstatus.Healthz.Error)
			}
			replicasInfo = fmt.Sprintf("%s health:%q", replicasInfo, healthStatus)
		}
		sf = append(sf, replicasInfo)

		sf = append(sf, status)
		fmt.Printf("%-10s %-15s %-15s %-10s %-15s | %-20s | %-20s | %-25s | %-12s | %s \n", sf...)

		prev = consumerName
	}
}

const (
	srvVarzSubj    = "$SYS.REQ.SERVER.%s.VARZ"
	srvConnzSubj   = "$SYS.REQ.SERVER.%s.CONNZ"
	srvSubszSubj   = "$SYS.REQ.SERVER.%s.SUBSZ"
	srvHealthzSubj = "$SYS.REQ.SERVER.%s.HEALTHZ"
	srvJszSubj     = "$SYS.REQ.SERVER.%s.JSZ"
)

var (
	ErrValidation      = errors.New("validation error")
	ErrInvalidServerID = errors.New("server with given ID does not exist")
)

// System can be used to request monitoring data from the server
type System struct {
	nc *nats.Conn
}

// ServerInfo identifies remote servers.
type ServerInfo struct {
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	ID        string    `json:"id"`
	Cluster   string    `json:"cluster,omitempty"`
	Domain    string    `json:"domain,omitempty"`
	Version   string    `json:"ver"`
	Tags      []string  `json:"tags,omitempty"`
	Seq       uint64    `json:"seq"`
	JetStream bool      `json:"jetstream"`
	Time      time.Time `json:"time"`
}

func NewSysClient(nc *nats.Conn) System {
	return System{
		nc: nc,
	}
}

type requestManyOpts struct {
	maxWait     time.Duration
	maxInterval time.Duration
	count       int
}

type RequestManyOpt func(*requestManyOpts) error

func WithRequestManyMaxWait(maxWait time.Duration) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		if maxWait <= 0 {
			return fmt.Errorf("%w: max wait has to be greater than 0", ErrValidation)
		}
		opts.maxWait = maxWait
		return nil
	}
}

func WithRequestManyMaxInterval(interval time.Duration) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		if interval <= 0 {
			return fmt.Errorf("%w: max interval has to be greater than 0", ErrValidation)
		}
		opts.maxInterval = interval
		return nil
	}
}

func WithRequestManyCount(count int) RequestManyOpt {
	return func(opts *requestManyOpts) error {
		if count <= 0 {
			return fmt.Errorf("%w: expected request count has to be greater than 0", ErrValidation)
		}
		opts.count = count
		return nil
	}
}

func (s *System) RequestMany(subject string, data []byte, opts ...RequestManyOpt) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: expected subject 0", ErrValidation)
	}

	conn := s.nc
	reqOpts := &requestManyOpts{
		maxWait:     DefaultRequestTimeout,
		maxInterval: 30 * time.Second,
		count:       5,
	}

	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	inbox := nats.NewInbox()
	res := make([]*nats.Msg, 0)
	msgsChan := make(chan *nats.Msg, 100)

	intervalTimer := time.NewTimer(reqOpts.maxInterval)
	sub, err := conn.Subscribe(inbox, func(msg *nats.Msg) {
		intervalTimer.Reset(reqOpts.maxInterval)
		msgsChan <- msg
	})
	defer sub.Unsubscribe()

	if err := conn.PublishRequest(subject, inbox, data); err != nil {
		return nil, err
	}

	for {
		select {
		case msg := <-msgsChan:
			if msg.Header.Get("Status") == "503" {
				return nil, fmt.Errorf("server request on subject %q failed: %w", subject, err)
			}
			res = append(res, msg)
			if reqOpts.count != -1 && len(res) == reqOpts.count {
				return res, nil
			}
		case <-intervalTimer.C:
			return res, nil
		case <-time.After(reqOpts.maxWait):
			return res, nil
		}
	}
}

func jsonString(s string) string {
	return "\"" + s + "\""
}

type (
	HealthzResp struct {
		Server  ServerInfo `json:"server"`
		Healthz Healthz    `json:"data"`
	}

	Healthz struct {
		Status HealthStatus `json:"status"`
		Error  string       `json:"error,omitempty"`
	}

	HealthStatus int

	// HealthzOptions are options passed to Healthz
	HealthzOptions struct {
		JSEnabledOnly bool   `json:"js-enabled-only,omitempty"`
		JSServerOnly  bool   `json:"js-server-only,omitempty"`
		Account       string `json:"account,omitempty"`
		Stream        string `json:"stream,omitempty"`
		Consumer      string `json:"consumer,omitempty"`
	}
)

const (
	StatusOK HealthStatus = iota
	StatusUnavailable
	StatusError
)

func (hs *HealthStatus) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("ok"):
		*hs = StatusOK
	case jsonString("na"), jsonString("unavailable"):
		*hs = StatusUnavailable
	case jsonString("error"):
		*hs = StatusError
	default:
		return fmt.Errorf("cannot unmarshal %q", data)
	}

	return nil
}

func (hs HealthStatus) MarshalJSON() ([]byte, error) {
	switch hs {
	case StatusOK:
		return json.Marshal("ok")
	case StatusUnavailable:
		return json.Marshal("na")
	case StatusError:
		return json.Marshal("error")
	default:
		return nil, fmt.Errorf("unknown health status: %v", hs)
	}
}

func (hs HealthStatus) String() string {
	switch hs {
	case StatusOK:
		return "ok"
	case StatusUnavailable:
		return "na"
	case StatusError:
		return "error"
	default:
		return "unknown health status"
	}
}

// Healthz checks server health status.
func (s *System) Healthz(id string, opts HealthzOptions) (*HealthzResp, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: server id cannot be empty", ErrValidation)
	}
	conn := s.nc
	subj := fmt.Sprintf(srvHealthzSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Request(subj, payload, DefaultRequestTimeout)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidServerID, id)
		}
		return nil, err
	}
	var healthzResp HealthzResp
	if err := json.Unmarshal(resp.Data, &healthzResp); err != nil {
		return nil, err
	}

	return &healthzResp, nil
}

func (s *System) HealthzPing(opts HealthzOptions) ([]HealthzResp, error) {
	subj := fmt.Sprintf(srvHealthzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.RequestMany(subj, payload)
	if err != nil {
		return nil, err
	}
	srvHealthz := make([]HealthzResp, 0, len(resp))
	for _, msg := range resp {
		var healthzResp HealthzResp
		if err := json.Unmarshal(msg.Data, &healthzResp); err != nil {
			return nil, err
		}
		srvHealthz = append(srvHealthz, healthzResp)
	}
	return srvHealthz, nil
}

type (
	JSZResp struct {
		Server ServerInfo `json:"server"`
		JSInfo JSInfo     `json:"data"`
	}

	JSInfo struct {
		ID       string          `json:"server_id"`
		Now      time.Time       `json:"now"`
		Disabled bool            `json:"disabled,omitempty"`
		Config   JetStreamConfig `json:"config,omitempty"`
		JetStreamStats
		Streams   int              `json:"streams"`
		Consumers int              `json:"consumers"`
		Messages  uint64           `json:"messages"`
		Bytes     uint64           `json:"bytes"`
		Meta      *MetaClusterInfo `json:"meta_cluster,omitempty"`

		// aggregate raft info
		AccountDetails []*AccountDetail `json:"account_details,omitempty"`
	}

	AccountDetail struct {
		Name string `json:"name"`
		Id   string `json:"id"`
		JetStreamStats
		Streams []StreamDetail `json:"stream_detail,omitempty"`
	}

	StreamDetail struct {
		Name               string                   `json:"name"`
		Created            time.Time                `json:"created"`
		Cluster            *nats.ClusterInfo        `json:"cluster,omitempty"`
		Config             *nats.StreamConfig       `json:"config,omitempty"`
		State              nats.StreamState         `json:"state,omitempty"`
		Consumer           []*nats.ConsumerInfo     `json:"consumer_detail,omitempty"`
		Mirror             *nats.StreamSourceInfo   `json:"mirror,omitempty"`
		Sources            []*nats.StreamSourceInfo `json:"sources,omitempty"`
		RaftGroup          string                   `json:"stream_raft_group,omitempty"`
		ConsumerRaftGroups []*RaftGroupDetail       `json:"consumer_raft_groups,omitempty"`
	}

	RaftGroupDetail struct {
		Name      string `json:"name"`
		RaftGroup string `json:"raft_group,omitempty"`
	}

	JszEventOptions struct {
		JszOptions
		EventFilterOptions
	}

	JszOptions struct {
		Account    string `json:"account,omitempty"`
		Accounts   bool   `json:"accounts,omitempty"`
		Streams    bool   `json:"streams,omitempty"`
		Consumer   bool   `json:"consumer,omitempty"`
		Config     bool   `json:"config,omitempty"`
		LeaderOnly bool   `json:"leader_only,omitempty"`
		Offset     int    `json:"offset,omitempty"`
		Limit      int    `json:"limit,omitempty"`
		RaftGroups bool   `json:"raft,omitempty"`
	}
)

// Jsz returns server jetstream details
func (s *System) Jsz(id string, opts JszEventOptions) (*JSZResp, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: server id cannot be empty", ErrValidation)
	}
	conn := s.nc
	subj := fmt.Sprintf(srvJszSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Request(subj, payload, DefaultRequestTimeout)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidServerID, id)
		}
		return nil, err
	}

	var jszResp JSZResp
	if err := json.Unmarshal(resp.Data, &jszResp); err != nil {
		return nil, err
	}

	return &jszResp, nil
}

func (s *System) JszPing(opts JszEventOptions) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.RequestMany(subj, payload)
	if err != nil {
		return nil, err
	}
	srvJsz := make([]JSZResp, 0, len(resp))
	for _, msg := range resp {
		var jszResp JSZResp
		if err := json.Unmarshal(msg.Data, &jszResp); err != nil {
			return nil, err
		}
		srvJsz = append(srvJsz, jszResp)
	}
	return srvJsz, nil
}

const (
	DefaultRequestTimeout = 60 * time.Second
)

type (
	VarzResp struct {
		Server ServerInfo `json:"server"`
		Varz   Varz       `json:"data"`
	}

	// VarzResp is a server response from VARZ endpoint, containing general information about the server.
	Varz struct {
		ID                  string            `json:"server_id"`
		Name                string            `json:"server_name"`
		Version             string            `json:"version"`
		Proto               int               `json:"proto"`
		GitCommit           string            `json:"git_commit,omitempty"`
		GoVersion           string            `json:"go"`
		Host                string            `json:"host"`
		Port                int               `json:"port"`
		AuthRequired        bool              `json:"auth_required,omitempty"`
		TLSRequired         bool              `json:"tls_required,omitempty"`
		TLSVerify           bool              `json:"tls_verify,omitempty"`
		IP                  string            `json:"ip,omitempty"`
		ClientConnectURLs   []string          `json:"connect_urls,omitempty"`
		WSConnectURLs       []string          `json:"ws_connect_urls,omitempty"`
		MaxConn             int               `json:"max_connections"`
		MaxSubs             int               `json:"max_subscriptions,omitempty"`
		PingInterval        time.Duration     `json:"ping_interval"`
		MaxPingsOut         int               `json:"ping_max"`
		HTTPHost            string            `json:"http_host"`
		HTTPPort            int               `json:"http_port"`
		HTTPBasePath        string            `json:"http_base_path"`
		HTTPSPort           int               `json:"https_port"`
		AuthTimeout         float64           `json:"auth_timeout"`
		MaxControlLine      int32             `json:"max_control_line"`
		MaxPayload          int               `json:"max_payload"`
		MaxPending          int64             `json:"max_pending"`
		Cluster             ClusterOptsVarz   `json:"cluster,omitempty"`
		Gateway             GatewayOptsVarz   `json:"gateway,omitempty"`
		LeafNode            LeafNodeOptsVarz  `json:"leaf,omitempty"`
		MQTT                MQTTOptsVarz      `json:"mqtt,omitempty"`
		Websocket           WebsocketOptsVarz `json:"websocket,omitempty"`
		JetStream           JetStreamVarz     `json:"jetstream,omitempty"`
		TLSTimeout          float64           `json:"tls_timeout"`
		WriteDeadline       time.Duration     `json:"write_deadline"`
		Start               time.Time         `json:"start"`
		Now                 time.Time         `json:"now"`
		Uptime              string            `json:"uptime"`
		Mem                 int64             `json:"mem"`
		Cores               int               `json:"cores"`
		MaxProcs            int               `json:"gomaxprocs"`
		CPU                 float64           `json:"cpu"`
		Connections         int               `json:"connections"`
		TotalConnections    uint64            `json:"total_connections"`
		Routes              int               `json:"routes"`
		Remotes             int               `json:"remotes"`
		Leafs               int               `json:"leafnodes"`
		InMsgs              int64             `json:"in_msgs"`
		OutMsgs             int64             `json:"out_msgs"`
		InBytes             int64             `json:"in_bytes"`
		OutBytes            int64             `json:"out_bytes"`
		SlowConsumers       int64             `json:"slow_consumers"`
		Subscriptions       uint32            `json:"subscriptions"`
		HTTPReqStats        map[string]uint64 `json:"http_req_stats"`
		ConfigLoadTime      time.Time         `json:"config_load_time"`
		TrustedOperatorsJwt []string          `json:"trusted_operators_jwt,omitempty"`
		SystemAccount       string            `json:"system_account,omitempty"`
		PinnedAccountFail   uint64            `json:"pinned_account_fails,omitempty"`
	}

	// ClusterOptsVarz contains monitoring cluster information
	ClusterOptsVarz struct {
		Name        string   `json:"name,omitempty"`
		Host        string   `json:"addr,omitempty"`
		Port        int      `json:"cluster_port,omitempty"`
		AuthTimeout float64  `json:"auth_timeout,omitempty"`
		URLs        []string `json:"urls,omitempty"`
		TLSTimeout  float64  `json:"tls_timeout,omitempty"`
		TLSRequired bool     `json:"tls_required,omitempty"`
		TLSVerify   bool     `json:"tls_verify,omitempty"`
	}

	// GatewayOptsVarz contains monitoring gateway information
	GatewayOptsVarz struct {
		Name           string                  `json:"name,omitempty"`
		Host           string                  `json:"host,omitempty"`
		Port           int                     `json:"port,omitempty"`
		AuthTimeout    float64                 `json:"auth_timeout,omitempty"`
		TLSTimeout     float64                 `json:"tls_timeout,omitempty"`
		TLSRequired    bool                    `json:"tls_required,omitempty"`
		TLSVerify      bool                    `json:"tls_verify,omitempty"`
		Advertise      string                  `json:"advertise,omitempty"`
		ConnectRetries int                     `json:"connect_retries,omitempty"`
		Gateways       []RemoteGatewayOptsVarz `json:"gateways,omitempty"`
		RejectUnknown  bool                    `json:"reject_unknown,omitempty"` // config got renamed to reject_unknown_cluster
	}

	// RemoteGatewayOptsVarz contains monitoring remote gateway information
	RemoteGatewayOptsVarz struct {
		Name       string   `json:"name"`
		TLSTimeout float64  `json:"tls_timeout,omitempty"`
		URLs       []string `json:"urls,omitempty"`
	}

	// LeafNodeOptsVarz contains monitoring leaf node information
	LeafNodeOptsVarz struct {
		Host        string               `json:"host,omitempty"`
		Port        int                  `json:"port,omitempty"`
		AuthTimeout float64              `json:"auth_timeout,omitempty"`
		TLSTimeout  float64              `json:"tls_timeout,omitempty"`
		TLSRequired bool                 `json:"tls_required,omitempty"`
		TLSVerify   bool                 `json:"tls_verify,omitempty"`
		Remotes     []RemoteLeafOptsVarz `json:"remotes,omitempty"`
	}

	// RemoteLeafOptsVarz contains monitoring remote leaf node information
	RemoteLeafOptsVarz struct {
		LocalAccount string     `json:"local_account,omitempty"`
		TLSTimeout   float64    `json:"tls_timeout,omitempty"`
		URLs         []string   `json:"urls,omitempty"`
		Deny         *DenyRules `json:"deny,omitempty"`
	}

	// DenyRules Contains lists of subjects not allowed to be imported/exported
	DenyRules struct {
		Exports []string `json:"exports,omitempty"`
		Imports []string `json:"imports,omitempty"`
	}

	// MQTTOptsVarz contains monitoring MQTT information
	MQTTOptsVarz struct {
		Host           string        `json:"host,omitempty"`
		Port           int           `json:"port,omitempty"`
		NoAuthUser     string        `json:"no_auth_user,omitempty"`
		AuthTimeout    float64       `json:"auth_timeout,omitempty"`
		TLSMap         bool          `json:"tls_map,omitempty"`
		TLSTimeout     float64       `json:"tls_timeout,omitempty"`
		TLSPinnedCerts []string      `json:"tls_pinned_certs,omitempty"`
		JsDomain       string        `json:"js_domain,omitempty"`
		AckWait        time.Duration `json:"ack_wait,omitempty"`
		MaxAckPending  uint16        `json:"max_ack_pending,omitempty"`
	}

	// WebsocketOptsVarz contains monitoring websocket information
	WebsocketOptsVarz struct {
		Host             string        `json:"host,omitempty"`
		Port             int           `json:"port,omitempty"`
		Advertise        string        `json:"advertise,omitempty"`
		NoAuthUser       string        `json:"no_auth_user,omitempty"`
		JWTCookie        string        `json:"jwt_cookie,omitempty"`
		HandshakeTimeout time.Duration `json:"handshake_timeout,omitempty"`
		AuthTimeout      float64       `json:"auth_timeout,omitempty"`
		NoTLS            bool          `json:"no_tls,omitempty"`
		TLSMap           bool          `json:"tls_map,omitempty"`
		TLSPinnedCerts   []string      `json:"tls_pinned_certs,omitempty"`
		SameOrigin       bool          `json:"same_origin,omitempty"`
		AllowedOrigins   []string      `json:"allowed_origins,omitempty"`
		Compression      bool          `json:"compression,omitempty"`
	}

	// JetStreamVarz contains basic runtime information about jetstream
	JetStreamVarz struct {
		Config *JetStreamConfig `json:"config,omitempty"`
		Stats  *JetStreamStats  `json:"stats,omitempty"`
		Meta   *MetaClusterInfo `json:"meta,omitempty"`
	}

	// Statistics about JetStream for this server.
	JetStreamStats struct {
		Memory         uint64            `json:"memory"`
		Store          uint64            `json:"storage"`
		ReservedMemory uint64            `json:"reserved_memory"`
		ReservedStore  uint64            `json:"reserved_storage"`
		Accounts       int               `json:"accounts"`
		HAAssets       int               `json:"ha_assets"`
		API            JetStreamAPIStats `json:"api"`
	}

	// JetStreamConfig determines this server's configuration.
	// MaxMemory and MaxStore are in bytes.
	JetStreamConfig struct {
		MaxMemory  int64  `json:"max_memory"`
		MaxStore   int64  `json:"max_storage"`
		StoreDir   string `json:"store_dir,omitempty"`
		Domain     string `json:"domain,omitempty"`
		CompressOK bool   `json:"compress_ok,omitempty"`
		UniqueTag  string `json:"unique_tag,omitempty"`
	}

	JetStreamAPIStats struct {
		Total    uint64 `json:"total"`
		Errors   uint64 `json:"errors"`
		Inflight uint64 `json:"inflight,omitempty"`
	}

	// MetaClusterInfo shows information about the meta group.
	MetaClusterInfo struct {
		Name     string      `json:"name,omitempty"`
		Leader   string      `json:"leader,omitempty"`
		Peer     string      `json:"peer,omitempty"`
		Replicas []*PeerInfo `json:"replicas,omitempty"`
		Size     int         `json:"cluster_size"`
	}

	// PeerInfo shows information about all the peers in the cluster that
	// are supporting the stream or consumer.
	PeerInfo struct {
		Name    string        `json:"name"`
		Current bool          `json:"current"`
		Offline bool          `json:"offline,omitempty"`
		Active  time.Duration `json:"active"`
		Lag     uint64        `json:"lag,omitempty"`
		Peer    string        `json:"peer"`
	}

	// In the context of system events, VarzEventOptions are options passed to Varz
	VarzEventOptions struct {
		EventFilterOptions
	}

	// Common filter options for system requests STATSZ VARZ SUBSZ CONNZ ROUTEZ GATEWAYZ LEAFZ
	EventFilterOptions struct {
		Name    string   `json:"server_name,omitempty"` // filter by server name
		Cluster string   `json:"cluster,omitempty"`     // filter by cluster name
		Host    string   `json:"host,omitempty"`        // filter by host name
		Tags    []string `json:"tags,omitempty"`        // filter by tags (must match all tags)
		Domain  string   `json:"domain,omitempty"`      // filter by JS domain
	}
)

// Varz returns general server information
func (s *System) Varz(id string, opts VarzEventOptions) (*VarzResp, error) {
	conn := s.nc
	subj := fmt.Sprintf(srvVarzSubj, id)
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := conn.Request(subj, payload, DefaultRequestTimeout)
	if err != nil {
		return nil, err
	}

	var varzResp VarzResp
	if err := json.Unmarshal(resp.Data, &varzResp); err != nil {
		return nil, err
	}

	return &varzResp, nil
}

func (s *System) VarzPing(opts VarzEventOptions) ([]VarzResp, error) {
	subj := fmt.Sprintf(srvVarzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.RequestMany(subj, payload)
	if err != nil {
		return nil, err
	}
	srvVarz := make([]VarzResp, 0, len(resp))
	for _, msg := range resp {
		var varzResp VarzResp
		if err := json.Unmarshal(msg.Data, &varzResp); err != nil {
			return nil, err
		}
		srvVarz = append(srvVarz, varzResp)
	}
	return srvVarz, nil
}
