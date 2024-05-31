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
	AccountID            string
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

type streamDetail struct {
	StreamName   string
	Account      string
	AccountID    string
	RaftGroup    string
	State        nats.StreamState
	Cluster      *nats.ClusterInfo
	HealthStatus string
	ServerID     string
}

func main() {
	log.SetFlags(0)
	var (
		urls, sname, cname string
		user, pass         string
		creds              string
		timeout            int
		health             bool
		unsyncedFilter     bool
		expected           int
		readTimeout        int
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	flag.StringVar(&creds, "creds", "", "The NATS credentials")
	flag.StringVar(&user, "user", "", "User")
	flag.StringVar(&pass, "pass", "", "Pass")
	flag.StringVar(&sname, "stream", "", "Select a single stream")
	flag.StringVar(&cname, "consumer", "", "Select a single consumer")
	flag.BoolVar(&health, "health", false, "Check health from consumers")
	flag.IntVar(&timeout, "timeout", 30, "Connect timeout")
	flag.IntVar(&readTimeout, "read-timeout", 5, "Read timeout in seconds")
	flag.IntVar(&expected, "expected", 3, "Expected number of servers")
	flag.BoolVar(&unsyncedFilter, "unsynced", false, "Filter by streams that are out of sync")
	flag.Parse()

	start := time.Now()

	opts := []nats.Option{
		nats.Timeout(time.Duration(timeout) * time.Second),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if user != "" && pass != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}

	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected in %.3fs", time.Since(start).Seconds())

	start = time.Now()
	sys := Sys(nc)
	fetchTimeout := FetchTimeout(time.Duration(timeout) * time.Second)
	fetchExpected := FetchExpected(expected)
	fetchReadTimeout := FetchReadTimeout(time.Duration(readTimeout) * time.Second)
	servers, err := sys.JszPing(JszEventOptions{
		JszOptions: JszOptions{
			Streams:    true,
			Consumer:   true,
			RaftGroups: true,
		},
	}, fetchTimeout, fetchReadTimeout, fetchExpected)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Response took %.3fs", time.Since(start).Seconds())

	header := fmt.Sprintf("Servers: %d", len(servers))
	fmt.Println(header)

	streams := make(map[string]map[string]*streamDetail)
	consumers := make(map[string]map[string]*ConsumerDetail)
	// Collect all info from servers.
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var mok bool
				var ms map[string]*streamDetail
				mkey := fmt.Sprintf("%s|%s", acc.Name, stream.RaftGroup)
				if ms, mok = streams[mkey]; !mok {
					ms = make(map[string]*streamDetail)
					streams[mkey] = ms
				}
				ms[server.Name] = &streamDetail{
					ServerID:   server.ID,
					StreamName: stream.Name,
					Account:    acc.Name,
					AccountID:  acc.Id,
					RaftGroup:  stream.RaftGroup,
					State:      stream.State,
					Cluster:    stream.Cluster,
				}

				for _, consumer := range stream.Consumer {
					var raftGroup string
					for _, cr := range stream.ConsumerRaftGroups {
						if cr.Name == consumer.Name {
							raftGroup = cr.RaftGroup
							break
						}
					}

					var ok bool
					var m map[string]*ConsumerDetail
					key := fmt.Sprintf("%s|%s", acc.Name, raftGroup)
					if m, ok = consumers[key]; !ok {
						m = make(map[string]*ConsumerDetail)
						consumers[key] = m
					}

					m[server.Name] = &ConsumerDetail{
						ServerID:     server.ID,
						StreamName:   consumer.Stream,
						ConsumerName: consumer.Name,
						Account:      acc.Name,
						AccountID:    acc.Id,
						RaftGroup:    raftGroup,
						// StreamRaftGroup:      stream.RaftGroup,
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

	fields := []any{"CONSUMER", "STREAM", "RAFT", "ACCOUNT", "ACC_ID", "NODE", "DELIVERED (S,C)", "ACK_FLOOR (S,C)", "COUNTERS", "STATUS"}
	fmt.Printf("%-10s %-15s %-15s %-15s %-35s %-15s | %-20s | %-20s | %-25s | %-30s\n", fields...)

	var prev, prevAccount string
	for i, k := range keys {
		var unsynced bool
		// v := strings.Split(k, "/")
		av := strings.Split(k, "|")
		accName := av[0]
		v := strings.Split(av[1], "/")
		consumerName, serverName := v[0], v[1]
		if cname != "" && consumerName != cname {
			continue
		}

		key := fmt.Sprintf("%s|%s", accName, consumerName)
		consumer := consumers[key]
		replica := consumer[serverName]
		var status string
		statuses := make(map[string]bool)

		// stream := streams[replica.StreamName]
		// fmt.Println(streams)

		if replica.State.LastSeq < replica.DeliveredStreamSeq {
			statuses["UNSYNCED:DELIVERED_AHEAD_OF_STREAM_SEQ"] = true
			unsynced = true
		}

		if replica.State.LastSeq < replica.AckFloorStreamSeq {
			statuses["UNSYNCED:ACKFLOOR_AHEAD_OF_STREAM_SEQ"] = true
			unsynced = true
		}

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
				unsynced = false
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
			statuses["EMPTY"] = true
			// unsynced = true
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
		if i > 0 && prev != consumerName || prevAccount != accName {
			fmt.Println(line)
		}
		var alen int
		if len(replica.Account) > 10 {
			alen = 10
		} else {
			alen = len(replica.Account)
		}

		sf := make([]any, 0)
		sf = append(sf, replica.ConsumerName)
		sf = append(sf, replica.StreamName)
		sf = append(sf, replica.RaftGroup)
		// sf = append(sf, replica.Account)
		sf = append(sf, strings.Replace(replica.Account[:alen], " ", "_", -1))
		sf = append(sf, replica.AccountID)

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

		progress := "0%"
		if replica.State.LastSeq > 0 {
			result := (replica.DeliveredStreamSeq / replica.State.LastSeq) * 100
			progress = fmt.Sprintf("%v%%", result)
		}
		sf = append(sf, fmt.Sprintf("%d / %d (%v) | %d", replica.DeliveredStreamSeq, replica.State.LastSeq, progress, replica.DeliveredConsumerSeq))
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
		fmt.Printf("%-10s %-15s %-15s %-15s %-35s %-15s | %-20s | %-20s | %-25s | %-12s | %s \n", sf...)

		prev = consumerName
		prevAccount = accName
	}
}

const (
	srvHealthzSubj = "$SYS.REQ.SERVER.%s.HEALTHZ"
	srvJszSubj     = "$SYS.REQ.SERVER.%s.JSZ"
)

var (
	ErrValidation      = errors.New("validation error")
	ErrInvalidServerID = errors.New("server with given ID does not exist")
)

// SysClient can be used to request monitoring data from the server.
type SysClient struct {
	nc *nats.Conn
}

func Sys(nc *nats.Conn) SysClient {
	return SysClient{
		nc: nc,
	}
}

func (s *SysClient) JszPing(opts JszEventOptions, fopts ...FetchOpt) ([]JSZResp, error) {
	subj := fmt.Sprintf(srvJszSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.Fetch(subj, payload, fopts...)
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

type FetchOpts struct {
	Timeout     time.Duration
	ReadTimeout time.Duration
	Expected    int
}

type FetchOpt func(*FetchOpts) error

func FetchTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: timeout has to be greater than 0", ErrValidation)
		}
		opts.Timeout = timeout
		return nil
	}
}

func FetchReadTimeout(timeout time.Duration) FetchOpt {
	return func(opts *FetchOpts) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: read timeout has to be greater than 0", ErrValidation)
		}
		opts.ReadTimeout = timeout
		return nil
	}
}

func FetchExpected(expected int) FetchOpt {
	return func(opts *FetchOpts) error {
		if expected <= 0 {
			return fmt.Errorf("%w: expected request count has to be greater than 0", ErrValidation)
		}
		opts.Expected = expected
		return nil
	}
}

func (s *SysClient) Fetch(subject string, data []byte, opts ...FetchOpt) ([]*nats.Msg, error) {
	if subject == "" {
		return nil, fmt.Errorf("%w: expected subject 0", ErrValidation)
	}

	conn := s.nc
	reqOpts := &FetchOpts{}
	for _, opt := range opts {
		if err := opt(reqOpts); err != nil {
			return nil, err
		}
	}

	inbox := nats.NewInbox()
	res := make([]*nats.Msg, 0)
	msgsChan := make(chan *nats.Msg, 100)

	readTimer := time.NewTimer(reqOpts.ReadTimeout)
	sub, err := conn.Subscribe(inbox, func(msg *nats.Msg) {
		readTimer.Reset(reqOpts.ReadTimeout)
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
			if reqOpts.Expected != -1 && len(res) == reqOpts.Expected {
				return res, nil
			}
		case <-readTimer.C:
			return res, nil
		case <-time.After(reqOpts.Timeout):
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
const DefaultRequestTimeout = 60 * time.Second

func (s *SysClient) Healthz(id string, opts HealthzOptions) (*HealthzResp, error) {
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

type (

	// ServerInfo identifies remote servers.
	ServerInfo struct {
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

type (
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

	// Common filter options for system requests STATSZ VARZ SUBSZ CONNZ ROUTEZ GATEWAYZ LEAFZ
	EventFilterOptions struct {
		Name    string   `json:"server_name,omitempty"` // filter by server name
		Cluster string   `json:"cluster,omitempty"`     // filter by cluster name
		Host    string   `json:"host,omitempty"`        // filter by host name
		Tags    []string `json:"tags,omitempty"`        // filter by tags (must match all tags)
		Domain  string   `json:"domain,omitempty"`      // filter by JS domain
	}
)
