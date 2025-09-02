package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

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

type sortableEntry struct {
	key     string
	replica *streamDetail
}

func main() {
	log.SetFlags(0)
	var (
		urls, sname, creds string
		user, pass         string
		timeout            string
		unsyncedFilter     bool
		health             bool
		expected           int
		readTimeout        int
		stdin              bool
		human              bool
		sortBy             string
		sortOrder          string
		backup             bool
		compare            string
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	flag.StringVar(&creds, "creds", "", "The NATS credentials")
	flag.StringVar(&sname, "stream", "", "Select a single stream")
	flag.StringVar(&user, "user", "", "User")
	flag.StringVar(&pass, "pass", "", "Pass")
	flag.BoolVar(&health, "health", false, "Check health from streams")
	flag.StringVar(&timeout, "timeout", "30s", "Connect timeout (e.g. 30s, 1m)")
	flag.IntVar(&readTimeout, "read-timeout", 5, "Read timeout in seconds")
	flag.IntVar(&expected, "expected", 3, "Expected number of servers")
	flag.BoolVar(&unsyncedFilter, "unsynced", false, "Filter by streams that are out of sync")
	flag.BoolVar(&stdin, "stdin", false, "Process the contents from STDIN")
	flag.BoolVar(&human, "human", false, "Format bytes in human-readable units (KB, MB, GB)")
	flag.StringVar(&sortBy, "sort", "", "Sort by field: bytes, messages, consumers, subjects")
	flag.StringVar(&sortOrder, "order", "desc", "Sort order: asc or desc (default: desc)")
	flag.BoolVar(&backup, "backup", false, "Save JSZ responses to jsz-YYYYMMDD.json file")
	flag.StringVar(&compare, "compare", "", "Compare against previous backup file (shows deltas)")
	flag.Parse()

	// Load comparison data if compare flag is provided
	var compareData map[string]*streamDetail
	if compare != "" {
		var err error
		compareData, err = loadComparisonData(compare)
		if err != nil {
			log.Fatalf("Failed to load comparison data: %v", err)
		}
		log.Printf("Loaded comparison data from %s", compare)
	}

	start := time.Now()

	timeoutDuration, parseErr := time.ParseDuration(timeout)
	if parseErr != nil {
		log.Fatalf("Invalid timeout duration: %v", parseErr)
	}

	opts := []nats.Option{
		nats.Timeout(timeoutDuration),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if user != "" && pass != "" {
		opts = append(opts, nats.UserInfo(user, pass))
	}

	var (
		nc      *nats.Conn
		servers []JSZResp
		err     error
		sys     SysClient
	)
	if stdin {
		servers = make([]JSZResp, 0)
		reader := bufio.NewReader(os.Stdin)

		for i := 0; i < expected; i++ {
			data, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if len(data) > 0 {
				var jszResp JSZResp
				if err := json.Unmarshal([]byte(data), &jszResp); err != nil {
					log.Fatal(err)
				}
				servers = append(servers, jszResp)
			}
		}
	} else {
		nc, err = nats.Connect(urls, opts...)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Connected in %.3fs", time.Since(start).Seconds())

		start = time.Now()
		sys = Sys(nc)
		fetchTimeout := FetchTimeout(timeoutDuration)
		fetchExpected := FetchExpected(expected)
		fetchReadTimeout := FetchReadTimeout(time.Duration(readTimeout) * time.Second)
		servers, err = sys.JszPing(JszEventOptions{
			JszOptions: JszOptions{
				Streams:    true,
				RaftGroups: true,
			},
		}, fetchTimeout, fetchReadTimeout, fetchExpected)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Response took %.3fs", time.Since(start).Seconds())

		// Save backup if requested
		if backup {
			if err := saveBackup(servers, compare != ""); err != nil {
				log.Printf("Failed to save backup: %v", err)
			}
		}

	}

	header := fmt.Sprintf("Servers: %d", len(servers))
	fmt.Println(header)

	// Collect all info from servers.
	streams := make(map[string]map[string]*streamDetail)
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				var ok bool
				var m map[string]*streamDetail
				key := fmt.Sprintf("%s|%s", acc.Name, stream.RaftGroup)
				if m, ok = streams[key]; !ok {
					m = make(map[string]*streamDetail)
					streams[key] = m
				}
				m[server.Name] = &streamDetail{
					ServerID:   server.ID,
					StreamName: stream.Name,
					Account:    acc.Name,
					AccountID:  acc.Id,
					RaftGroup:  stream.RaftGroup,
					State:      stream.State,
					Cluster:    stream.Cluster,
				}
			}
		}
	}

	// Create sortable entries
	entries := make([]sortableEntry, 0)
	for k := range streams {
		for kk := range streams[k] {
			entries = append(entries, sortableEntry{
				key:     fmt.Sprintf("%s/%s", k, kk),
				replica: streams[k][kk],
			})
		}
	}

	// Sort entries if sort field is specified
	if sortBy != "" {
		sortEntries(entries, sortBy, sortOrder)
	} else {
		// Default sorting by key (alphabetical)
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].key < entries[j].key
		})
	}

	// Extract keys from sorted entries
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.key
	}

	line := strings.Repeat("-", 220)
	fmt.Printf("Streams: %d\n", len(keys))
	fmt.Println()

	fields := []any{"STREAM REPLICA", "RAFT", "ACCOUNT", "ACC_ID", "NODE", "MESSAGES", "BYTES", "SUBJECTS", "DELETED", "CONSUMERS", "SEQUENCES", "STATUS"}
	fmt.Printf("%-40s %-15s %-10s %-56s %-25s %-15s %-15s %-15s %-15s %-15s %-30s %-30s\n", fields...)

	var prev, prevAccount string
	for i, k := range keys {
		var unsynced bool
		av := strings.Split(k, "|")
		accName := av[0]
		v := strings.Split(av[1], "/")
		streamName, serverName := v[0], v[1]
		if sname != "" && streamName != sname {
			continue
		}

		key := fmt.Sprintf("%s|%s", accName, streamName)
		stream := streams[key]
		replica := stream[serverName]
		status := "IN SYNC"

		// Make comparisons against other peers.
		for _, peer := range stream {
			if peer.State.Msgs != replica.State.Msgs && peer.State.Bytes != replica.State.Bytes {
				status = "UNSYNCED"
				unsynced = true
			}
			if peer.State.FirstSeq != replica.State.FirstSeq {
				status = "UNSYNCED"
				unsynced = true
			}
			if peer.State.LastSeq != replica.State.LastSeq {
				status = "UNSYNCED"
				unsynced = true
			}
			// Cannot trust results unless coming from the stream leader.
			// Need Stream INFO and collect multiple responses instead.
			if peer.Cluster.Leader != replica.Cluster.Leader {
				status = "MULTILEADER"
				unsynced = true
			}
		}
		if unsyncedFilter && !unsynced {
			continue
		}
		if i > 0 && prev != streamName || prevAccount != accName {
			fmt.Println(line)
		}

		sf := make([]any, 0)
		if replica == nil {
			status = "?"
			unsynced = true
			continue
		}
		var alen int
		if len(replica.Account) > 10 {
			alen = 10
		} else {
			alen = len(replica.Account)
		}
		sf = append(sf, replica.StreamName)
		sf = append(sf, replica.RaftGroup)
		sf = append(sf, strings.Replace(replica.Account[:alen], " ", "_", -1))
		sf = append(sf, replica.AccountID)

		// Mark it in case it is a leader.
		var suffix string
		var isStreamLeader bool
		if serverName == replica.Cluster.Leader {
			isStreamLeader = true
			suffix = "*"
		} else if replica.Cluster.Leader == "" {
			status = "LEADERLESS"
			unsynced = true
		}

		var replicasInfo string
		for _, r := range replica.Cluster.Replicas {
			if isStreamLeader && r.Name == replica.Cluster.Leader {
				status = "LEADER_IS_FOLLOWER"
				unsynced = true
			}
			info := fmt.Sprintf("%s(current=%-5v,offline=%v)", r.Name, r.Current, r.Offline)
			replicasInfo = fmt.Sprintf("%-40s %s", info, replicasInfo)
		}

		s := fmt.Sprintf("%s%s", serverName, suffix)
		sf = append(sf, s)

		// Add values or deltas based on compare mode
		if compareData != nil {
			// Compare mode - show deltas
			compareKey := fmt.Sprintf("%s|%s/%s", replica.Account, replica.RaftGroup, serverName)
			if compareReplica, exists := compareData[compareKey]; exists {
				sf = append(sf, formatDelta(replica.State.Msgs, compareReplica.State.Msgs, false))
				sf = append(sf, formatDelta(replica.State.Bytes, compareReplica.State.Bytes, human))
				sf = append(sf, formatDelta(replica.State.NumSubjects, compareReplica.State.NumSubjects, false))
				sf = append(sf, formatDelta(replica.State.NumDeleted, compareReplica.State.NumDeleted, false))
				sf = append(sf, formatDelta(replica.State.Consumers, compareReplica.State.Consumers, false))
				sf = append(sf, formatSequenceDelta(replica.State.FirstSeq, compareReplica.State.FirstSeq))
				sf = append(sf, formatSequenceDelta(replica.State.LastSeq, compareReplica.State.LastSeq))
			} else {
				// New stream - show as all positive deltas
				sf = append(sf, fmt.Sprintf("+%d", replica.State.Msgs))
				if human && replica.State.Bytes > 1024 {
					sf = append(sf, fmt.Sprintf("+%s", formatBytes(replica.State.Bytes)))
				} else {
					sf = append(sf, fmt.Sprintf("+%d", replica.State.Bytes))
				}
				sf = append(sf, fmt.Sprintf("+%d", replica.State.NumSubjects))
				sf = append(sf, fmt.Sprintf("+%d", replica.State.NumDeleted))
				sf = append(sf, fmt.Sprintf("+%d", replica.State.Consumers))
				sf = append(sf, fmt.Sprintf("+%d", replica.State.FirstSeq))
				sf = append(sf, fmt.Sprintf("+%d", replica.State.LastSeq))
			}
		} else {
			// Normal mode - show absolute values
			sf = append(sf, replica.State.Msgs)
			if human {
				sf = append(sf, formatBytes(replica.State.Bytes))
			} else {
				sf = append(sf, replica.State.Bytes)
			}
			sf = append(sf, replica.State.NumSubjects)
			sf = append(sf, replica.State.NumDeleted)
			sf = append(sf, replica.State.Consumers)
			sf = append(sf, replica.State.FirstSeq)
			sf = append(sf, replica.State.LastSeq)
		}
		sf = append(sf, status)
		sf = append(sf, replica.Cluster.Leader)

		// Include Healthz if option added.
		var healthStatus string
		if health {
			hstatus, err := sys.Healthz(replica.ServerID, HealthzOptions{
				Account: replica.Account,
				Stream:  replica.StreamName,
			})
			if err != nil {
				healthStatus = err.Error()
			} else {
				healthStatus = fmt.Sprintf(":%s:%s", hstatus.Healthz.Status, hstatus.Healthz.Error)
			}
			replicasInfo = fmt.Sprintf("health:%q %s", healthStatus, replicasInfo)
		}

		sf = append(sf, replicasInfo)
		if compareData != nil {
			// Compare mode - all numeric fields are now strings (deltas)
			fmt.Printf("%-40s %-15s %-10s %-56s %-25s %-15s %-15s %-15s %-15s %-15s %-15s %-15s| %-10s | leader: %s | peers: %s\n", sf...)
		} else if human {
			fmt.Printf("%-40s %-15s %-10s %-56s %-25s %-15d %-15s %-15d %-15d %-15d %-15d %-15d| %-10s | leader: %s | peers: %s\n", sf...)
		} else {
			fmt.Printf("%-40s %-15s %-10s %-56s %-25s %-15d %-15d %-15d %-15d %-15d %-15d %-15d| %-10s | leader: %s | peers: %s\n", sf...)
		}

		prev = streamName
		prevAccount = accName
	}
}

func formatBytes(bytes uint64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	}
	
	units := []struct {
		threshold uint64
		divisor   uint64
		suffix    string
	}{
		{1024 * 1024 * 1024 * 1024 * 1024 * 1024, 1024 * 1024 * 1024 * 1024 * 1024 * 1024, "EB"},
		{1024 * 1024 * 1024 * 1024 * 1024, 1024 * 1024 * 1024 * 1024 * 1024, "PB"},
		{1024 * 1024 * 1024 * 1024, 1024 * 1024 * 1024 * 1024, "TB"},
		{1024 * 1024 * 1024, 1024 * 1024 * 1024, "GB"},
		{1024 * 1024, 1024 * 1024, "MB"},
		{1024, 1024, "KB"},
	}
	
	for _, unit := range units {
		if bytes >= unit.threshold {
			return fmt.Sprintf("%.1f%s", float64(bytes)/float64(unit.divisor), unit.suffix)
		}
	}
	return "" // unreachable
}

func sortEntries(entries []sortableEntry, sortBy, sortOrder string) {
	isDesc := sortOrder == "desc"
	
	sort.Slice(entries, func(i, j int) bool {
		var less bool
		
		switch sortBy {
		case "bytes":
			less = entries[i].replica.State.Bytes < entries[j].replica.State.Bytes
		case "messages":
			less = entries[i].replica.State.Msgs < entries[j].replica.State.Msgs
		case "consumers":
			less = entries[i].replica.State.Consumers < entries[j].replica.State.Consumers
		case "subjects":
			less = entries[i].replica.State.NumSubjects < entries[j].replica.State.NumSubjects
		default:
			// Default to alphabetical by key
			less = entries[i].key < entries[j].key
		}
		
		if isDesc {
			return !less
		}
		return less
	})
}

func saveBackup(servers []JSZResp, isCompareMode bool) error {
	var filename string
	if isCompareMode {
		filename = "jsz-latest.json"
	} else {
		now := time.Now()
		filename = fmt.Sprintf("jsz-%04d%02d%02d.json", now.Year(), now.Month(), now.Day())
	}
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create backup file %s: %w", filename, err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	for _, server := range servers {
		if err := encoder.Encode(server); err != nil {
			return fmt.Errorf("failed to encode server data: %w", err)
		}
	}
	
	log.Printf("Backup saved to %s (%d servers)", filename, len(servers))
	return nil
}

func loadComparisonData(filename string) (map[string]*streamDetail, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open comparison file: %w", err)
	}
	defer file.Close()

	compareData := make(map[string]*streamDetail)
	decoder := json.NewDecoder(file)
	
	for {
		var jszResp JSZResp
		if err := decoder.Decode(&jszResp); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode comparison data: %w", err)
		}

		server := jszResp.Server
		jsz := jszResp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				key := fmt.Sprintf("%s|%s/%s", acc.Name, stream.RaftGroup, server.Name)
				compareData[key] = &streamDetail{
					ServerID:   server.ID,
					StreamName: stream.Name,
					Account:    acc.Name,
					AccountID:  acc.Id,
					RaftGroup:  stream.RaftGroup,
					State:      stream.State,
					Cluster:    stream.Cluster,
				}
			}
		}
	}

	return compareData, nil
}

func formatDelta(current, previous interface{}, human bool) string {
	var currentVal, previousVal int64
	
	switch v := current.(type) {
	case uint64:
		currentVal = int64(v)
	case int:
		currentVal = int64(v)
	case int64:
		currentVal = v
	}
	
	switch v := previous.(type) {
	case uint64:
		previousVal = int64(v)
	case int:
		previousVal = int64(v)
	case int64:
		previousVal = v
	}
	
	if previousVal == 0 && currentVal == 0 {
		return "0"
	}
	
	delta := currentVal - previousVal
	if delta == 0 {
		return "0"
	}
	
	var deltaStr string
	if delta > 0 {
		if human && (delta > 1024) {
			deltaStr = fmt.Sprintf("+%s", formatBytes(uint64(delta)))
		} else {
			deltaStr = fmt.Sprintf("+%d", delta)
		}
	} else {
		if human && (-delta > 1024) {
			deltaStr = fmt.Sprintf("-%s", formatBytes(uint64(-delta)))
		} else {
			deltaStr = fmt.Sprintf("%d", delta)
		}
	}
	
	return deltaStr
}

func formatSequenceDelta(current, previous uint64) string {
	if previous == 0 && current == 0 {
		return "0"
	}
	
	delta := int64(current) - int64(previous)
	if delta == 0 {
		return "0"
	}
	
	if delta > 0 {
		return fmt.Sprintf("+%d", delta)
	}
	return fmt.Sprintf("%d", delta)
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
