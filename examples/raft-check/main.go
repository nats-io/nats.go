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

type raftDetail struct {
	ServerID  string
	RaftGroup string
	Status    RaftzGroup
}

func main() {
	log.SetFlags(0)
	var (
		urls, sname, creds string
		user, pass         string
		account, raftGroup string
		timeout            int
		unsyncedFilter     bool
		expected           int
		readTimeout        int
		stdin              bool
	)
	flag.StringVar(&urls, "s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	flag.StringVar(&creds, "creds", "", "The NATS credentials")
	flag.StringVar(&sname, "stream", "", "Select a single stream")
	flag.StringVar(&user, "user", "", "User")
	flag.StringVar(&pass, "pass", "", "Pass")
	flag.StringVar(&account, "account", "", "Account Filter")
	flag.StringVar(&raftGroup, "group", "", "Group Filter")
	flag.IntVar(&timeout, "timeout", 30, "Connect timeout in seconds")
	flag.IntVar(&readTimeout, "read-timeout", 5, "Read timeout in seconds")
	flag.IntVar(&expected, "expected", 3, "Expected number of servers")
	flag.BoolVar(&unsyncedFilter, "unsynced", false, "Filter by streams that are out of sync")
	flag.BoolVar(&stdin, "stdin", false, "Process the contents from STDIN")
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

	var (
		nc      *nats.Conn
		servers []RaftzResp
		err     error
		sys     SysClient
	)
	if stdin {
		servers = make([]RaftzResp, 0)
		reader := bufio.NewReader(os.Stdin)

		for i := 0; i < expected; i++ {
			data, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if len(data) > 0 {
				var raftzResp RaftzResp
				if err := json.Unmarshal([]byte(data), &raftzResp); err != nil {
					log.Println(err)
					continue
				}
				servers = append(servers, raftzResp)
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
		fetchTimeout := FetchTimeout(time.Duration(timeout) * time.Second)
		fetchExpected := FetchExpected(expected)
		fetchReadTimeout := FetchReadTimeout(time.Duration(readTimeout) * time.Second)
		servers, err = sys.RaftzPing(RaftzOptions{
			AccountFilter: account,
			GroupFilter:   raftGroup,
		}, fetchTimeout, fetchReadTimeout, fetchExpected)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Response took %.3fs", time.Since(start).Seconds())
	}

	header := fmt.Sprintf("Servers: %d", len(servers))
	fmt.Println(header)

	// Collect all info from servers
	//
	// RaftGroup -> Nodes -> Raft Status
	//
	rafts := make(map[string]map[string]*raftDetail)
	for _, resp := range servers {
		server := resp.Server
		raftz := resp.Raftz
		for _, raftGroups := range raftz {
			for raftGroupName, raftGroup := range raftGroups {
				var ok bool
				var m map[string]*raftDetail
				key := raftGroupName
				if m, ok = rafts[key]; !ok {
					m = make(map[string]*raftDetail)
					rafts[key] = m
				}
				m[server.Name] = &raftDetail{
					ServerID:  server.ID,
					RaftGroup: raftGroupName,
					Status:    raftGroup,
				}
			}
		}
	}
	keys := make([]string, 0)
	for k := range rafts {
		for kk := range rafts[k] {
			keys = append(keys, fmt.Sprintf("%s/%s", k, kk))
		}
	}
	sort.Strings(keys)

	line := strings.Repeat("-", 265)
	fmt.Printf("Rafts: %d\n", len(keys))
	fmt.Println()

	fields := []any{"RAFT", "NODE", "COMMITTED", "APPLIED", "TERM", "PTERM", "PINDEX", "MESSAGES", "BYTES", "FIRST", "LAST", "STATUS", "TIME", "IPQ", "PEERS"}
	fmt.Printf("%-15s %-25s %-15s %-15s %-8s %-8s %-15s %-15s %-15s %-15s %-15s  %-15s   %-30s %-25s %-10s\n", fields...)

	var prev string
	for i, k := range keys {
		var unsynced bool
		v := strings.Split(k, "/")
		raftGroup, serverName := v[0], v[1]
		status := "IN SYNC"
		nodes := rafts[raftGroup]
		r := nodes[serverName]
		raft := r.Status
		sf := make([]any, 0)
		sf = append(sf, raftGroup)
		sname := serverName
		if raft.ID == raft.Leader {
			sname += "*"
		}
		// Make comparisons against other peers.
		for _, n := range nodes {
			peer := n.Status
			if raft.Committed != peer.Committed {
				status = "UNSYNCED"
				unsynced = true
			}
			if raft.Applied != peer.Applied {
				status = "UNSYNCED"
				unsynced = true
			}
			if raft.Term != peer.Term {
				status = "UNSYNCED"
				unsynced = true
			}
			if raft.PTerm != peer.PTerm {
				status = "UNSYNCED"
				unsynced = true
			}
			// if raft.WAL.Msgs != peer.WAL.Msgs {
			// 	status = "UNSYNCED"
			// 	unsynced = true
			// }
			// if raft.WAL.Bytes != peer.WAL.Bytes {
			// 	status = "UNSYNCED"
			// 	unsynced = true
			// }
			// if raft.WAL.FirstSeq != peer.WAL.FirstSeq {
			// 	status = "UNSYNCED"
			// 	unsynced = true
			// }
			if raft.WAL.LastSeq != peer.WAL.LastSeq {
				status = "UNSYNCED"
				unsynced = true
			}
		}

		sf = append(sf, sname)
		sf = append(sf, raft.Committed)
		sf = append(sf, raft.Applied)
		sf = append(sf, raft.Term)
		sf = append(sf, raft.PTerm)
		sf = append(sf, raft.PIndex)
		sf = append(sf, raft.WAL.Msgs)
		sf = append(sf, raft.WAL.Bytes)

		if raft.WAL.Msgs == 0 || raft.WAL.Bytes == 0 {
			status = "EMPTY"
		}

		sf = append(sf, raft.WAL.FirstSeq)
		sf = append(sf, raft.WAL.LastSeq)
		firstTime := raft.WAL.FirstTime.Truncate(time.Second).UTC()
		sf = append(sf, status)
		sf = append(sf, firstTime)

		// Counters
		sf = append(sf, fmt.Sprintf("(p:%d, e:%d, r:%d, a:%d)",
			raft.IPQPropLen,
			raft.IPQEntryLen,
			raft.IPQRespLen,
			raft.IPQApplyLen,
		))

		var replicasInfo string
		for _, r := range raft.Peers {
			info := fmt.Sprintf("%s(known=%v,last_seen=%v,last_index=%v)",
				r.Name, r.Known, r.LastSeen, r.LastReplicatedIndex,
			)
			replicasInfo = fmt.Sprintf("%-40s %s", info, replicasInfo)
		}

		// Skip if only looking for unsynced rafts.
		if unsyncedFilter && !unsynced {
			continue
		}
		if i > 0 && prev != raftGroup {
			fmt.Println(line)
		}
		sf = append(sf, replicasInfo)
		fmt.Printf("%-15v %-25v %-15v %-15v %-8v %-8v %-15v %-15v %-15v %-15v %-15d | %-15s | %-10s | %-20v | %-30v\n", sf...)
		prev = raftGroup
	}
}

const (
	srvHealthzSubj = "$SYS.REQ.SERVER.%s.HEALTHZ"
	srvJszSubj     = "$SYS.REQ.SERVER.%s.JSZ"
	srvRaftzSubj   = "$SYS.REQ.SERVER.%s.RAFTZ"
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

func (s *SysClient) RaftzPing(opts RaftzOptions, fopts ...FetchOpt) ([]RaftzResp, error) {
	subj := fmt.Sprintf(srvRaftzSubj, "PING")
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	resp, err := s.Fetch(subj, payload, fopts...)
	if err != nil {
		return nil, err
	}
	servers := make([]RaftzResp, 0, len(resp))
	for _, msg := range resp {
		var raftzResp RaftzResp
		if err := json.Unmarshal([]byte(msg.Data), &raftzResp); err != nil {
			return nil, err
		}
		servers = append(servers, raftzResp)
	}
	return servers, nil
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

	RaftzOptions struct {
		AccountFilter string `json:"account"`
		GroupFilter   string `json:"group"`
	}

	RaftzResp struct {
		Server ServerInfo  `json:"server"`
		Raftz  RaftzStatus `json:"data"`
	}

	RaftzGroup struct {
		// ID Skip
		ID            string                    `json:"id"`
		State         string                    `json:"state"`
		Size          int                       `json:"size"`
		QuorumNeeded  int                       `json:"quorum_needed"`
		Observer      bool                      `json:"observer,omitempty"`
		Paused        bool                      `json:"paused,omitempty"`
		Committed     uint64                    `json:"committed"`
		Applied       uint64                    `json:"applied"`
		CatchingUp    bool                      `json:"catching_up,omitempty"`
		Leader        string                    `json:"leader,omitempty"`
		EverHadLeader bool                      `json:"ever_had_leader"`
		Term          uint64                    `json:"term"`
		Vote          string                    `json:"voted_for,omitempty"`
		PTerm         uint64                    `json:"pterm"`
		PIndex        uint64                    `json:"pindex"`
		IPQPropLen    int                       `json:"ipq_proposal_len"`
		IPQEntryLen   int                       `json:"ipq_entry_len"`
		IPQRespLen    int                       `json:"ipq_resp_len"`
		IPQApplyLen   int                       `json:"ipq_apply_len"`
		WAL           nats.StreamState          `json:"wal"`
		WALError      error                     `json:"wal_error,omitempty"`
		Peers         map[string]RaftzGroupPeer `json:"peers"`
	}

	RaftzGroupPeer struct {
		Name                string `json:"name"`
		Known               bool   `json:"known"`
		LastReplicatedIndex uint64 `json:"last_replicated_index,omitempty"`
		LastSeen            string `json:"last_seen,omitempty"`
	}

	// Account -> Raft Group -> Details
	RaftzStatus map[string]map[string]RaftzGroup

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
