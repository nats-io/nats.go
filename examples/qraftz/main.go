package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type RaftzResponse struct {
	Server struct {
		Name    string    `json:"name"`
		Host    string    `json:"host"`
		ID      string    `json:"id"`
		Cluster string    `json:"cluster"`
		Ver     string    `json:"ver"`
		Time    time.Time `json:"time"`
	} `json:"server"`
	Data map[string]map[string]RaftGroup `json:"data"` // account -> group -> data
}

type RaftGroup struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	Size         int    `json:"size"`
	QuorumNeeded int    `json:"quorum_needed"`
	Committed    int64  `json:"committed"`
	Applied      int64  `json:"applied"`
	CatchingUp   bool   `json:"catching_up"`
	Leader       string `json:"leader"`
	Term         int64  `json:"term"`
	VotedFor     string `json:"voted_for"`
	IPQProposal  int    `json:"ipq_proposal_len"`
	IPQEntry     int    `json:"ipq_entry_len"`
	IPQResp      int    `json:"ipq_resp_len"`
	IPQApply     int    `json:"ipq_apply_len"`
	WAL          WAL    `json:"wal"`
}

type WAL struct {
	Messages  int64     `json:"messages"`
	Bytes     int64     `json:"bytes"`
	FirstSeq  int64     `json:"first_seq"`
	LastSeq   int64     `json:"last_seq"`
	FirstTS   time.Time `json:"first_ts"`
	LastTS    time.Time `json:"last_ts"`
}

// Flattened entry for display
type RaftEntry struct {
	Server         string
	Account        string
	Group          string
	State          string
	Term           int64
	Committed      int64
	Applied        int64
	CatchingUp     bool
	WALMsgs        int64
	WALBytes       int64
	WALFirstTS     time.Time
	WALLastTS      time.Time
	CommitRate     float64 // commits/sec (delta committed)
	ApplyRate      float64 // applies/sec (delta applied)
	MaxCommitRate  float64 // max commits/sec observed
	MaxApplyRate   float64 // max applies/sec observed
	AvgCommitRate  float64 // average commits/sec since start
	AvgApplyRate   float64 // average applies/sec since start
	ApplyDrift     float64 // difference from leader's apply rate
	WALMsgsRate    float64 // WAL msgs/sec (delta wal msgs)
	WALBytesRate   float64 // WAL bytes/sec (delta wal bytes)
}

// Rate tracker for calculating deltas
type RateTracker struct {
	prevEntries   map[string]RaftEntry // key: server|account|group
	prevTime      time.Time
	maxCommitRate map[string]float64   // max commit rate per entry
	maxApplyRate  map[string]float64   // max apply rate per entry
	startTime     map[string]time.Time // start time per entry for avg calculation
	startApplied  map[string]int64     // initial applied count per entry
	startCommitted map[string]int64    // initial committed count per entry
}

func NewRateTracker() *RateTracker {
	return &RateTracker{
		prevEntries:    make(map[string]RaftEntry),
		maxCommitRate:  make(map[string]float64),
		maxApplyRate:   make(map[string]float64),
		startTime:      make(map[string]time.Time),
		startApplied:   make(map[string]int64),
		startCommitted: make(map[string]int64),
	}
}

func (rt *RateTracker) entryKey(e RaftEntry) string {
	return e.Server + "|" + e.Account + "|" + e.Group
}

func (rt *RateTracker) UpdateRates(entries []RaftEntry) {
	now := time.Now()
	elapsed := now.Sub(rt.prevTime).Seconds()

	if elapsed > 0 && len(rt.prevEntries) > 0 {
		for i := range entries {
			key := rt.entryKey(entries[i])

			// Track start time and initial values for average calculation
			if _, exists := rt.startTime[key]; !exists {
				rt.startTime[key] = now
				rt.startApplied[key] = entries[i].Applied
				rt.startCommitted[key] = entries[i].Committed
			}

			if prev, ok := rt.prevEntries[key]; ok {
				committedDelta := entries[i].Committed - prev.Committed
				appliedDelta := entries[i].Applied - prev.Applied
				walMsgsDelta := entries[i].WALMsgs - prev.WALMsgs
				walBytesDelta := entries[i].WALBytes - prev.WALBytes

				// Calculate current rates
				if committedDelta >= 0 {
					entries[i].CommitRate = float64(committedDelta) / elapsed

					// Update max commit rate
					if entries[i].CommitRate > rt.maxCommitRate[key] {
						rt.maxCommitRate[key] = entries[i].CommitRate
					}
				}
				if appliedDelta >= 0 {
					entries[i].ApplyRate = float64(appliedDelta) / elapsed

					// Update max apply rate
					if entries[i].ApplyRate > rt.maxApplyRate[key] {
						rt.maxApplyRate[key] = entries[i].ApplyRate
					}
				}

				// Calculate WAL rates (can be negative if WAL is compacted)
				entries[i].WALMsgsRate = float64(walMsgsDelta) / elapsed
				entries[i].WALBytesRate = float64(walBytesDelta) / elapsed

				// Set max rates
				entries[i].MaxCommitRate = rt.maxCommitRate[key]
				entries[i].MaxApplyRate = rt.maxApplyRate[key]

				// Calculate average rates since start
				totalElapsed := now.Sub(rt.startTime[key]).Seconds()
				if totalElapsed > 0 {
					appliedTotal := entries[i].Applied - rt.startApplied[key]
					committedTotal := entries[i].Committed - rt.startCommitted[key]

					if appliedTotal >= 0 {
						entries[i].AvgApplyRate = float64(appliedTotal) / totalElapsed
					}
					if committedTotal >= 0 {
						entries[i].AvgCommitRate = float64(committedTotal) / totalElapsed
					}
				}
			}
		}
	}

	// Calculate apply drift relative to leader for each raft group
	calculateApplyDrift(entries)

	// Store current as previous
	rt.prevEntries = make(map[string]RaftEntry)
	for _, e := range entries {
		rt.prevEntries[rt.entryKey(e)] = e
	}
	rt.prevTime = now
}

func raftGroupKey(e RaftEntry) string {
	return e.Account + "|" + e.Group
}

func calculateApplyDrift(entries []RaftEntry) {
	// Find leader's apply rate for each raft group
	leaderRates := make(map[string]float64)
	for _, e := range entries {
		if e.State == "LEADER" {
			leaderRates[raftGroupKey(e)] = e.ApplyRate
		}
	}

	// Calculate drift for each entry
	for i := range entries {
		groupKey := raftGroupKey(entries[i])
		if leaderRate, ok := leaderRates[groupKey]; ok {
			entries[i].ApplyDrift = leaderRate - entries[i].ApplyRate
		}
	}
}

func main() {
	var server string
	var credsFile string
	var nkeyFile string
	var account string
	var raft string
	var expected int
	var timeout time.Duration
	var topView bool
	var sortBy string
	var reverse bool
	var human bool

	flag.StringVar(&server, "s", nats.DefaultURL, "NATS server URL")
	flag.StringVar(&credsFile, "creds", "", "Path to credentials file")
	flag.StringVar(&nkeyFile, "nkey", "", "Path to nkey seed file")
	flag.StringVar(&account, "account", "", "Filter by account name")
	flag.StringVar(&raft, "raft", "", "Filter by raft group name")
	flag.IntVar(&expected, "expected", 0, "Expected number of servers to wait for")
	flag.DurationVar(&timeout, "timeout", 2*time.Second, "Timeout for waiting for replies")
	flag.BoolVar(&topView, "top", true, "Display in top view (default: true)")
	flag.StringVar(&sortBy, "sort-by", "server", "Sort by: server, wal-msgs, wal-bytes, applied, committed, state")
	flag.BoolVar(&reverse, "reverse", false, "Reverse sort order")
	flag.BoolVar(&human, "human", true, "Display numbers in human-readable format (default: true)")
	flag.Parse()

	opts := []nats.Option{}
	if credsFile != "" {
		opts = append(opts, nats.UserCredentials(credsFile))
	} else if nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(nkeyFile)
		if err != nil {
			log.Fatalf("Failed to load nkey seed file: %v", err)
		}
		opts = append(opts, opt)
	}

	nc, err := nats.Connect(server, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	if topView {
		runTopView(nc, account, raft, expected, timeout, sortBy, reverse, human)
	} else {
		entries := gatherRaftz(context.Background(), nc, account, raft, expected, timeout)
		if len(entries) == 0 {
			fmt.Println("No servers responded")
			return
		}
		sortEntries(entries, sortBy, reverse)
		displayStaticView(entries, sortBy, reverse, human)
	}
}

func runTopView(nc *nats.Conn, account string, raft string, expected int, timeout time.Duration, sortBy string, reverse bool, human bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-shutdown
		cancel()
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	rateTracker := NewRateTracker()

	fmt.Print("\033[2J")

	// Do initial fetch immediately
	entries := gatherRaftz(ctx, nc, account, raft, expected, timeout)
	if len(entries) > 0 {
		rateTracker.UpdateRates(entries)
		sortEntries(entries, sortBy, reverse)
		fmt.Print("\033[H")
		displayTopView(entries, sortBy, reverse, account, raft, human)
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Print("\033[H\033[2J")
			fmt.Println("Exiting...")
			return
		case <-ticker.C:
			entries := gatherRaftz(ctx, nc, account, raft, expected, timeout)
			if ctx.Err() != nil {
				return
			}
			if len(entries) > 0 {
				rateTracker.UpdateRates(entries)
				sortEntries(entries, sortBy, reverse)
				fmt.Print("\033[H")
				displayTopView(entries, sortBy, reverse, account, raft, human)
			}
		}
	}
}

func gatherRaftz(ctx context.Context, nc *nats.Conn, account string, raft string, expected int, timeout time.Duration) []RaftEntry {
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
		return nil
	}
	defer sub.Unsubscribe()

	// Build request with optional account filter
	// Note: group filter is applied client-side after flattening
	req := map[string]interface{}{}
	if account != "" {
		req["account"] = account
	}
	reqBytes, _ := json.Marshal(req)

	err = nc.PublishRequest("$SYS.REQ.SERVER.PING.RAFTZ", inbox, reqBytes)
	if err != nil {
		log.Printf("Failed to publish request: %v", err)
		return nil
	}

	var responses []RaftzResponse
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return flattenResponses(responses, raft)
		default:
			msg, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}

			var resp RaftzResponse
			if err := json.Unmarshal(msg.Data, &resp); err != nil {
				log.Printf("Failed to unmarshal response: %v", err)
				continue
			}

			responses = append(responses, resp)

			if expected > 0 && len(responses) >= expected {
				return flattenResponses(responses, raft)
			}

			timer.Reset(timeout)
		}
	}
}

func flattenResponses(responses []RaftzResponse, raftFilter string) []RaftEntry {
	var entries []RaftEntry

	for _, resp := range responses {
		for acct, groups := range resp.Data {
			for groupName, group := range groups {
				// Filter by raft group name if specified
				if raftFilter != "" {
					// Try exact match first, then contains match
					if groupName != raftFilter && !strings.Contains(groupName, raftFilter) {
						continue
					}
				}
				entries = append(entries, RaftEntry{
					Server:     resp.Server.Name,
					Account:    acct,
					Group:      groupName,
					State:      group.State,
					Term:       group.Term,
					Committed:  group.Committed,
					Applied:    group.Applied,
					CatchingUp: group.CatchingUp,
					WALMsgs:    group.WAL.Messages,
					WALBytes:   group.WAL.Bytes,
					WALFirstTS: group.WAL.FirstTS,
					WALLastTS:  group.WAL.LastTS,
				})
			}
		}
	}

	return entries
}

func displayTopView(entries []RaftEntry, sortBy string, reverse bool, accountFilter string, raftFilter string, human bool) {
	sortOrder := "asc"
	if reverse {
		sortOrder = "desc"
	}

	// Select formatting functions based on human flag
	fmtNum := formatNumber
	fmtBytes := formatBytes
	fmtRate := formatRate
	fmtDrift := formatDrift
	fmtMsgsRate := formatMsgsRate
	fmtBytesRate := formatBytesRate
	if !human {
		fmtNum = formatNumberRaw
		fmtBytes = formatBytesRaw
		fmtRate = formatRateRaw
		fmtDrift = formatDriftRaw
		fmtMsgsRate = formatMsgsRateRaw
		fmtBytesRate = formatBytesRateRaw
	}

	title := "QRaftz - Raft WAL Monitor"
	if accountFilter != "" {
		title += fmt.Sprintf(" (Account: %s)", accountFilter)
	}
	if raftFilter != "" {
		title += fmt.Sprintf(" (Raft: %s)", raftFilter)
	}
	fmt.Printf("%s - %s | Sort: %s (%s)\n", title, time.Now().Format("15:04:05"), sortBy, sortOrder)
	fmt.Println(strings.Repeat("=", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
		"SERVER", "ACCOUNT", "GROUP", "STATE", "TERM", "APPLIED", "COMMITTED", "COMMIT/s", "MAX C/s", "AVG C/s", "APPLY/s", "MAX A/s", "AVG A/s", "APPLY DRIFT", "WAL MSGS", "MSGS/s", "WAL SIZE", "WAL/s", "WAL FIRST", "WAL LAST")
	fmt.Println(strings.Repeat("-", 253))

	var totalWALMsgs, totalWALBytes int64
	var totalCommitRate, totalApplyRate, totalWALMsgsRate, totalWALBytesRate float64
	leaders := 0

	for _, e := range entries {
		state := e.State
		if e.CatchingUp {
			state = state + "!"
		}

		walFirst := "-"
		if !e.WALFirstTS.IsZero() {
			walFirst = e.WALFirstTS.Format("15:04:05")
		}
		walLast := "-"
		if !e.WALLastTS.IsZero() {
			walLast = e.WALLastTS.Format("15:04:05")
		}

		fmt.Printf("%-15s %-8s %-12s %-10s %6d %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
			truncate(e.Server, 15),
			truncate(e.Account, 8),
			truncate(e.Group, 12),
			state,
			e.Term,
			fmtNum(e.Applied),
			fmtNum(e.Committed),
			fmtRate(e.CommitRate),
			fmtRate(e.MaxCommitRate),
			fmtRate(e.AvgCommitRate),
			fmtRate(e.ApplyRate),
			fmtRate(e.MaxApplyRate),
			fmtRate(e.AvgApplyRate),
			fmtDrift(e.ApplyDrift),
			fmtNum(e.WALMsgs),
			fmtMsgsRate(e.WALMsgsRate),
			fmtBytes(e.WALBytes),
			fmtBytesRate(e.WALBytesRate),
			walFirst,
			walLast)

		totalWALMsgs += e.WALMsgs
		totalWALBytes += e.WALBytes
		totalCommitRate += e.CommitRate
		totalApplyRate += e.ApplyRate
		totalWALMsgsRate += e.WALMsgsRate
		totalWALBytesRate += e.WALBytesRate
		if e.State == "LEADER" {
			leaders++
		}
	}

	fmt.Println(strings.Repeat("-", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s\n",
		"TOTAL", "", "", "", "",
		"", "",
		fmtRate(totalCommitRate),
		"", "", // max and avg commit rate (not shown in total)
		fmtRate(totalApplyRate),
		"", "", // max and avg apply rate (not shown in total)
		"",
		fmtNum(totalWALMsgs),
		fmtMsgsRate(totalWALMsgsRate),
		fmtBytes(totalWALBytes),
		fmtBytesRate(totalWALBytesRate))
	fmt.Println(strings.Repeat("=", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
		"SERVER", "ACCOUNT", "GROUP", "STATE", "TERM", "APPLIED", "COMMITTED", "COMMIT/s", "MAX C/s", "AVG C/s", "APPLY/s", "MAX A/s", "AVG A/s", "APPLY DRIFT", "WAL MSGS", "MSGS/s", "WAL SIZE", "WAL/s", "WAL FIRST", "WAL LAST")
	fmt.Printf("Raft Groups: %d | Leaders: %d\n", len(entries), leaders)
	fmt.Println()
}

func displayStaticView(entries []RaftEntry, sortBy string, reverse bool, human bool) {
	sortOrder := "asc"
	if reverse {
		sortOrder = "desc"
	}

	// Select formatting functions based on human flag
	fmtNum := formatNumber
	fmtBytes := formatBytes
	if !human {
		fmtNum = formatNumberRaw
		fmtBytes = formatBytesRaw
	}

	fmt.Printf("\nQRaftz - Raft WAL Monitor | Sort: %s (%s)\n", sortBy, sortOrder)
	fmt.Println(strings.Repeat("=", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
		"SERVER", "ACCOUNT", "GROUP", "STATE", "TERM", "APPLIED", "COMMITTED", "COMMIT/s", "MAX C/s", "AVG C/s", "APPLY/s", "MAX A/s", "AVG A/s", "APPLY DRIFT", "WAL MSGS", "MSGS/s", "WAL SIZE", "WAL/s", "WAL FIRST", "WAL LAST")
	fmt.Println(strings.Repeat("-", 253))

	var totalWALMsgs, totalWALBytes int64
	leaders := 0

	for _, e := range entries {
		state := e.State
		if e.CatchingUp {
			state = state + "!"
		}

		walFirst := "-"
		if !e.WALFirstTS.IsZero() {
			walFirst = e.WALFirstTS.Format("15:04:05")
		}
		walLast := "-"
		if !e.WALLastTS.IsZero() {
			walLast = e.WALLastTS.Format("15:04:05")
		}

		fmt.Printf("%-15s %-8s %-12s %-10s %6d %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
			truncate(e.Server, 15),
			truncate(e.Account, 8),
			truncate(e.Group, 12),
			state,
			e.Term,
			fmtNum(e.Applied),
			fmtNum(e.Committed),
			"-", // commit/s
			"-", // max commit/s
			"-", // avg commit/s
			"-", // apply/s
			"-", // max apply/s
			"-", // avg apply/s
			"-", // apply drift
			fmtNum(e.WALMsgs),
			"-", // msgs/s
			fmtBytes(e.WALBytes),
			"-", // wal/s
			walFirst,
			walLast)

		totalWALMsgs += e.WALMsgs
		totalWALBytes += e.WALBytes
		if e.State == "LEADER" {
			leaders++
		}
	}

	fmt.Println(strings.Repeat("-", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s\n",
		"TOTAL", "", "", "", "",
		"", "",
		"-", // commit/s
		"-", // max commit/s
		"-", // avg commit/s
		"-", // apply/s
		"-", // max apply/s
		"-", // avg apply/s
		"",  // apply drift
		fmtNum(totalWALMsgs),
		"-", // msgs/s
		fmtBytes(totalWALBytes),
		"-") // wal/s
	fmt.Println(strings.Repeat("=", 253))
	fmt.Printf("%-15s %-8s %-12s %-10s %6s %12s %12s %10s %10s %10s %10s %10s %10s %12s %10s %10s %10s %10s %-10s %-10s\n",
		"SERVER", "ACCOUNT", "GROUP", "STATE", "TERM", "APPLIED", "COMMITTED", "COMMIT/s", "MAX C/s", "AVG C/s", "APPLY/s", "MAX A/s", "AVG A/s", "APPLY DRIFT", "WAL MSGS", "MSGS/s", "WAL SIZE", "WAL/s", "WAL FIRST", "WAL LAST")
	fmt.Printf("Raft Groups: %d | Leaders: %d\n\n", len(entries), leaders)
}

func sortEntries(entries []RaftEntry, sortBy string, reverse bool) {
	sort.Slice(entries, func(i, j int) bool {
		var less bool

		switch sortBy {
		case "wal-msgs":
			less = entries[i].WALMsgs < entries[j].WALMsgs
		case "wal-bytes":
			less = entries[i].WALBytes < entries[j].WALBytes
		case "applied":
			less = entries[i].Applied < entries[j].Applied
		case "committed":
			less = entries[i].Committed < entries[j].Committed
		case "state":
			less = entries[i].State < entries[j].State
		case "account":
			less = entries[i].Account < entries[j].Account
		case "group":
			less = entries[i].Group < entries[j].Group
		default: // "server"
			if entries[i].Server == entries[j].Server {
				if entries[i].Account == entries[j].Account {
					less = entries[i].Group < entries[j].Group
				} else {
					less = entries[i].Account < entries[j].Account
				}
			} else {
				less = entries[i].Server < entries[j].Server
			}
		}

		if reverse {
			return !less
		}
		return less
	})
}

func formatBytes(bytes int64) string {
	if bytes >= 1024*1024*1024*1024 {
		return fmt.Sprintf("%.1fT", float64(bytes)/(1024*1024*1024*1024))
	} else if bytes >= 1024*1024*1024 {
		return fmt.Sprintf("%.1fG", float64(bytes)/(1024*1024*1024))
	} else if bytes >= 1024*1024 {
		return fmt.Sprintf("%.1fM", float64(bytes)/(1024*1024))
	} else if bytes >= 1024 {
		return fmt.Sprintf("%.1fK", float64(bytes)/1024)
	}
	return fmt.Sprintf("%d", bytes)
}

func formatNumber(n int64) string {
	if n >= 1000000000000 {
		return fmt.Sprintf("%.1fT", float64(n)/1000000000000)
	} else if n >= 1000000000 {
		return fmt.Sprintf("%.1fB", float64(n)/1000000000)
	} else if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	} else if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

func formatRate(rate float64) string {
	if rate >= 1000000 {
		return fmt.Sprintf("%.1fM", rate/1000000)
	} else if rate >= 1000 {
		return fmt.Sprintf("%.1fK", rate/1000)
	} else if rate >= 1 {
		return fmt.Sprintf("%.0f", rate)
	} else if rate > 0 {
		return fmt.Sprintf("%.1f", rate)
	}
	return "0"
}

func formatDrift(drift float64) string {
	if drift == 0 {
		return "0"
	}
	// Positive drift means follower is behind leader
	// Negative drift means follower is ahead (catching up faster)
	if drift >= 1000000 {
		return fmt.Sprintf("%+.1fM", drift/1000000)
	} else if drift >= 1000 || drift <= -1000 {
		return fmt.Sprintf("%+.1fK", drift/1000)
	} else if drift >= 1 || drift <= -1 {
		return fmt.Sprintf("%+.0f", drift)
	}
	return fmt.Sprintf("%+.1f", drift)
}

// Raw formatting functions (no human-readable suffixes)
func formatBytesRaw(bytes int64) string {
	return fmt.Sprintf("%d", bytes)
}

func formatNumberRaw(n int64) string {
	return fmt.Sprintf("%d", n)
}

func formatRateRaw(rate float64) string {
	if rate == 0 {
		return "0"
	}
	return fmt.Sprintf("%.2f", rate)
}

func formatDriftRaw(drift float64) string {
	if drift == 0 {
		return "0"
	}
	return fmt.Sprintf("%+.2f", drift)
}

func formatMsgsRate(rate float64) string {
	if rate == 0 {
		return "0"
	}
	absRate := rate
	sign := "+"
	if rate < 0 {
		absRate = -rate
		sign = "-"
	}
	if absRate >= 1000000 {
		return fmt.Sprintf("%s%.1fM/s", sign, absRate/1000000)
	} else if absRate >= 1000 {
		return fmt.Sprintf("%s%.1fK/s", sign, absRate/1000)
	} else if absRate >= 1 {
		return fmt.Sprintf("%s%.0f/s", sign, absRate)
	}
	return fmt.Sprintf("%s%.1f/s", sign, absRate)
}

func formatMsgsRateRaw(rate float64) string {
	if rate == 0 {
		return "0"
	}
	return fmt.Sprintf("%+.2f", rate)
}

func formatBytesRate(rate float64) string {
	if rate == 0 {
		return "0"
	}
	absRate := rate
	sign := "+"
	if rate < 0 {
		absRate = -rate
		sign = "-"
	}
	if absRate >= 1024*1024*1024 {
		return fmt.Sprintf("%s%.1fG/s", sign, absRate/(1024*1024*1024))
	} else if absRate >= 1024*1024 {
		return fmt.Sprintf("%s%.1fM/s", sign, absRate/(1024*1024))
	} else if absRate >= 1024 {
		return fmt.Sprintf("%s%.1fK/s", sign, absRate/1024)
	}
	return fmt.Sprintf("%s%.0f/s", sign, absRate)
}

func formatBytesRateRaw(rate float64) string {
	if rate == 0 {
		return "0"
	}
	return fmt.Sprintf("%+.2f", rate)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
