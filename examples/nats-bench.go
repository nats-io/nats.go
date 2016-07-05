// Copyright 2015 Apcera Inc. All rights reserved.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/csv"
	"encoding/json"

	"github.com/nats-io/nats"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
)

func usage() {
	log.Fatalf("Usage: nats-bench [-s server (%s)] [--tls] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] [-ms MESSAGE_SIZE] [-o jsonfile] [-csv csvfile] <subject>\n", nats.DefaultURL)
}

var subStatChan chan *stats
var pubStatChan chan *stats

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var tls = flag.Bool("tls", false, "Use TLS Secure Connection")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var csvFile = flag.String("csv", "", "Save bench data to csv file")
	var jsonFile = flag.String("o", "", "Save raw data to json file")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	// Setup the option block
	opts := nats.DefaultOptions
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}
	opts.Secure = *tls

	subStatChan = make(chan *stats, *numSubs)
	pubStatChan = make(chan *stats, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startwg.Add(*numSubs)
	subCounts := msgsPerClient(*numMsgs, *numSubs)
	for i := 0; i < *numSubs; i++ {
		go runSubscriber(&startwg, &donewg, opts, subCounts[i], *msgSize)
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	pubCounts := msgsPerClient(*numMsgs, *numPubs)
	for i := 0; i < *numPubs; i++ {
		go runPublisher(&startwg, &donewg, opts, pubCounts[i], *msgSize)
	}

	log.Printf("Starting benchmark [msgs=%d, pubs=%d, subs=%d]\n", *numMsgs, *numPubs, *numSubs)

	startwg.Wait()
	start := time.Now()
	donewg.Wait()
	end := time.Now()

	close(pubStatChan)
	pubStats := newStatSums()
	for s := range pubStatChan {
		pubStats.addStat(s)
	}

	close(subStatChan)
	subStats := newStatSums()
	for s := range subStatChan {
		subStats.addStat(s)
	}

	bench := newBenchStats(start, end, pubStats, subStats)
	fmt.Print(bench.human())

	if "" != *jsonFile {
		json := bench.json()
		ioutil.WriteFile(*jsonFile, []byte(json), 0644)
		fmt.Printf("Saved JSON data for the run in %s\n", *jsonFile)
	}

	if "" != *csvFile {
		csv := bench.csv()
		ioutil.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}
}

func runPublisher(startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int, msgSize int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()
	startwg.Done()

	args := flag.Args()
	subj := args[0]
	var msg []byte
	if msgSize > 0 {
		msg = make([]byte, msgSize)
	}

	start := time.Now()

	for i := 0; i < numMsgs; i++ {
		nc.Publish(subj, msg)
	}
	pubStatChan <- newStats(numMsgs, msgSize, start, time.Now(), nc, true)

	nc.Flush()
	donewg.Done()
}

func runSubscriber(startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int, msgSize int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	args := flag.Args()
	subj := args[0]

	received := 0
	start := time.Now()
	nc.Subscribe(subj, func(msg *nats.Msg) {
		received++
		if received >= numMsgs {
			subStatChan <- newStats(numMsgs, msgSize, start, time.Now(), nc, false)
			donewg.Done()
			nc.Close()
		}
	})
	nc.Flush()
	startwg.Done()
}

type stats struct {
	JobCount int
	MsgBytes uint64
	IOBytes  uint64
	MsgCount uint64
	Start    time.Time
	End      time.Time
}

func newStats(jobCount int, msgSize int, start, end time.Time, nc *nats.Conn, isPub bool) *stats {
	s := stats{JobCount: jobCount, Start: start, End: end}
	s.MsgBytes = uint64(msgSize * jobCount)
	if isPub {
		s.MsgCount = nc.OutMsgs
		s.IOBytes = nc.OutBytes
	} else {
		s.MsgCount = nc.InMsgs
		s.IOBytes = nc.InBytes
	}
	return &s
}

func (s *stats) throughput() float64 {
	return float64(s.MsgBytes) / s.duration().Seconds()
}

func (s *stats) rate() int64 {
	return int64(float64(s.JobCount) / s.duration().Seconds())
}

func (s *stats) String() string {
	rate := commaFormat(s.rate())
	jobMessages := commaFormat(int64(s.JobCount))
	protocolMessages := commaFormat(int64(s.MsgCount - uint64(s.JobCount)))
	throughput := humanBytes(s.throughput(), false)
	return fmt.Sprintf("%s msgs/sec | %s (%s pmsgs) msgs in %v | %s/sec", rate, jobMessages, protocolMessages, s.duration(), throughput)
}

func (s *stats) duration() time.Duration {
	return s.End.Sub(s.Start)
}

func (s *stats) Seconds() float64 {
	return s.duration().Seconds()
}

type statSums struct {
	stats
	Clients []*stats
}

func newStatSums() *statSums {
	s := new(statSums)
	s.Clients = make([]*stats, 0, 0)
	return s
}

func (s *statSums) minMaxAverage() string {
	return fmt.Sprintf("min %s | avg %s | max %s | stddev %s msgs", commaFormat(s.minRate()), commaFormat(s.avgRate()), commaFormat(s.maxRate()), commaFormat(int64(s.stddev())))
}

func (s *statSums) minRate() int64 {
	m := int64(0)
	for i, c := range s.Clients {
		if i == 0 {
			m = c.rate()
		}
		m = min(m, c.rate())
	}
	return m
}

func (s *statSums) maxRate() int64 {
	m := int64(0)
	for i, c := range s.Clients {
		if i == 0 {
			m = c.rate()
		}
		m = max(m, c.rate())
	}
	return m
}

func (s *statSums) avgRate() int64 {
	sum := uint64(0)
	for _, c := range s.Clients {
		sum += uint64(c.rate())
	}
	return int64(sum / uint64(len(s.Clients)))
}

func (s *statSums) stddev() float64 {
	avg := float64(s.avgRate())
	sum := float64(0)
	for _, c := range s.Clients {
		sum += math.Pow(float64(c.rate())-avg, 2)
	}
	variance := sum / float64(len(s.Clients))
	return math.Sqrt(variance)
}

func (s *statSums) addStat(e *stats) {
	s.Clients = append(s.Clients, e)

	if len(s.Clients) == 1 {
		s.Start = e.Start
		s.End = e.End
	}
	s.IOBytes += e.IOBytes
	s.JobCount += e.JobCount
	s.MsgCount += e.MsgCount
	s.MsgBytes += e.MsgBytes

	if e.Start.Before(s.Start) {
		s.Start = e.Start
	}

	if e.End.After(s.End) {
		s.End = e.End
	}
}

type benchStats struct {
	stats
	PubStats *statSums
	SubStats *statSums
}

func newBenchStats(start, end time.Time, pubStats, subStats *statSums) *benchStats {
	bs := benchStats{PubStats: pubStats, SubStats: subStats}
	bs.Start = start
	bs.End = end
	bs.MsgBytes = pubStats.MsgBytes + subStats.MsgBytes
	bs.IOBytes = pubStats.IOBytes + subStats.IOBytes
	bs.MsgCount = pubStats.MsgCount + subStats.MsgCount
	bs.JobCount = pubStats.JobCount + subStats.JobCount
	return &bs
}

func (b *benchStats) human() string {
	var buffer bytes.Buffer
	jobCount := commaFormat(int64(b.JobCount))
	msgThroughput := commaFormat(b.rate())
	buffer.WriteString(fmt.Sprintf("NATS (Publishers/Subscribers) throughput is %s msgs/sec (%s msgs in %v)\n", msgThroughput, jobCount, b.duration()))

	overhead := b.IOBytes - b.MsgBytes
	buffer.WriteString(fmt.Sprintf("Data/Overhead: %s / %s\n", humanBytes(float64(b.MsgBytes), false), humanBytes(float64(overhead), false)))

	pubCount := len(b.PubStats.Clients)
	if pubCount > 0 {
		buffer.WriteString(fmt.Sprintf(" Publisher Stats (%d) %v\n", pubCount, b.PubStats))
		if pubCount > 1 {
			for i, stat := range b.PubStats.Clients {
				buffer.WriteString(fmt.Sprintf("  [%d] %v\n", i+1, stat))
			}
			buffer.WriteString(fmt.Sprintf("  %s\n", b.PubStats.minMaxAverage()))
		}
	}

	subCount := len(b.SubStats.Clients)
	if subCount > 0 {
		buffer.WriteString(fmt.Sprintf(" Subscriber Stats (%d) %v\n", subCount, b.SubStats))
		if subCount > 1 {
			for i, stat := range b.SubStats.Clients {
				buffer.WriteString(fmt.Sprintf("  [%d] %v\n", i+1, stat))
			}
			buffer.WriteString(fmt.Sprintf("  %s\n", b.SubStats.minMaxAverage()))
		}
	}
	return buffer.String()
}

func (b *benchStats) json() string {
	var buffer bytes.Buffer
	bytes, _ := json.Marshal(b)
	json.Indent(&buffer, bytes, "", " ")
	return buffer.String()
}

type results struct {
	Desc         string
	MsgCnt       int
	MsgBytes     uint64
	MsgsPerSec   int64
	BytesPerSec  float64
	DurationSecs float64
}

func (b *benchStats) results() []results {
	pubs := len(b.PubStats.Clients)
	subs := len(b.SubStats.Clients)
	buf := make([]results, pubs+subs)
	var s *stats
	for i := 0; i < pubs; i++ {
		s = b.PubStats.Clients[i]
		buf[i] = results{Desc: fmt.Sprintf("P%d", i+1), MsgCnt: s.JobCount, MsgBytes: s.MsgBytes, MsgsPerSec: s.rate(), BytesPerSec: s.throughput(), DurationSecs: s.duration().Seconds()}
	}
	for i := 0; i < subs; i++ {
		s = b.SubStats.Clients[i]
		buf[pubs+i] = results{Desc: fmt.Sprintf("S%d", i+1), MsgCnt: s.JobCount, MsgBytes: s.MsgBytes, MsgsPerSec: s.rate(), BytesPerSec: s.throughput(), DurationSecs: s.duration().Seconds()}
	}
	return buf
}

func (b *benchStats) csv() string {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	writer.Write([]string{"ClientID", "MsgCount", "MsgBytes", "MsgsPerSec", "BytesPerSec", "DurationSecs"})
	for _, i := range b.results() {
		r := []string{i.Desc, fmt.Sprintf("%d", i.MsgCnt), fmt.Sprintf("%d", i.MsgBytes), fmt.Sprintf("%d", i.MsgsPerSec), fmt.Sprintf("%f", i.BytesPerSec), fmt.Sprintf("%f", i.DurationSecs)}
		err := writer.Write(r)
		if err != nil {
			log.Fatal(err)
		}
	}
	writer.Flush()
	return buffer.String()
}

func commaFormat(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}
	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

func humanBytes(bytes float64, si bool) string {
	var base = 1024
	pre := [...]string{"K", "M", "G", "T", "P", "E"}
	var post = "B"
	if si {
		base = 1000
		pre = [...]string{"k", "M", "G", "T", "P", "E"}
		post = "iB"
	}
	if bytes < float64(base) {
		return fmt.Sprintf("%.2f B", bytes)
	}
	exp := int(math.Log(bytes) / math.Log(float64(base)))
	index := exp - 1
	units := pre[index] + post
	return fmt.Sprintf("%.2f %s", bytes/math.Pow(float64(base), float64(exp)), units)
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func msgsPerClient(numMsgs, numClients int) []int {
	var counts []int
	if numClients == 0 || numMsgs == 0 {
		return counts
	}
	counts = make([]int, numClients)
	mc := numMsgs / numClients
	for i := 0; i < numClients; i++ {
		counts[i] = mc
	}
	extra := numMsgs % numClients
	for i := 0; i < extra; i++ {
		counts[i]++
	}
	return counts
}
