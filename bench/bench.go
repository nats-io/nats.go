// Copyright 2016 Apcera Inc. All rights reserved.

package bench

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/nats-io/nats"
	"github.com/nats-io/nuid"
)

// A Sample for a particular client
type Sample struct {
	JobMsgCnt int
	MsgCnt    uint64
	MsgBytes  uint64
	IOBytes   uint64
	Start     time.Time
	End       time.Time
}

// SampleGroup for a number of samples, the group is a Sample itself agregating the values the Samples
type SampleGroup struct {
	Sample
	Samples []*Sample
}

// Benchmark to hold the various Samples organized by publishers and subscribers
type Benchmark struct {
	Sample
	RunID      string
	Pubs       *SampleGroup
	Subs       *SampleGroup
	subChannel chan *Sample
	pubChannel chan *Sample
}

// NewBenchmark initializes a Benchmark. After creating a bench call AddSubSample/AddPubSample.
// When done collecting samples, call EndBenchmark
func NewBenchmark(subCnt, pubCnt int) *Benchmark {
	bm := Benchmark{RunID: nuid.Next()}
	bm.Subs = NewSampleGroup()
	bm.Pubs = NewSampleGroup()
	bm.subChannel = make(chan *Sample, subCnt)
	bm.pubChannel = make(chan *Sample, pubCnt)
	return &bm
}

// Close organizes collected Samples and calculates aggregates. After Close(), no more samples can be added.
func (bm *Benchmark) Close() {
	close(bm.subChannel)
	close(bm.pubChannel)

	for s := range bm.subChannel {
		bm.Subs.AddSample(s)
	}
	for s := range bm.pubChannel {
		bm.Pubs.AddSample(s)
	}

	bm.Start = bm.Subs.Start
	if bm.Start.After(bm.Pubs.Start) {
		bm.Start = bm.Pubs.Start
	}
	bm.End = bm.Subs.End
	if bm.End.Before(bm.Pubs.End) {
		bm.End = bm.Pubs.End
	}

	bm.MsgBytes = bm.Pubs.MsgBytes + bm.Subs.MsgBytes
	bm.IOBytes = bm.Pubs.IOBytes + bm.Subs.IOBytes
	bm.MsgCnt = bm.Pubs.MsgCnt + bm.Subs.MsgCnt
	bm.JobMsgCnt = bm.Pubs.JobMsgCnt + bm.Subs.JobMsgCnt
}

// AddSubSample to the benchmark
func (bm *Benchmark) AddSubSample(s *Sample) {
	bm.subChannel <- s
}

// AddPubSample to the benchmark
func (bm *Benchmark) AddPubSample(s *Sample) {
	bm.pubChannel <- s
}

// CSV generates a csv report of all the samples collected
func (bm *Benchmark) CSV() string {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	headers := []string{"#RunID", "ClientID", "MsgCount", "MsgBytes", "MsgsPerSec", "BytesPerSec", "DurationSecs"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error while serializing headers %q: %v", headers, err)
	}
	groups := []*SampleGroup{bm.Subs, bm.Pubs}
	pre := "S"
	for i, g := range groups {
		if i == 1 {
			pre = "P"
		}
		for j, c := range g.Samples {
			r := []string{bm.RunID, fmt.Sprintf("%s%d", pre, j), fmt.Sprintf("%d", c.MsgCnt), fmt.Sprintf("%d", c.MsgBytes), fmt.Sprintf("%d", c.Rate()), fmt.Sprintf("%f", c.Throughput()), fmt.Sprintf("%f", c.Duration().Seconds())}
			if err := writer.Write(r); err != nil {
				log.Fatalf("Error while serializing %v: %v", c, err)
			}
		}
	}

	writer.Flush()
	return buffer.String()
}

// NewSample creates a new Sample initialized to the provided values. The nats.Conn information captured
func NewSample(jobCount int, msgSize int, start, end time.Time, nc *nats.Conn) *Sample {
	s := Sample{JobMsgCnt: jobCount, Start: start, End: end}
	s.MsgBytes = uint64(msgSize * jobCount)
	s.MsgCnt = nc.OutMsgs + nc.InMsgs
	s.IOBytes = nc.OutBytes + nc.InBytes
	return &s
}

// Throughput of bytes per second
func (s *Sample) Throughput() float64 {
	return float64(s.MsgBytes) / s.Duration().Seconds()
}

// Rate of meessages in the job per second
func (s *Sample) Rate() int64 {
	return int64(float64(s.JobMsgCnt) / s.Duration().Seconds())
}

func (s *Sample) String() string {
	rate := commaFormat(s.Rate())
	jobMessages := commaFormat(int64(s.JobMsgCnt))
	protocolMessages := commaFormat(int64(s.MsgCnt - uint64(s.JobMsgCnt)))
	throughput := humanBytes(s.Throughput(), false)
	return fmt.Sprintf("%s msgs/sec | %s (%s pmsgs) msgs in %v | %s/sec", rate, jobMessages, protocolMessages, s.Duration(), throughput)
}

// Duration that the sample was active
func (s *Sample) Duration() time.Duration {
	return s.End.Sub(s.Start)
}

// Seconds that the sample or samples were active
func (s *Sample) Seconds() float64 {
	return s.Duration().Seconds()
}

// NewSampleGroup initializer
func NewSampleGroup() *SampleGroup {
	s := new(SampleGroup)
	s.Samples = make([]*Sample, 0, 0)
	return s
}

// Statistics information of the sample group (min, average, max and standard deviation)
func (sg *SampleGroup) Statistics() string {
	return fmt.Sprintf("min %s | avg %s | max %s | stddev %s msgs", commaFormat(sg.MinRate()), commaFormat(sg.AvgRate()), commaFormat(sg.MaxRate()), commaFormat(int64(sg.StdDev())))
}

// MinRate returns the smallest message rate in the SampleGroup
func (sg *SampleGroup) MinRate() int64 {
	m := int64(0)
	for i, s := range sg.Samples {
		if i == 0 {
			m = s.Rate()
		}
		m = min(m, s.Rate())
	}
	return m
}

// MaxRate returns the largest message rate in the SampleGroup
func (sg *SampleGroup) MaxRate() int64 {
	m := int64(0)
	for i, s := range sg.Samples {
		if i == 0 {
			m = s.Rate()
		}
		m = max(m, s.Rate())
	}
	return m
}

// AvgRate returns the average of all the message rates in the SampleGroup
func (sg *SampleGroup) AvgRate() int64 {
	sum := uint64(0)
	for _, s := range sg.Samples {
		sum += uint64(s.Rate())
	}
	return int64(sum / uint64(len(sg.Samples)))
}

// StdDev returns the standard deviation the message rates in the SampleGroup
func (sg *SampleGroup) StdDev() float64 {
	avg := float64(sg.AvgRate())
	sum := float64(0)
	for _, c := range sg.Samples {
		sum += math.Pow(float64(c.Rate())-avg, 2)
	}
	variance := sum / float64(len(sg.Samples))
	return math.Sqrt(variance)
}

// AddSample adds a Sample to the SampleGroup. After adding a Sample it shouldn't be modified.
func (sg *SampleGroup) AddSample(e *Sample) {
	sg.Samples = append(sg.Samples, e)

	if len(sg.Samples) == 1 {
		sg.Start = e.Start
		sg.End = e.End
	}
	sg.IOBytes += e.IOBytes
	sg.JobMsgCnt += e.JobMsgCnt
	sg.MsgCnt += e.MsgCnt
	sg.MsgBytes += e.MsgBytes

	if e.Start.Before(sg.Start) {
		sg.Start = e.Start
	}

	if e.End.After(sg.End) {
		sg.End = e.End
	}
}

// String returns a human readable report of the samples taken in the Benchmark
func (bm *Benchmark) String() string {
	var buffer bytes.Buffer
	jobCount := commaFormat(int64(bm.JobMsgCnt))
	msgRate := commaFormat(bm.Rate())
	buffer.WriteString(fmt.Sprintf("NATS (Publishers/Subscribers) throughput is %s msgs/sec (%s msgs in %v)\n", msgRate, jobCount, bm.Duration()))

	overhead := bm.IOBytes - bm.MsgBytes
	buffer.WriteString(fmt.Sprintf("Data/Overhead: %s / %s\n", humanBytes(float64(bm.MsgBytes), false), humanBytes(float64(overhead), false)))

	pubCount := len(bm.Pubs.Samples)
	if pubCount > 0 {
		buffer.WriteString(fmt.Sprintf(" Publisher Stats (%d) %v\n", pubCount, bm.Pubs))
		if pubCount > 1 {
			for i, stat := range bm.Pubs.Samples {
				buffer.WriteString(fmt.Sprintf("  [%d] %v\n", i+1, stat))
			}
			buffer.WriteString(fmt.Sprintf("  %s\n", bm.Pubs.Statistics()))
		}
	}

	subCount := len(bm.Subs.Samples)
	if subCount > 0 {
		buffer.WriteString(fmt.Sprintf(" Subscriber Stats (%d) %v\n", subCount, bm.Subs))
		if subCount > 1 {
			for i, stat := range bm.Subs.Samples {
				buffer.WriteString(fmt.Sprintf("  [%d] %v\n", i+1, stat))
			}
			buffer.WriteString(fmt.Sprintf("  %s\n", bm.Subs.Statistics()))
		}
	}
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
	pre := []string{"K", "M", "G", "T", "P", "E"}
	var post = "B"
	if si {
		base = 1000
		pre = []string{"k", "M", "G", "T", "P", "E"}
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

// MsgsPerClient divides the number of messages by the number of clients and tries to distribute them as evenly as possible
func MsgsPerClient(numMsgs, numClients int) []int {
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
