package bench

import (
	"github.com/nats-io/nats"
	"strings"
	"testing"
	"time"
)

const (
	Byte    = 8
	Million = 1000 * 1000
)

var baseTime = time.Now()

func millionMessagesSecondSample(seconds int) *Sample {
	messages := Million * seconds
	start := baseTime
	end := start.Add(time.Second * time.Duration(seconds))
	nc := new(nats.Conn)

	s := NewSample(messages, Byte, start, end, nc)
	s.MsgCnt = uint64(messages)
	s.MsgBytes = uint64(messages * Byte)
	s.IOBytes = s.MsgBytes
	return s
}

func TestDuration(t *testing.T) {
	s := millionMessagesSecondSample(1)
	duration := s.End.Sub(s.Start)
	if duration != s.Duration() || duration != time.Second {
		t.Fail()
	}
}

func TestSeconds(t *testing.T) {
	s := millionMessagesSecondSample(1)
	seconds := s.End.Sub(s.Start).Seconds()
	if seconds != s.Seconds() || seconds != 1.0 {
		t.Fail()
	}
}

func TestRate(t *testing.T) {
	s := millionMessagesSecondSample(60)
	if s.Rate() != Million {
		t.Fail()
	}
}

func TestThoughput(t *testing.T) {
	s := millionMessagesSecondSample(60)
	if s.Throughput() != Million*Byte {
		t.Fail()
	}
}

func TestStrings(t *testing.T) {
	s := millionMessagesSecondSample(60)
	if len(s.String()) == 0 {
		t.Fail()
	}
}

func TestGroupDuration(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	duration := sg.End.Sub(sg.Start)
	if duration != sg.Duration() || duration != time.Duration(2)*time.Second {
		t.Fail()
	}
}

func TestGroupSeconds(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	seconds := sg.End.Sub(sg.Start).Seconds()
	if seconds != sg.Seconds() || seconds != 3.0 {
		t.Fail()
	}
}

func TestGroupRate(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	if sg.Rate() != Million*2 {
		t.Fail()
	}
}

func TestGroupThoughput(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	if sg.Throughput() != 2*Million*Byte {
		t.Fail()
	}
}

func TestMinMaxRate(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	if sg.MinRate() != sg.MaxRate() {
		t.Fail()
	}
}

func TestAvgRate(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	if sg.MinRate() != sg.AvgRate() {
		t.Fail()
	}
}

func TestStdDev(t *testing.T) {
	sg := NewSampleGroup()
	sg.AddSample(millionMessagesSecondSample(1))
	sg.AddSample(millionMessagesSecondSample(2))
	sg.AddSample(millionMessagesSecondSample(3))
	if sg.StdDev() != 0.0 {
		t.Fail()
	}
}

func TestBenchSetup(t *testing.T) {
	bench := NewBenchmark(1, 1)
	bench.AddSubSample(millionMessagesSecondSample(1))
	bench.AddPubSample(millionMessagesSecondSample(1))
	bench.Close()
	if len(bench.RunID) == 0 {
		t.Fail()
	}
	if len(bench.Pubs.Samples) != 1 || len(bench.Subs.Samples) != 1 {
		t.Fail()
	}

	if bench.MsgCnt != 2*Million || bench.IOBytes != 2*Million*8 {
		t.Fail()
	}

	if bench.Duration() != time.Second {
		t.Fail()
	}
}

func makeBench(subs, pubs int) *Benchmark {
	bench := NewBenchmark(subs, pubs)
	for i := 0; i < subs; i++ {
		bench.AddSubSample(millionMessagesSecondSample(1))
	}
	for i := 0; i < pubs; i++ {
		bench.AddPubSample(millionMessagesSecondSample(1))
	}
	bench.Close()
	return bench
}

func TestCsv(t *testing.T) {
	bench := makeBench(1, 1)
	csv := bench.CSV()
	lines := strings.Split(csv, "\n")
	// there's a header line and trailing ""
	if len(lines) != 4 {
		t.Fail()
	}

	fields := strings.Split(lines[1], ",")
	if len(fields) != 7 {
		t.Fail()
	}
}

func TestBenchStrings(t *testing.T) {
	bench := makeBench(1, 1)
	s := bench.String()
	lines := strings.Split(s, "\n")
	// header, overhead, pub header, sub headers, empty
	if len(lines) != 5 {
		t.Fail()
	}

	bench = makeBench(2, 2)
	s = bench.String()
	lines = strings.Split(s, "\n")
	// header, overhead, pub header, pub x 2, stats, sub headers, sub x 2, stats, empty
	if len(lines) != 11 {
		t.Fail()
	}
}

func TestMsgsPerClient(t *testing.T) {
	zero := MsgsPerClient(0, 0)
	if len(zero) != 0 {
		t.Fail()
	}
	onetwo := MsgsPerClient(1, 2)
	if len(onetwo) != 2 || onetwo[0] != 1 || onetwo[1] != 0 {
		t.Fail()
	}
	twotwo := MsgsPerClient(2, 2)
	if len(twotwo) != 2 || twotwo[0] != 1 || twotwo[1] != 1 {
		t.Fail()
	}
	threetwo := MsgsPerClient(3, 2)
	if len(threetwo) != 2 || threetwo[0] != 2 || threetwo[1] != 1 {
		t.Fail()
	}
}
