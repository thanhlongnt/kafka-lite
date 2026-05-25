package log_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/thanhlongnt/kafka-lite/internal/log"
	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
)

// logFactory returns a fresh Log instance for each sub-test.
type logFactory func(t *testing.T) log.Log

func testAppendRead(t *testing.T, factory logFactory) {
	t.Helper()
	l := factory(t)

	msgs := []*pb.Message{
		{Topic: "t", Partition: 0, Value: []byte("a")},
		{Topic: "t", Partition: 0, Value: []byte("b")},
		{Topic: "t", Partition: 0, Value: []byte("c")},
	}
	for i, m := range msgs {
		off, err := l.Append(m)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("Append[%d]: expected offset %d, got %d", i, i, off)
		}
	}

	got, err := l.Read(0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(got))
	}
	for i, m := range got {
		if string(m.Value) != string(msgs[i].Value) {
			t.Errorf("msg[%d]: expected %q, got %q", i, msgs[i].Value, m.Value)
		}
		if m.Offset != int64(i) {
			t.Errorf("msg[%d]: expected offset %d, got %d", i, i, m.Offset)
		}
	}
}

func testPartialRead(t *testing.T, factory logFactory) {
	t.Helper()
	l := factory(t)

	for i := 0; i < 5; i++ {
		if _, err := l.Append(&pb.Message{Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := l.Read(2, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Offset != 2 || got[1].Offset != 3 {
		t.Errorf("unexpected offsets: %d, %d", got[0].Offset, got[1].Offset)
	}
}

func testOutOfRangeRead(t *testing.T, factory logFactory) {
	t.Helper()
	l := factory(t)
	l.Append(&pb.Message{Value: []byte("x")}) //nolint:errcheck

	if _, err := l.Read(-1, 1); err == nil {
		t.Error("expected error for negative offset")
	}
	if _, err := l.Read(999, 1); err == nil {
		t.Error("expected error for offset beyond end")
	}
}

func testLen(t *testing.T, factory logFactory) {
	t.Helper()
	l := factory(t)
	if l.Len() != 0 {
		t.Fatalf("expected 0, got %d", l.Len())
	}
	l.Append(&pb.Message{Value: []byte("a")}) //nolint:errcheck
	l.Append(&pb.Message{Value: []byte("b")}) //nolint:errcheck
	if l.Len() != 2 {
		t.Fatalf("expected 2, got %d", l.Len())
	}
}

func testConcurrentAppendRead(t *testing.T, factory logFactory) {
	t.Helper()
	l := factory(t)
	const n = 100

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := l.Append(&pb.Message{Value: []byte(fmt.Sprintf("%d", i))}); err != nil {
				t.Errorf("Append: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if l.Len() != n {
		t.Fatalf("expected %d messages, got %d", n, l.Len())
	}
	got, err := l.Read(0, n)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d messages from Read, got %d", n, len(got))
	}
	// Verify offsets are a permutation of [0, n).
	seen := make(map[int64]bool, n)
	for _, m := range got {
		seen[m.Offset] = true
	}
	for i := 0; i < n; i++ {
		if !seen[int64(i)] {
			t.Errorf("missing offset %d", i)
		}
	}
}

// MemLog tests

func memFactory(t *testing.T) log.Log {
	return &log.MemLog{}
}

func TestMemLog_AppendRead(t *testing.T)            { testAppendRead(t, memFactory) }
func TestMemLog_PartialRead(t *testing.T)            { testPartialRead(t, memFactory) }
func TestMemLog_OutOfRangeRead(t *testing.T)         { testOutOfRangeRead(t, memFactory) }
func TestMemLog_Len(t *testing.T)                    { testLen(t, memFactory) }
func TestMemLog_ConcurrentAppendRead(t *testing.T)   { testConcurrentAppendRead(t, memFactory) }

// FileLog tests

func fileFactory(t *testing.T) log.Log {
	fl, err := log.NewFileLog(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	t.Cleanup(func() { fl.Close() })
	return fl
}

func TestFileLog_AppendRead(t *testing.T)            { testAppendRead(t, fileFactory) }
func TestFileLog_PartialRead(t *testing.T)            { testPartialRead(t, fileFactory) }
func TestFileLog_OutOfRangeRead(t *testing.T)         { testOutOfRangeRead(t, fileFactory) }
func TestFileLog_Len(t *testing.T)                    { testLen(t, fileFactory) }
func TestFileLog_ConcurrentAppendRead(t *testing.T)   { testConcurrentAppendRead(t, fileFactory) }

// TestFileLog_PersistAcrossReopen verifies messages survive close and reopen.
func TestFileLog_PersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	fl1, err := log.NewFileLog(dir)
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	want := []string{"hello", "world", "kafka"}
	for _, v := range want {
		if _, err := fl1.Append(&pb.Message{Value: []byte(v)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := fl1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fl2, err := log.NewFileLog(dir)
	if err != nil {
		t.Fatalf("NewFileLog (reopen): %v", err)
	}
	defer fl2.Close()

	if fl2.Len() != int64(len(want)) {
		t.Fatalf("expected %d messages after reopen, got %d", len(want), fl2.Len())
	}
	got, err := fl2.Read(0, len(want))
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	for i, m := range got {
		if string(m.Value) != want[i] {
			t.Errorf("msg[%d]: expected %q, got %q", i, want[i], m.Value)
		}
	}
}

// TestFileLog_TruncatedWriteRecovery verifies that a partial trailing record is
// silently discarded and previously complete records are readable.
func TestFileLog_TruncatedWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	segPath := dir + "/segment.log"

	fl, err := log.NewFileLog(dir)
	if err != nil {
		t.Fatalf("NewFileLog: %v", err)
	}
	fl.Append(&pb.Message{Value: []byte("good1")}) //nolint:errcheck
	fl.Append(&pb.Message{Value: []byte("good2")}) //nolint:errcheck
	fl.Close()

	// Append a 4-byte length header with no body to simulate a truncated write.
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	f.Write([]byte{0x10, 0x00, 0x00, 0x00}) // declares 16-byte body, writes none
	f.Close()

	fl2, err := log.NewFileLog(dir)
	if err != nil {
		t.Fatalf("NewFileLog after truncation: %v", err)
	}
	defer fl2.Close()

	if fl2.Len() != 2 {
		t.Fatalf("expected 2 intact messages, got %d", fl2.Len())
	}
	got, err := fl2.Read(0, 2)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got[0].Value) != "good1" || string(got[1].Value) != "good2" {
		t.Errorf("unexpected values: %q, %q", got[0].Value, got[1].Value)
	}
}
