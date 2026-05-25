package log

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/thanhlongnt/kafka-lite/internal/proto/kafka_lite"
)

// messageRecord is the JSON-serializable form of a message stored on disk.
type messageRecord struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
	Offset    int64  `json:"offset"`
}

// FileLog is a file-backed implementation of Log.
//
// Segment file format: sequential records, each encoded as:
//
//	[4-byte little-endian uint32 length][length bytes of JSON-encoded messageRecord]
//
// An in-memory index maps log offset → byte position in the segment file.
// The index is rebuilt by scanning the segment on startup, so the log is
// crash-recoverable: any partial trailing record is silently discarded.
type FileLog struct {
	mu    sync.RWMutex
	file  *os.File
	index []int64 // index[i] = byte position of the i-th message in file
}

// NewFileLog opens or creates the segment file inside dataDir.
// If an existing segment is found its index is rebuilt by scanning.
func NewFileLog(dataDir string) (*FileLog, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	path := filepath.Join(dataDir, "segment.log")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	fl := &FileLog{file: file}
	if err := fl.rebuildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("rebuild index: %w", err)
	}
	return fl, nil
}

// rebuildIndex scans the segment from the beginning and populates fl.index.
// A truncated trailing record is silently dropped (crash recovery), and the
// file is truncated to the last clean record boundary so future appends are
// not corrupted by leftover orphan bytes.
func (fl *FileLog) rebuildIndex() error {
	if _, err := fl.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	fl.index = fl.index[:0]
	var pos int64
	for {
		var length uint32
		if err := binary.Read(fl.file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// Try reading the full record body to detect truncation.
		body := make([]byte, length)
		if _, err := io.ReadFull(fl.file, body); err != nil {
			// Truncated record — stop here without adding to index.
			break
		}
		fl.index = append(fl.index, pos)
		pos += 4 + int64(length)
	}
	// Remove any orphaned bytes from a partial trailing write so that
	// subsequent appends begin at a clean record boundary.
	return fl.file.Truncate(pos)
}

// Append writes msg to the segment file, assigns its offset, and updates the index.
func (fl *FileLog) Append(msg *pb.Message) (int64, error) {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	offset := int64(len(fl.index))
	msg.Offset = offset

	data, err := json.Marshal(messageRecord{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Key:       msg.Key,
		Value:     msg.Value,
		Offset:    offset,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	// Seek to end to obtain the write position.
	pos, err := fl.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek end: %w", err)
	}

	if err := binary.Write(fl.file, binary.LittleEndian, uint32(len(data))); err != nil {
		return 0, fmt.Errorf("write length: %w", err)
	}
	if _, err := fl.file.Write(data); err != nil {
		return 0, fmt.Errorf("write data: %w", err)
	}

	fl.index = append(fl.index, pos)
	return offset, nil
}

// Read returns up to maxMsgs messages starting at offset.
func (fl *FileLog) Read(offset int64, maxMsgs int) ([]*pb.Message, error) {
	fl.mu.RLock()
	defer fl.mu.RUnlock()

	n := int64(len(fl.index))
	if offset < 0 || offset > n {
		return nil, fmt.Errorf("offset %d out of range [0, %d]", offset, n)
	}
	end := offset + int64(maxMsgs)
	if end > n {
		end = n
	}

	result := make([]*pb.Message, 0, end-offset)
	for i := offset; i < end; i++ {
		msg, err := fl.readAt(fl.index[i])
		if err != nil {
			return nil, fmt.Errorf("read message at offset %d: %w", i, err)
		}
		result = append(result, msg)
	}
	return result, nil
}

// readAt reads and decodes the message stored at byte position pos.
// Uses ReadAt so it is safe to call concurrently with other ReadAt calls.
func (fl *FileLog) readAt(pos int64) (*pb.Message, error) {
	var lenBuf [4]byte
	if _, err := fl.file.ReadAt(lenBuf[:], pos); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])

	data := make([]byte, length)
	if _, err := fl.file.ReadAt(data, pos+4); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var rec messageRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &pb.Message{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Key:       rec.Key,
		Value:     rec.Value,
		Offset:    rec.Offset,
	}, nil
}

// Len returns the number of messages in the log.
func (fl *FileLog) Len() int64 {
	fl.mu.RLock()
	defer fl.mu.RUnlock()
	return int64(len(fl.index))
}

// Close closes the underlying segment file.
func (fl *FileLog) Close() error {
	return fl.file.Close()
}
