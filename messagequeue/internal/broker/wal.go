package broker

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

type DiskLog struct {
	file *os.File
	path string
}

func NewDiskLog(path string) (*DiskLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &DiskLog{file: f, path: path}, nil
}

func (l *DiskLog) Append(msg Message) error {
	topicLen := uint32(len(msg.Topic))
	bodyLen := uint32(len(msg.Body))

	header := make([]byte, 16)
	binary.BigEndian.PutUint64(header[0:8], msg.Offset)
	binary.BigEndian.PutUint32(header[8:12], topicLen)
	binary.BigEndian.PutUint32(header[12:16], bodyLen)

	if _, err := l.file.Write(header); err != nil {
		return err
	}
	if _, err := l.file.Write([]byte(msg.Topic)); err != nil {
		return err
	}
	if _, err := l.file.Write(msg.Body); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *DiskLog) ReadAll() ([]Message, error) {
	// Rewind file to start for recovery reads
	if _, err := l.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var messages []Message
	header := make([]byte, 16)

	for {
		_, err := io.ReadFull(l.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

		offset := binary.BigEndian.Uint64(header[0:8])
		topicLen := binary.BigEndian.Uint32(header[8:12])
		bodyLen := binary.BigEndian.Uint32(header[12:16])

		topicBuf := make([]byte, topicLen)
		if _, err := io.ReadFull(l.file, topicBuf); err != nil {
			return nil, err
		}

		bodyBuf := make([]byte, bodyLen)
		if _, err := io.ReadFull(l.file, bodyBuf); err != nil {
			return nil, err
		}

		messages = append(messages, Message{
			Offset:    offset,
			Topic:     string(topicBuf),
			Body:      bodyBuf,
			Timestamp: time.Now(),
		})
	}
	return messages, nil
}

func (l *DiskLog) Close() error {
	return l.file.Close()
}

// Compact rebuilds the log file, removing messages with an offset lower than minRequiredOffset.
func (l *DiskLog) Compact(minRequiredOffset uint64) error {
	// 1. Read all historical records currently sitting in the file
	messages, err := l.ReadAll()
	if err != nil {
		return err
	}

	// 2. Open a temporary clean file to write the active un-acked subset
	tmpPath := l.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	// Temporarily wrap the new file descriptor inside an isolated DiskLog struct
	tmpLog := &DiskLog{file: tmpFile, path: tmpPath}

	// 3. Only append messages that haven't been fully processed by consumers yet
	compactedCount := 0
	for _, msg := range messages {
		if msg.Offset >= minRequiredOffset {
			if err := tmpLog.Append(msg); err != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
				return err
			}
		} else {
			compactedCount++
		}
	}
	tmpLog.Close()

	// 4. Safely hot-swap the files using OS level atomic renaming operations
	l.file.Close()
	if err := os.Rename(tmpPath, l.path); err != nil {
		return err
	}

	// Re-open the main log file descriptor in append mode
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	l.file = f

	if compactedCount > 0 {
		println("[Compactor] Successfully purged", compactedCount, "acknowledged records from disk log.")
	}
	return nil
}
