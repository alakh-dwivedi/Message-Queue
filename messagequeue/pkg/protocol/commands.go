package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// CommandType represents our custom message queue operational codes.
type CommandType string

const (
	CmdPub CommandType = "PUB"
	CmdSub CommandType = "SUB"
	CmdAck CommandType = "ACK"
)

// Command represents a fully parsed, type-safe request payload.
type Command struct {
	Type         CommandType
	Topic        string
	Body         string
	ConsumerName string
	Offset       uint64
}

// ParseRequest takes a raw string from the network socket and translates it into a structured Command.
func ParseRequest(line string) (*Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, errors.New("empty payload stream")
	}

	// Split by space. We do NOT restrict to 3 parts here so we can parse everything safely.
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, errors.New("invalid parameter syntax")
	}

	cmdType := CommandType(strings.ToUpper(parts[0]))

	switch cmdType {
	case CmdPub:
		// Format: PUB <topic> <body>
		if len(parts) < 3 {
			return nil, errors.New("invalid format: must be 'PUB <topic> <body>'")
		}
		// Recombine anything after the topic as the body text
		body := strings.Join(parts[2:], " ")
		return &Command{Type: CmdPub, Topic: parts[1], Body: body}, nil

	case CmdSub:
		// Format: SUB <topic> <consumer_name>
		if len(parts) < 3 {
			return nil, errors.New("invalid format: must be 'SUB <topic> <consumer_name>'")
		}
		return &Command{Type: CmdSub, Topic: parts[1], ConsumerName: parts[2]}, nil

	case CmdAck:
		// Format: ACK <consumer_name> <offset>
		if len(parts) < 3 {
			return nil, errors.New("invalid format: must be 'ACK <consumer_name> <offset>'")
		}
		var offset uint64
		_, err := fmt.Sscanf(parts[2], "%d", &offset)
		if err != nil {
			return nil, errors.New("invalid offset format: must be an integer index")
		}
		return &Command{Type: CmdAck, ConsumerName: parts[1], Offset: offset}, nil

	default:
		return nil, fmt.Errorf("unknown operation token: %s", parts[0])
	}
}
