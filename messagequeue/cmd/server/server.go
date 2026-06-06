package main

import (
	"bufio"
	"fmt"
	"messagequeue/internal/broker"
	"messagequeue/pkg/protocol"
	"net"
	// Cleaned up protocol package path
)

// Server coordinates the network port, memory broker, and disk logging.
type Server struct {
	broker     *broker.Broker
	diskLog    *broker.DiskLog
	listenAddr string
}

// HandleClient manages the lifecycle of a single network connection.
func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		cmd, err := protocol.ParseRequest(line)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("ERR: %v\n", err)))
			continue
		}

		switch cmd.Type {
		case protocol.CmdPub:
			msg, err := s.broker.PublishAssign(cmd.Topic, []byte(cmd.Body))
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR: %v\n", err)))
				continue
			}
			_ = s.diskLog.Append(msg)
			conn.Write([]byte(fmt.Sprintf("ACK: Offset %d\n", msg.Offset)))

		case protocol.CmdSub:
			msgChan, unsubscribe, err := s.broker.SubscribeDurable(cmd.Topic, cmd.ConsumerName, 100)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR: %v\n", err)))
				return
			}

			// Send ACK acknowledgement line to the Client SDK
			_, err = conn.Write([]byte("ACK: Streaming started\n"))
			if err != nil {
				unsubscribe()
				return
			}

			for msg := range msgChan {
				_, err := conn.Write([]byte(fmt.Sprintf("MSG %d %s %s\n", msg.Offset, msg.Topic, string(msg.Body))))
				if err != nil {
					unsubscribe()
					return
				}
			}
			return

		case protocol.CmdAck:
			s.broker.Acknowledge(cmd.ConsumerName, cmd.Offset)
			conn.Write([]byte("ACK: Processed checkpoint saved\n"))
		}
	}
}
