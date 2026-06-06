package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Client manages a persistent TCP connection to our message queue server.
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

// NewClient establishes a network connection to the target message queue server address.
func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

// Publish sends a payload string to a specific topic and returns the assigned tracking offset.
func (c *Client) Publish(topic string, body string) (string, error) {
	// Format the outgoing wire frame protocol message cleanly with a newline
	_, err := fmt.Fprintf(c.conn, "PUB %s %s\n", topic, body)
	if err != nil {
		return "", fmt.Errorf("failed to send publish command: %w", err)
	}

	// Wait for the synchronous execution confirmation response from the server socket
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read publish response: %w", err)
	}

	resp = strings.TrimSpace(resp)
	if strings.HasPrefix(resp, "ERR:") {
		return "", fmt.Errorf("server returned error: %s", resp)
	}

	return resp, nil
}

// Subscribe listens for incoming stream messages on a topic.
// It returns a read-only data channel and an error if initialization fails.
func (c *Client) Subscribe(topic string, consumerName string) (<-chan string, error) {
	_, err := fmt.Fprintf(c.conn, "SUB %s %s\n", topic, consumerName)
	if err != nil {
		return nil, fmt.Errorf("failed to send subscribe command: %w", err)
	}

	// Read the initial ACK confirmation line from the server
	ack, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read subscription acknowledgement: %w", err)
	}

	if strings.HasPrefix(ack, "ERR:") {
		return nil, fmt.Errorf("server subscription rejected: %s", strings.TrimSpace(ack))
	}

	msgChan := make(chan string, 100)

	// Spawn a background listener worker routine to read the continuous incoming network message stream
	go func() {
		defer close(msgChan)
		for {
			line, err := c.reader.ReadString('\n')
			if err != nil {
				// Connection broken or closed gracefully by user script
				return
			}
			msgChan <- strings.TrimSpace(line)
		}
	}()

	return msgChan, nil
}

// Acknowledge sends a processing checkpoint offset confirmation marker to the queue server.
func (c *Client) Acknowledge(consumerName string, offset uint64) error {
	_, err := fmt.Fprintf(c.conn, "ACK %s %d\n", consumerName, offset)
	if err != nil {
		return fmt.Errorf("failed to transmit checkpoint message: %w", err)
	}

	// Read acknowledgement status response line
	_, _ = c.reader.ReadString('\n')
	return nil
}

// Close gracefully terminates the underlying client network connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
