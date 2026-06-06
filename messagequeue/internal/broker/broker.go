package broker

import (
	"sync"
	"time"
)

type Message struct {
	Offset    uint64
	Topic     string
	Body      []byte
	Timestamp time.Time
}

type TopicState struct {
	mu          sync.RWMutex
	Messages    []Message
	NextOffset  uint64
	Subscribers []chan Message
}

type Broker struct {
	mu      sync.RWMutex
	Topics  map[string]*TopicState
	Offsets map[string]uint64
}

func NewBroker() *Broker {
	return &Broker{
		Topics:  make(map[string]*TopicState),
		Offsets: make(map[string]uint64),
	}
}

func (b *Broker) GetOrCreateTopic(topic string) *TopicState {
	b.mu.Lock()
	defer b.mu.Unlock()

	ts, exists := b.Topics[topic]
	if !exists {
		ts = &TopicState{
			Messages: make([]Message, 0),
		}
		b.Topics[topic] = ts
	}
	return ts
}

// SubscribeDurable registers a consumer by name and catches them up on missed messages.
func (b *Broker) SubscribeDurable(topic string, consumerName string, bufferSize int) (<-chan Message, func(), error) {
	ts := b.GetOrCreateTopic(topic)
	ts.mu.Lock()

	// Create client delivery pipeline
	ch := make(chan Message, bufferSize)
	ts.Subscribers = append(ts.Subscribers, ch)

	// Fetch consumer offset marker progress
	b.mu.RLock()
	lastAcked := b.Offsets[consumerName]
	b.mu.RUnlock()

	// REPLAY PHASE: Catch up client on any missed historical items
	for _, msg := range ts.Messages {
		if msg.Offset > lastAcked {
			select {
			case ch <- msg:
			default:
				// Avoid stalling the initialization phase if buffers overflow immediately
			}
		}
	}
	ts.mu.Unlock()

	unsubscribe := func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		for i, sub := range ts.Subscribers {
			if sub == ch {
				ts.Subscribers = append(ts.Subscribers[:i], ts.Subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsubscribe, nil
}

// PublishAssign automatically increments the offset index during message delivery.
func (b *Broker) PublishAssign(topic string, body []byte) (Message, error) {
	ts := b.GetOrCreateTopic(topic)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.NextOffset++
	msg := Message{
		Offset:    ts.NextOffset,
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now(),
	}

	// Persist to historical buffer for replay tracking
	ts.Messages = append(ts.Messages, msg)

	// Stream out live broadcasts
	for _, ch := range ts.Subscribers {
		select {
		case ch <- msg:
		default:
			// Backpressure triggered
		}
	}

	return msg, nil
}

// Acknowledge updates a specific consumer's reading offset watermark.
func (b *Broker) Acknowledge(consumerName string, offset uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Only move offsets forward to ensure processing safety patterns
	if offset > b.Offsets[consumerName] {
		b.Offsets[consumerName] = offset
	}
}

// GetMinimumAckedOffset returns the lowest offset currently acknowledged across all consumers.
func (b *Broker) GetMinimumAckedOffset() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.Offsets) == 0 {
		return 0 // No consumers have registered an ACK yet; preserve all history
	}

	var lowest uint64 = ^uint64(0) // Initialize to max uint64 value
	for _, offset := range b.Offsets {
		if offset < lowest {
			lowest = offset
		}
	}
	return lowest
}
