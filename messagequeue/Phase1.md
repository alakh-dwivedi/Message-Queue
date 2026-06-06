First, we need to establish what a Message looks like and lay out our Broker structure.

sync.RWMutex: We use a Read-Write mutex. This allows hundreds of goroutines to read data or publish simultaneously without blocking each other. The lock only becomes exclusive when a new consumer modifies the map structure by subscribing or unsubscribing.

Slices of Channels: The map maps a string (topic) to a slice of channels ([]chan Message). This means one topic can broadcast to multiple unique subscribers simultaneously.

Now, let's allow consumers to sign up for a topic. We will also return an elegant, self-cleaning unsubscribe function to prevent memory leaks when a worker shuts down.
Buffered Channels: The bufferSize variable determines how many messages a consumer can accumulate before falling behind. This acts as the buffer pool.
Idiomatic Cleanups: Returning a closure (func()) for unsubscribing lets the consumer manage its own lifecycle cleanly without having to pass internal channel pointers back into the broker manually.


Publisher:
Non-blocking Channel Sends: A standard channel write (ch <- msg) pauses the entire goroutine if the channel is full. By wrapping it in a select statement with a default path, Go will attempt to write. If it cannot do so instantly, it immediately falls through to the default case. This guarantees your core broker engine never hangs due to one broken client.