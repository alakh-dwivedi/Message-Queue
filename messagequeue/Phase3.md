Currently, our broker only works internally inside a single Go application. To make it a real, independent infrastructure server, we need to expose it over a network port using raw TCP. This allows completely external standalone producer and consumer scripts to connect over the network.

Step 3.1: Designing Our Wire ProtocolInstead of heavy HTTP text headers, we will use a simple, lightweight text-based protocol that is easy to parse. Clients will send messages terminated by a newline (\n).We will support three network commands:
SUB <topic> — Tell the server this connection wants to consume messages from a specific topic.
PUB <topic> <body> — Publish a message to a topic.
CLOSE — Gracefully close the connection.