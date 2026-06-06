Phase 2: The Persistence Layer (Write-Ahead Log).

In a real message queue, you cannot rely purely on RAM. If your server loses power or crashes, your data vanishes. To prevent this, we write messages sequentially to an append-only file on disk called a Write-Ahead Log (WAL).


Step 2.1: Designing the Binary Storage formatWe shouldn't just dump plain text into a file. If we do, the computer won't know where one message ends and the next begins when it reads them back on startup.We will design a simple, robust binary frame format:Bytes 0–3 (4 bytes): A Big-Endian encoded integer stating the length of the topic string.Bytes 4–7 (4 bytes): A Big-Endian encoded integer stating the length of the body data.Next bytes: The actual raw string characters for the Topic.Remaining bytes: The actual raw payload body.