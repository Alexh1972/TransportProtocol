## Reliable Data Transfer with Selective Repeat

This code implements a reliable data transfer protocol using a selective repeat algorithm, ensuring in-order and error-free 
delivery of data. The implementation includes the three-way handshake for establishing a connection and utilizes a 
window size calculated based on network bandwidth, delay, and segment size to optimize transmission.

**Key Features:**

* **Selective Repeat:** This protocol allows the sender to retransmit only the packets that are lost. Received packets are 
buffered and reordered at the receiver.
* **Three-Way Handshake:** A standard three-way handshake mechanism (SYN, SYN-ACK, ACK) is implemented in the `wait4connect` and 
`setup_connection` functions to establish a reliable connection between the sender and receiver before data transmission begins.
* **Sending Data (`send_data`):** The `send_data` function segments the outgoing data into packets, adds a header containing 
sequence numbers, and adds these segments to the send window queue. It then transmits segments within the current window size.
* **Receiving Data (`recv_data`):** The `recv_data` function retrieves ordered data from the receiver's window queue. It waits 
for segments with the expected sequence number, copies the data to the provided buffer, and updates the expected next sequence 
number.
* **Receiver Handler (`receiver_handler`):** This thread continuously listens for incoming data segments. It adds received 
segments to a window queue, reorders them based on sequence numbers, and sends back acknowledgements (ACKs) to the sender. 
Duplicate packets are detected and discarded.
* **Sender Handler (`sender_handler`):** This thread listens for incoming ACKs. Upon receiving an ACK, it marks the corresponding 
segment in the send window as acknowledged. It also handles retransmissions for segments that have timed out, ensuring reliable 
delivery.
