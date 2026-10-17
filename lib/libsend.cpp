#include <poll.h>
#include <pthread.h>
#include <sys/timerfd.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>

#include "lib.h"
#include "protocol.h"
#include "utils.h"

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int window_size;

int send_data(int conn_id, char *buffer, int len) {
  int total_size = 0, size = 0;
  while (len > 0) {
    pthread_mutex_lock(&cons[conn_id]->con_lock);

    size = min(len, MAX_DATA_SIZE);
    connection *conn = cons[conn_id];
    tcp_data_segment segment;
    memset(&segment, 0, sizeof(tcp_data_segment));
    segment.hdr.conn_id = conn_id;
    segment.hdr.len = sizeof(poli_tcp_data_hdr) + size;
    segment.hdr.protocol_id = POLI_PROTOCOL_ID;
    segment.hdr.seq_num = conn->next_seq;
    conn->next_seq++;
    memcpy(segment.data, buffer + total_size, size);

    tcp_data_segment_info segment_info;
    segment_info.segment = segment;
    segment_info.ackd = 0;

    conn->send_window_queue.push_back(segment_info);

    long unsigned index = conn->in_flight;
    while (index < conn->send_window_queue.size() &&
           conn->in_flight < conn->max_window_seq) {
      conn->in_flight++;

      send_all(conn->sockfd, &conn->send_window_queue[index].segment,
               sizeof(conn->send_window_queue[index].segment), conn->servaddr,
               sizeof(conn->servaddr));
      index++;
    }

    total_size += size;
    len -= size;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);
  }
  return total_size;
}

void *sender_handler(void *arg) {
  int res = 0;
  char buf[MAX_SEGMENT_SIZE];

  while (1) {
    if (cons.size() == 0) {
      continue;
    }
    int conn_id = -1;
    do {
      res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
    } while (res == -14);

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    if (res >= 0) {
      connection *conn = cons[conn_id];
      tcp_data_segment *segment = (tcp_data_segment *)buf;
      if (segment->hdr.type == TYPE_ACK) {
        int ackd = 0;

        // Set segment as ACKD.
        for (size_t i = 0; i < conn->send_window_queue.size(); i++) {
          if (conn->send_window_queue[i].segment.hdr.seq_num ==
              segment->hdr.seq_num) {
            conn->send_window_queue[i].ackd = 1;
            break;
          }
        }

        // Eliminate all segments from the beginning of the queue.
        while (0 < conn->window_queue.size() &&
               conn->send_window_queue[0].ackd == 1) {
          conn->send_window_queue.erase(conn->send_window_queue.begin());
          conn->in_flight--;
          ackd++;
        }

        // Send as many segments as the number of segments eliminated.
        long unsigned index = conn->in_flight;
        while (index < conn->send_window_queue.size() && ackd) {
          send_all(conn->sockfd, &conn->send_window_queue[index].segment,
                   sizeof(conn->send_window_queue[index].segment),
                   conn->servaddr, sizeof(conn->servaddr));
          ackd--;
          index++;
        }
      }
    } else {
      connection *conn = cons[conn_id];
      long unsigned index = 0;
      // Send all packets which have timeout.
      while (index < conn->send_window_queue.size() &&
             (int)index < conn->max_window_seq) {
        if (!conn->send_window_queue[index].ackd) {
          send_all(conn->sockfd, &conn->send_window_queue[index].segment,
                   sizeof(conn->send_window_queue[index].segment),
                   conn->servaddr, sizeof(conn->servaddr));
        }
        index++;
      }
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);
  }
}

int setup_connection(uint32_t ip, uint16_t port) {
  char buffer[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
  struct poli_tcp_ctrl_hdr ctrl_hdr,
      *ack_ctrl_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
  uint16_t *send_port = (uint16_t *)(buffer + sizeof(struct poli_tcp_ctrl_hdr));
  bool syn_ack_recv = false;
  struct connection *con =
      (struct connection *)malloc(sizeof(struct connection));
  struct sockaddr_in serv_addr, server_recv_addr;
  socklen_t socket_len = sizeof(struct sockaddr_in);
  struct timeval tv;

  int conn_id = 1;
  con->max_window_seq = window_size;
  con->next_seq = 1;
  con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

  memset(&serv_addr, 0, socket_len);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = port;
  serv_addr.sin_addr.s_addr = ip;
  tv.tv_sec = 0;
  tv.tv_usec = 50000;
  if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    perror("setsockopt(SO_RCVTIMEO) failed");
  }

  while (!syn_ack_recv) {
    memset(&ctrl_hdr, 0, sizeof(ctrl_hdr));
    ctrl_hdr.type = TYPE_SYN;
    ctrl_hdr.ack_num = 0;
    ctrl_hdr.recv_window = window_size;
    // Send SYN.
    send_all(con->sockfd, &ctrl_hdr, sizeof(ctrl_hdr), serv_addr,
             sizeof(serv_addr));
    memset(&ctrl_hdr, 0, sizeof(ctrl_hdr));
    // Receive SYN-ACK.
    recvfrom(con->sockfd, buffer, sizeof(buffer), 0,
             (struct sockaddr *)&server_recv_addr, &socket_len);

    if (server_recv_addr.sin_addr.s_addr == serv_addr.sin_addr.s_addr &&
        ack_ctrl_hdr->type == (TYPE_SYN | TYPE_ACK)) {
      con->max_window_seq = window_size;

      while (cons.count(conn_id)) {
        conn_id++;
      }
      server_recv_addr.sin_port = htons(*send_port);
      con->servaddr = server_recv_addr;

      memset(&ctrl_hdr, 0, sizeof(ctrl_hdr));
      ctrl_hdr.ack_num = ack_ctrl_hdr->ack_num + 1;
      ctrl_hdr.type = TYPE_ACK;
      // Send ACK.
      send_all(con->sockfd, &ctrl_hdr, sizeof(ctrl_hdr), serv_addr,
               sizeof(serv_addr));

      syn_ack_recv = true;
    }
  }

  tv.tv_sec = 0;
  tv.tv_usec = 0;
  if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    perror("setsockopt(SO_RCVTIMEO) failed");
  }

  data_fds[fdmax].fd = con->sockfd;
  data_fds[fdmax].events = POLLIN;

  timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
  timer_fds[fdmax].events = POLLIN;
  struct itimerspec spec;
  spec.it_value.tv_sec = 0;
  spec.it_value.tv_nsec = 1000;
  spec.it_interval.tv_sec = 0;
  spec.it_interval.tv_nsec = 1000;
  timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
  fdmax++;

  pthread_mutex_init(&con->con_lock, NULL);
  cons.insert({conn_id, con});

  return conn_id;
}

void init_sender(int speed, int delay) {
  pthread_t thread1;
  int ret;

  window_size = 1LL * 1e6 * speed * delay / MAX_SEGMENT_SIZE;
  ret = pthread_create(&thread1, NULL, sender_handler, NULL);
  assert(ret == 0);
}
