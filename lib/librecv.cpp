#include <poll.h>
#include <pthread.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

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
int sockfd;

int recv_data(int conn_id, char *buffer, int len) {
  int size = 0;

  pthread_mutex_lock(&cons[conn_id]->con_lock);

  connection *conn = cons[conn_id];
  long unsigned index = 0;
  tcp_data_segment *segment;
  int data_len;
  long unsigned win_index = 0;
  while (win_index < conn->window_queue.size() &&
         conn->window_queue.size() != 0 &&
         conn->next_seq > conn->window_queue[win_index].hdr.seq_num) {
    win_index++;
  }

  if (win_index == conn->window_queue.size()) {
    // If packets are still to be received.
    pthread_mutex_unlock(&cons[conn_id]->con_lock);
    return size;
  }

  while (win_index < conn->window_queue.size() && len > 0) {
    if (conn->window_queue[win_index].hdr.seq_num != conn->next_seq) {
      pthread_mutex_unlock(&cons[conn_id]->con_lock);
      return size;
    }

    segment = &conn->window_queue[win_index];
    data_len = segment->hdr.len - sizeof(poli_tcp_data_hdr);
    if (len - data_len < 0) {
      // Copy a part of the data of the segment.
      memcpy(buffer + index, segment->data, len);
      memcpy(segment->data, segment->data + len, data_len - len);
      segment->hdr.len = data_len - len;
      size += len;
      len = 0;
      break;
    } else {
      // Copy the whole data of the segment.
      memcpy(buffer + index, segment->data, data_len);
      index += data_len;
      size += data_len;
      len -= data_len;
      conn->next_seq = segment->hdr.seq_num + 1;
      conn->window_queue.erase(conn->window_queue.begin() + win_index);
    }
  }

  pthread_mutex_unlock(&cons[conn_id]->con_lock);

  return size;
}

void *receiver_handler(void *arg) {
  char buf[MAX_SEGMENT_SIZE];
  int res;

  while (1) {
    int conn_id = -1;
    do {
      res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
    } while (res == -14);

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    if (res >= 0) {
      connection *conn = cons[conn_id];
      tcp_data_segment *segment = (tcp_data_segment *)buf;

      long unsigned index = 0;
      // Add segment to the queue in order to remain in increasing order.
      while (index < conn->window_queue.size() &&
             conn->window_queue[index].hdr.seq_num < segment->hdr.seq_num) {
        index++;
      }

      if (index == conn->window_queue.size()) {
        conn->window_queue.push_back(*segment);
      } else {
        if (conn->window_queue[index].hdr.seq_num == segment->hdr.seq_num) {
          conn->window_queue.erase(conn->window_queue.begin() + index);
        }
        conn->window_queue.insert(conn->window_queue.begin() + index, *segment);
      }

      segment->hdr.len = sizeof(poli_tcp_data_hdr);
      segment->hdr.conn_id = conn_id;
      segment->hdr.type = TYPE_ACK;
      send_all(conn->sockfd, segment, sizeof(segment), conn->servaddr,
               sizeof(conn->servaddr));
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);
  }
}

int is_connected(uint32_t ip, uint16_t port) {
  for (int i = 0; i < 256; i++) {
    if (cons.count(i)) {
      connection *conn = cons[i];

      if (conn->servaddr.sin_addr.s_addr == ip &&
          conn->servaddr.sin_port == port) {
        return 1;
      }
    }
  }

  return 0;
}

int wait4connect(uint32_t ip, uint16_t port) {
  int rc;
  struct sockaddr_in serv_addr, cli_addr, syn_ack_cli_addr;
  socklen_t socket_len = sizeof(struct sockaddr_in);
  bool connected = false;
  uint32_t cli_ip;
  uint16_t cli_port;

  char buffer[sizeof(struct poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
  struct poli_tcp_ctrl_hdr ctrl_hdr,
      *ack_ctrl_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
  uint16_t *send_port = (uint16_t *)(buffer + sizeof(struct poli_tcp_ctrl_hdr));

  struct connection *con =
      (struct connection *)malloc(sizeof(struct connection));
  int conn_id = 1;

  con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  con->in_flight = 0;
  con->next_seq = 1;
  memset(&serv_addr, 0, socket_len);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = 0;
  serv_addr.sin_addr.s_addr = ip;
  rc = bind(con->sockfd, (struct sockaddr *)&serv_addr, socket_len);
  assert(rc == 0);

  getsockname(con->sockfd, (struct sockaddr *)&serv_addr, &socket_len);
  *send_port = ntohs(serv_addr.sin_port);

  struct timeval tv;

  const int enable = 1;
  if (setsockopt(con->sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) <
      0)
    perror("setsockopt(SO_REUSEADDR) failed");

  bool syn_recv = false, ack_recv = false;
  int timeouts = 20;
  while (!connected) {
    while (!syn_recv) {
      memset(&ctrl_hdr, 0, sizeof(ctrl_hdr));
      // Receive SYN.
      recvfrom(sockfd, &ctrl_hdr, sizeof(ctrl_hdr), 0,
               (struct sockaddr *)&cli_addr, &socket_len);

      cli_port = cli_addr.sin_port;
      cli_ip = cli_addr.sin_addr.s_addr;
      if (is_connected(cli_ip, cli_port)) {
        continue;
      }

      if (ctrl_hdr.type == TYPE_SYN) {
        syn_recv = true;

        while (cons.count(conn_id)) {
          conn_id++;
        }
        con->servaddr = cli_addr;
      }
    }

    ack_ctrl_hdr->type = TYPE_ACK | TYPE_SYN;
    ack_ctrl_hdr->ack_num = ctrl_hdr.ack_num + 1;
    ack_ctrl_hdr->protocol_id = ctrl_hdr.protocol_id;

    tv.tv_sec = 0;
    tv.tv_usec = 5000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
      perror("setsockopt(SO_RCVTIMEO) failed");
    }

    while (!ack_recv) {
      memset(&ctrl_hdr, 0, sizeof(ctrl_hdr));
      // Send SYN-ACK.
      send_all(sockfd, buffer, sizeof(buffer), cli_addr, socket_len);
      // Receive ACK.
      recvfrom(sockfd, &ctrl_hdr, sizeof(ctrl_hdr), 0,
               (struct sockaddr *)&syn_ack_cli_addr, &socket_len);

      if (syn_ack_cli_addr.sin_addr.s_addr == cli_ip &&
          cli_port == syn_ack_cli_addr.sin_port) {
        if (ctrl_hdr.type == TYPE_ACK &&
            ctrl_hdr.ack_num == ack_ctrl_hdr->ack_num + 1) {
          ack_recv = true;
          connected = true;
        }
      }

      timeouts--;
      if (!timeouts) {
        ack_recv = true;
        connected = true;
      }
    }

    tv.tv_sec = 0;
    tv.tv_usec = 0;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
      perror("setsockopt(SO_RCVTIMEO) failed");
    }
  }

  pthread_mutex_init(&con->con_lock, NULL);
  pthread_mutex_lock(&con->con_lock);
  data_fds[fdmax].fd = con->sockfd;
  data_fds[fdmax].events = POLLIN;

  timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
  timer_fds[fdmax].events = POLLIN;
  struct itimerspec spec;
  spec.it_value.tv_sec = 0;
  spec.it_value.tv_nsec = 500;

  spec.it_interval.tv_sec = 0;
  spec.it_interval.tv_nsec = 500;
  timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);
  fdmax++;

  cons.insert({conn_id, con});
  pthread_mutex_unlock(&con->con_lock);
  return conn_id;
}

void init_receiver(int recv_buffer_bytes) {
  pthread_t thread1;
  struct sockaddr_in serv_addr;
  socklen_t socket_len = sizeof(struct sockaddr_in);
  int ret;

  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  memset(&serv_addr, 0, socket_len);
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(8032);
  serv_addr.sin_addr.s_addr = INADDR_ANY;

  ret = bind(sockfd, (struct sockaddr *)&serv_addr, socket_len);
  assert(ret == 0);

  const int enable = 1;
  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    perror("setsockopt(SO_REUSEADDR) failed");

  ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
  assert(ret == 0);
}
