#include <assert.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include "lib.h"
using namespace std;

/* We use extern to use the variables defined either in
   librecv or libsend */
extern std::map<int, struct connection *> cons;
extern struct pollfd data_fds[MAX_CONNECTIONS];
extern struct pollfd timer_fds[MAX_CONNECTIONS];
extern int fdmax;

int recv_all(int sockfd, void *buffer, size_t len) {
  size_t bytes_received = 0;
  size_t bytes_remaining = len;
  char *buff = (char *)buffer;
  int recv_len = 0;

  while (bytes_remaining) {
    recv_len = recv(sockfd, buff, bytes_remaining, 0);

    if (recv_len <= 0) {
      return bytes_received;
    }

    bytes_received += recv_len;
    bytes_remaining -= recv_len;
    buff += recv_len;
  }

  return bytes_received;
}

int send_all(int sockfd, void *buffer, size_t len, struct sockaddr_in addr,
             socklen_t addr_len) {
  size_t bytes_sent = 0;
  size_t bytes_remaining = len;
  char *buff = (char *)buffer;
  int send_len = 0;

  while (bytes_remaining) {
    send_len += sendto(sockfd, buff, bytes_remaining, 0,
                       (struct sockaddr *)&addr, addr_len);

    if (send_len <= 0) {
      return bytes_sent;
    }

    bytes_sent += send_len;
    buff += send_len;
    bytes_remaining -= send_len;
  }

  return bytes_sent;
}

int recv_message_or_timeout(char *buff, size_t len, int *conn_id) {
  int ret, i;

  /* We check if data is available on a socket or if a timer has expired */
  ret = poll(data_fds, fdmax, 0);
  assert(ret >= 0);
  ret = poll(timer_fds, fdmax, 0);
  assert(ret >= 0);

  for (i = 0; i < fdmax; i++) {
    /* Data available on a socket */
    if (data_fds[i].revents & POLLIN) {
      // DEBUG_PRINT("%d\n", data_fds[i].fd);
      struct sockaddr_in servaddr;
      socklen_t slen = sizeof(struct sockaddr_in);

      int n = recvfrom(data_fds[i].fd, buff, len, MSG_WAITALL,
                       (struct sockaddr *)&servaddr, &slen);
      int j = -1;

      /* Find which connection this socket coresponds to */
      for (auto const &x : cons) {
        // DEBUG_PRINT("%dA\n", x.second->sockfd);
        if (x.second->sockfd == data_fds[i].fd) {
          j = x.first;
          break;
        }
      }

      if (j == -1) {
        return -14;
      }
      /* Write in the conn_id the connection of the socket*/
      *conn_id = j;
      return n;
    }
    /* A timer has expired on a connection */
    if (timer_fds[i].revents & POLLIN) {
      char dummybuf[8];
      read(timer_fds[i].fd, dummybuf, 8);

      /* Find which connection this coresponds to */
      int j = -1;
      for (auto const &x : cons) {
        if (x.second->sockfd == data_fds[i].fd) {
          j = x.first;
          break;
        }
      }
      assert(j != -1);

      /* Write in the conn_id the connection of the socket*/
      *conn_id = j;
      return -1;
    }
  }

  /* If nothing happened, return -14. */
  return -14;
}
