#pragma once

#include <time.h>

#include <cstdint>

#include "lib.h"

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

#define TYPE_SYN 1
#define TYPE_ACK 2
#define TYPE_FIN 4

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
  uint8_t protocol_id;
  uint8_t conn_id;
  uint8_t type;
  uint16_t seq_num;
  uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
  uint8_t protocol_id;
  uint8_t conn_id;
  uint8_t type;
  uint16_t ack_num;
  uint16_t recv_window;
};

struct __attribute__((packed)) tcp_data_segment {
  struct poli_tcp_data_hdr hdr;
  char data[MAX_SEGMENT_SIZE];
};

struct tcp_data_segment_info {
  struct tcp_data_segment segment;
  uint8_t ackd;
};