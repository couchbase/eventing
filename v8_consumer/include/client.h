#ifndef CLIENT_H
#define CLIENT_H

#ifndef STANDALONE_BUILD
extern void(assert)(int);
#else
#include <cassert>
#endif

#include "v8worker.h"

#include <map>
#include <queue>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <vector>

const size_t MAX_BUF_SIZE = 65536;

const int HEADER_FRAGMENT_SIZE = 4;  // uint32
const int PAYLOAD_FRAGMENT_SIZE = 4; // uint32

typedef struct {
  uv_write_t req;
  uv_buf_t buf;
} write_req_t;

typedef struct resp_msg_s {
  uint8_t msg_type;
  uint8_t opcode;
  std::string msg;
} resp_msg_t;

class AppWorker {
public:
  static AppWorker *GetAppWorker();
  void InitTcpSock(const std::string &appname, const std::string &addr,
                   const std::string &worker_id, int batch_size, int port);

  void InitUDS(const std::string &appname, const std::string &addr,
               const std::string &worker_id, int batch_size,
               std::string uds_sock_path);

  void OnConnect(uv_connect_t *conn, int status);
  void OnRead(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
  void OnWrite(uv_write_t *req, int status);

  void ParseValidChunk(uv_stream_t *stream, int nread, const char *buf);

  void RouteMessageWithResponse(header_t *parsed_header,
                                message_t *parsed_message);

  std::vector<char> *GetReadBuffer();

private:
  AppWorker();
  ~AppWorker();

  std::map<int16_t, V8Worker *> workers;

  uv_loop_t main_loop;

  uv_pipe_t uds_sock;
  uv_tcp_t tcp_sock;

  uv_connect_t conn;
  uv_stream_t *conn_handle;
  struct sockaddr_in server_sock;

  bool main_loop_running;
  std::string app_name;

  std::string next_message;

  std::map<int16_t, int16_t> partition_thr_map;

  // Controls the number of virtual partitions, in order to shard work among
  // worker threads
  int16_t partition_count;

  // Controls the size of thread pool, each thread executing user supplied
  // handler code against dcp/timer events
  int16_t thr_count;

  // In order to improve throughput, dcp events are sent in batches
  // batch_size controls the size of it
  int batch_size;

  // Tracks counter for dcp events processed so far and writes to
  // socket when counter reaches batch_size;
  int messages_processed_counter;

  // Captures the config message that will be written by C++ worker
  // to the tcp socket in order to communicate message to Go world
  resp_msg_t *resp_msg;

  bool msg_priority;

  std::vector<char> read_buffer;
};

#endif
