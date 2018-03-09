#ifndef CLIENT_H
#define CLIENT_H

#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <math.h>
#include <queue>
#include <signal.h>
#include <sstream>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <vector>

#include "v8worker.h"

const size_t MAX_BUF_SIZE = 65536;
const int HEADER_FRAGMENT_SIZE = 4;  // uint32
const int PAYLOAD_FRAGMENT_SIZE = 4; // uint32
const int SIZEOF_UINT32 = 4;

typedef struct {
  uv_write_t req;
  uv_buf_t buf;
} write_req_t;

typedef struct resp_msg_s {
  std::string msg;
  uint8_t msg_type;
  uint8_t opcode;
} resp_msg_t;

typedef union {
  sockaddr_in sock4;
  sockaddr_in6 sock6;
} sockaddr_in46;

class AppWorker {
public:
  static AppWorker *GetAppWorker();
  std::vector<char> *GetReadBuffer();

  void InitTcpSock(const std::string &appname, const std::string &addr,
                   const std::string &worker_id, int batch_size,
                   int feedback_batch_size, int feedback_port, int port);

  void InitUDS(const std::string &appname, const std::string &addr,
               const std::string &worker_id, int batch_size,
               int feedback_batch_size, std::string feedback_sock_path,
               std::string uds_sock_path);

  void OnConnect(uv_connect_t *conn, int status);
  void OnFeedbackConnect(uv_connect_t *conn, int status);

  void OnRead(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

  void ParseValidChunk(uv_stream_t *stream, int nread, const char *buf);

  void RouteMessageWithResponse(header_t *parsed_header,
                                message_t *parsed_message);

  void StartFeedbackUVLoop();
  void StartMainUVLoop();

  void WriteResponses();

  std::thread main_uv_loop_thr;
  std::thread feedback_uv_loop_thr;

private:
  AppWorker();
  ~AppWorker();

  std::thread write_responses_thr;
  std::map<int16_t, V8Worker *> workers;

  // Socket  handles for out of band data channel to pipeline data to parent
  // eventing-producer
  int feedback_batch_size;
  uv_connect_t feedback_conn;
  uv_stream_t *feedback_conn_handle;
  uv_loop_t feedback_loop;
  bool feedback_loop_running;
  sockaddr_in46 feedback_server_sock;
  uv_tcp_t feedback_tcp_sock;
  uv_pipe_t feedback_uds_sock;

  // Socket handles for data channel to pipeline messages from parent
  // eventing-producer to cpp workers
  int batch_size;
  uv_connect_t conn;
  uv_stream_t *conn_handle;
  uv_loop_t main_loop;
  bool main_loop_running;
  sockaddr_in46 server_sock;
  uv_tcp_t tcp_sock;
  uv_pipe_t uds_sock;

  std::string app_name;

  std::string next_message;

  std::map<int16_t, int16_t> partition_thr_map;

  // Controls the number of virtual partitions, in order to shard work among
  // worker threads
  int16_t partition_count;

  // Controls the size of thread pool, each thread executing user supplied
  // handler code against dcp/timer events
  int16_t thr_count;

  // Captures the config message that will be written by C++ worker
  // to the tcp socket in order to communicate message to Go world
  resp_msg_t *resp_msg;

  bool msg_priority;

  std::vector<char> read_buffer;
};

#endif
