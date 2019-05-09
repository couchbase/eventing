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

#include "parse_deployment.h"
#include "v8worker.h"

const size_t MAX_BUF_SIZE = 65536;
const int HEADER_FRAGMENT_SIZE = 4;  // uint32
const int PAYLOAD_FRAGMENT_SIZE = 4; // uint32
const int SIZEOF_UINT32 = 4;

int64_t timer_context_size;

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
  std::vector<char> *GetReadBufferMain();

  std::vector<char> *GetReadBufferFeedback();

  void FlushToConn(uv_stream_t *stream, char *buffer, int length);

  void InitTcpSock(const std::string &function_name,
                   const std::string &function_id,
                   const std::string &user_prefix, const std::string &appname,
                   const std::string &addr, const std::string &worker_id,
                   int batch_size, int feedback_batch_size, int feedback_port,
                   int port);

  void InitUDS(const std::string &function_name, const std::string &function_id,
               const std::string &user_prefix, const std::string &appname,
               const std::string &addr, const std::string &worker_id,
               int batch_size, int feedback_batch_size,
               std::string feedback_sock_path, std::string uds_sock_path);

  void OnConnect(uv_connect_t *conn, int status);
  void OnFeedbackConnect(uv_connect_t *conn, int status);

  void OnRead(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

  static std::pair<bool, std::unique_ptr<WorkerMessage>>
  GetWorkerMessage(int encoded_header_size, int encoded_payload_size,
                   const std::string &msg);
  void ParseValidChunk(uv_stream_t *stream, int nread, const char *buf);

  void RouteMessageWithResponse(std::unique_ptr<WorkerMessage> worker_msg);

  void StartFeedbackUVLoop();
  void StartMainUVLoop();

  void WriteResponses();

  void ReadStdinLoop();

  static void StopUvLoop(uv_async_t *);

  void SendFilterAck(int opcode, int msgtype, int vb_no, int64_t seq_no,
                     bool skip_ack);

  std::thread main_uv_loop_thr_;
  std::thread feedback_uv_loop_thr_;
  std::thread stdin_read_thr_;

protected:
  void WriteResponseWithRetry(uv_stream_t *handle,
                              std::vector<uv_buf_t> messages,
                              size_t batch_size);

private:
  AppWorker();
  ~AppWorker();
  std::thread write_responses_thr_;
  std::map<int16_t, V8Worker *> workers_;
  std::chrono::milliseconds checkpoint_interval_;

  Histogram latency_stats_;
  Histogram curl_latency_stats_;

  // Socket  handles for out of band data channel to pipeline data to parent
  // eventing-producer
  int feedback_batch_size_;
  uv_connect_t feedback_conn_;
  uv_stream_t *feedback_conn_handle_;
  uv_loop_t feedback_loop_;
  uv_async_t feedback_loop_async_;
  bool feedback_loop_running_;
  sockaddr_in46 feedback_server_sock_;
  uv_tcp_t feedback_tcp_sock_;
  uv_pipe_t feedback_uds_sock_;

  // Socket handles for data channel to pipeline messages from parent
  // eventing-producer to cpp workers
  int batch_size_;
  uv_connect_t conn_;
  uv_stream_t *conn_handle_;
  uv_loop_t main_loop_;
  uv_async_t main_loop_async_;
  bool main_loop_running_;
  sockaddr_in46 server_sock_;
  uv_tcp_t tcp_sock_;
  uv_pipe_t uds_sock_;

  std::string app_name_;

  std::string function_name_;

  std::string function_id_;

  std::string user_prefix_;

  std::string next_message_;

  std::map<int16_t, int16_t> partition_thr_map_;

  int16_t curr_worker_idx_;

  // Controls the number of virtual partitions, in order to shard work among
  // worker threads
  int16_t partition_count_;

  // Controls the size of thread pool, each thread executing user supplied
  // handler code against dcp/timer events
  int16_t thr_count_;

  // Captures the config message that will be written by C++ worker
  // to the tcp socket in order to communicate message to Go world
  resp_msg_t *resp_msg_;

  bool msg_priority_;

  std::vector<char> read_buffer_main_;

  std::vector<char> read_buffer_feedback_;

  std::atomic<bool> thread_exit_cond_;
};

#endif
