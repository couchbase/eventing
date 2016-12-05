#pragma once

#include "message.h"

#include <queue>
#include <uv.h>
#include <vector>

typedef struct {
  uv_write_t req;
  uv_buf_t buf;
} write_req_t;

class AppWorker {
public:
  static AppWorker *GetAppWorker();
  void Init(const std::string& appname, const std::string& addr, int port);

  void OnConnect(uv_connect_t *conn, int status);
  void OnRead(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
  void OnWrite(uv_write_t *req, int status);

  void WriteMessage(Message *msg);

  void ParseValidChunk(uv_stream_t *stream, int nread, const char *buf);

  std::vector<char> *GetReadBuffer();

private:
  AppWorker();
  ~AppWorker();

  uv_loop_t main_loop;
  uv_tcp_t tcp_sock;
  uv_connect_t conn;
  uv_stream_t *conn_handle;
  struct sockaddr_in server_sock;

  bool main_loop_running;
  std::string app_name;

  std::string next_message;

  std::vector<char> read_buffer;
  MessagePool outgoing_queue;
};
