#ifndef UDS_CLIENT_H
#define UDS_CLIENT_H

#include <mutex>
#include <string>
#include <thread>
#include <uv.h>
#include <vector>

#include "distributor.h"
#include "messages.h"
#include "settings.h"

const size_t MAX_BUF_SIZE = 65536;
const int META_DATA_SIZE = 8;

inline bool IsIpv6(std::string ip_mode) { return (ip_mode == "ipv6"); }

inline std::string Localhost(std::string ip_mode) {
  return IsIpv6(ip_mode) ? "[::1]" : "127.0.0.1";
}

typedef union {
  sockaddr_in sock4;
  sockaddr_in6 sock6;
} sockaddr_in46;

enum reading_state { meta_data, identifier, payload };

class UDSClient : public communicator {

public:
  UDSClient(std::string ipc_type, std::string ip_mode, std::string reader_port,
            std::string feedback_sock_path,
            const std::shared_ptr<settings::cluster> cluster);
  ~UDSClient();

  void Start();
  void send_message(uint64_t metadata_size, const std::vector<uint8_t> &id,
                    const char *payload, uint32_t payload_size);
  void send_message(const std::unique_ptr<messages::worker_request> &wRequest,
                    const std::string &extras, const std::string &meta,
                    const std::string &value);

private:
  void OnConnect(uv_connect_t *conn, int status);
  void OnRead(uv_stream_t *stream, size_t nread, const uv_buf_t *buf);
  void alloc_buffer_main(uv_handle_t *handle, size_t suggested_size,
                         uv_buf_t *buf);

  void StartUDSReceiver();
  void StartTcpSockReceiver();
  void ParseValidChunk(int nread, const char *buf);
  void flushToConn();
  void StartFeedbackUVLoop() { uv_run(&feedback_loop_, UV_RUN_DEFAULT); };
  void StartMainLoop() { uv_run(&main_loop_, UV_RUN_DEFAULT); };
  void OnFeedbackConnect(uv_connect_t *conn, int status);

  uv_loop_t main_loop_;
  uv_loop_t feedback_loop_;

  uv_connect_t conn_;
  uv_connect_t feedback_conn_;

  std::vector<char> read_buffer_;
  std::vector<uint8_t> req_buffer_;
  std::vector<char> res_buffer_;

  reading_state reading_phase_;
  uint64_t id_len_;
  uint64_t payload_len_;

  std::unique_ptr<messages::worker_request> req_;

  uv_stream_t *conn_handle_{nullptr};
  std::mutex conn_mutex_;

  uv_pipe_t uds_sock_;
  uv_tcp_t tcp_sock_;
  uv_pipe_t feedback_uds_sock_;
  uv_tcp_t feedback_tcp_sock_;

  sockaddr_in46 feedback_server_sock_;
  sockaddr_in46 server_sock_;

  std::thread feedback_uv_loop_thr_;
  std::thread main_uv_loop_thr_;

  std::unique_ptr<distributor::distributor> distributor_;
  std::string ip_mode_;  // can be ipv4 or ipv6
  std::string ipc_type_; // can be af_unix or af_inet
  std::string port_;
  std::string feedback_sock_path_;
};

extern UDSClient *client_;

#endif
