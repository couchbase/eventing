#include "uds_client.h"
#include "breakpad.h"
#include "messages.h"
#include "stats.h"
#include <iostream>
#include <v8.h>

std::atomic<int64_t> uv_try_write_failure_counter = {0};
// UDSClient implementation
UDSClient *client_;

UDSClient::UDSClient(std::string ipc_type, std::string ip_mode,
                     std::string reader_port, std::string feedback_sock_path,
                     const std::shared_ptr<settings::cluster> cluster)
    : conn_mutex_(), ip_mode_(ip_mode), ipc_type_(ipc_type), port_(reader_port),
      feedback_sock_path_(feedback_sock_path) {

  uv_loop_init(&main_loop_);
  uv_loop_init(&feedback_loop_);

  read_buffer_.resize(MAX_BUF_SIZE);
  req_buffer_.reserve(META_DATA_SIZE);
  reading_phase_ = meta_data;

  auto comm = std::shared_ptr<communicator>(this);
  distributor_ = std::unique_ptr<distributor::distributor>(
      new distributor::multiFunctionDistributor(comm, cluster));

  id_len_ = 0;
  payload_len_ = 0;
}

UDSClient::~UDSClient() {
  if (main_uv_loop_thr_.joinable()) {
    main_uv_loop_thr_.join();
  }

  if (feedback_uv_loop_thr_.joinable()) {
    feedback_uv_loop_thr_.join();
  }
}

void UDSClient::OnRead(uv_stream_t *stream, size_t nread, const uv_buf_t *buf) {
  if (nread > 0) {
    ParseValidChunk(nread, buf->base);
  } else if (nread < 0) {
    if ((uv_errno_t)nread != UV_EOF) {
    }
    LOG(logError) << "Read error from libuv socket: " << nread << std::endl;
    uv_read_stop(stream);
  }
}

void UDSClient::ParseValidChunk(int nread, const char *buf) {
  int buf_index = 0;
  while (buf_index < nread) {
    switch (reading_phase_) {
    case meta_data: {
      req_buffer_.push_back(
          (uint8_t) static_cast<unsigned char>((buf[buf_index])));
      buf_index++;

      if (req_buffer_.size() != META_DATA_SIZE) {
        continue;
      }

      reading_phase_ = identifier;
      auto [id_len, payload_len] = messages::getMessageSize(req_buffer_);
      id_len_ = id_len;
      payload_len_ = payload_len;

      req_ = std::unique_ptr<messages::worker_request>(
          new messages::worker_request);
      req_->event = messages::getEventCode(req_buffer_);
      req_->opcode = messages::getOpcode(req_buffer_);
      req_->identifier.reserve(id_len_);
      req_->payload.reserve(payload_len_);

      req_buffer_.clear();
      break;
    }

    case identifier: {
      req_->identifier.push_back(
          (uint8_t) static_cast<unsigned char>((buf[buf_index])));
      buf_index++;
      if (req_->identifier.size() != id_len_) {
        continue;
      }

      reading_phase_ = payload;
      id_len_ = 0;

      if (payload_len_ == 0) {
        reading_phase_ = meta_data;
        distributor_->push_msg(std::move(req_));
      }
      break;
    }

    case payload: {
      req_->payload.push_back(
          (uint8_t) static_cast<unsigned char>((buf[buf_index])));
      buf_index++;
      if (req_->payload.size() != payload_len_) {
        continue;
      }
      distributor_->push_msg(std::move(req_));
      // push this message to evaluator
      reading_phase_ = meta_data;
      payload_len_ = 0;
      break;
    }
    }
  }
}

void UDSClient::alloc_buffer_main(uv_handle_t *handle, size_t suggested_size,
                                  uv_buf_t *buf) {
  *buf = uv_buf_init(read_buffer_.data(), read_buffer_.size());
}

void UDSClient::StartUDSReceiver() {
  uv_pipe_init(&feedback_loop_, &feedback_uds_sock_, 0);
  uv_pipe_init(&main_loop_, &uds_sock_, 0);

  uv_pipe_connect(&feedback_conn_, &feedback_uds_sock_,
                  feedback_sock_path_.c_str(),
                  [](uv_connect_t *feedback_conn, int status) {
                    client_->OnFeedbackConnect(feedback_conn, status);
                  });

  uv_pipe_connect(
      &conn_, &uds_sock_, port_.c_str(),
      [](uv_connect_t *conn, int status) { client_->OnConnect(conn, status); });

  std::thread f_thr(&UDSClient::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr_ = std::move(f_thr);

  std::thread m_thr(&UDSClient::StartMainLoop, this);
  main_uv_loop_thr_ = std::move(m_thr);
}

void UDSClient::StartTcpSockReceiver() {
  uv_tcp_init(&feedback_loop_, &feedback_tcp_sock_);
  uv_tcp_init(&main_loop_, &tcp_sock_);

  auto addr = Localhost(ip_mode_);
  auto ip_port = atoi(port_.c_str());
  auto feedback_port = atoi(feedback_sock_path_.c_str());
  if (IsIpv6(ip_mode_)) {
    uv_ip6_addr(addr.c_str(), feedback_port, &feedback_server_sock_.sock6);
    uv_ip6_addr(addr.c_str(), ip_port, &server_sock_.sock6);
  } else {
    uv_ip4_addr(addr.c_str(), feedback_port, &feedback_server_sock_.sock4);
    uv_ip4_addr(addr.c_str(), ip_port, &server_sock_.sock4);
  }

  uv_tcp_connect(&feedback_conn_, &feedback_tcp_sock_,
                 (const struct sockaddr *)&feedback_server_sock_,
                 [](uv_connect_t *feedback_conn, int status) {
                   client_->OnFeedbackConnect(feedback_conn, status);
                 });

  uv_tcp_connect(
      &conn_, &tcp_sock_, (const struct sockaddr *)&server_sock_,
      [](uv_connect_t *conn, int status) { client_->OnConnect(conn, status); });

  std::thread f_thr(&UDSClient::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr_ = std::move(f_thr);

  std::thread m_thr(&UDSClient::StartMainLoop, this);
  main_uv_loop_thr_ = std::move(m_thr);
}

void UDSClient::OnFeedbackConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    std::lock_guard<std::mutex> lck(conn_mutex_);
    conn_handle_ = conn->handle;
    if (res_buffer_.size() > 0) {
      flushToConn();
    }
  } else {
    LOG(logError) << "On feedback connect error: " << status << std::endl;
  }
}

void UDSClient::OnConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    uv_read_start(
        conn->handle,
        [](uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
          client_->alloc_buffer_main(handle, suggested_size, buf);
        },
        [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
          client_->OnRead(stream, nread, buf);
        });

  } else {
    LOG(logError) << "On connect error: " << status << std::endl;
  }
}

void UDSClient::Start() {
  if (ipc_type_ == "af_unix") {
    StartUDSReceiver();
  } else {
    StartTcpSockReceiver();
  }

  feedback_uv_loop_thr_.join();
  main_uv_loop_thr_.join();
}

void UDSClient::send_message(uint64_t metadata_size,
                             const std::vector<uint8_t> &id,
                             const char *payload, uint32_t payload_size) {
  std::lock_guard<std::mutex> lck(conn_mutex_);
  uint8_t byte;

  for (int i = 0; i < 8; i++) {
    byte = (metadata_size >> 56);
    res_buffer_.push_back(byte);
    metadata_size = metadata_size << 8;
  }

  for (auto byteIter = id.begin(); byteIter != id.end(); ++byteIter) {
    res_buffer_.push_back(*byteIter);
  }

  for (uint32_t i = 0; i < payload_size; i++) {
    res_buffer_.push_back(payload[i]);
  }

  flushToConn();
}

void UDSClient::send_message(
    const std::unique_ptr<messages::worker_request> &wRequest,
    const std::string &extras, const std::string &meta,
    const std::string &value) {

  flatbuffers::FlatBufferBuilder builder;
  auto extra_flatbuf = builder.CreateString(extras);
  auto meta_flatbuf = builder.CreateString(meta);
  auto val_flatbuf = builder.CreateString(value);
  auto r = flatbuf::header_v2::CreateHeaderV2(builder, extra_flatbuf,
                                              meta_flatbuf, val_flatbuf);
  builder.Finish(r);

  uint32_t size = builder.GetSize();
  std::string msg((const char *)builder.GetBufferPointer(), size);

  uint64_t meta_header = messages::getMetadata(wRequest, size);
  send_message(meta_header, wRequest->identifier, msg.c_str(), size);
}

void UDSClient::flushToConn() {
  if (!conn_handle_) {
    return;
  }
  auto buffer = uv_buf_init(res_buffer_.data(), res_buffer_.size());

  while (buffer.len > 0) {
    auto rc = uv_try_write(conn_handle_, &buffer, 1);
    if (rc == UV_EAGAIN) {
      continue;
    }
    if (rc < 0) {
      uv_try_write_failure_counter++;
      break;
    }

    buffer.base += rc;
    buffer.len -= rc;
  }

  res_buffer_.clear();
}

int main(int argc, char **argv) {

  if (argc != 15) {
    LOG(logError)
        << "Need at least 15 arguments: executable_loc, ipc_type, ip_mode, "
           "port, feedback_sock_path, diag_dir, eventing_dir, breakpad_on, "
           "ns_server_port, eventing_port, debugger_port, cert_file, "
           "client_cert_file, client_key_file, single_function_mode"
        << std::endl;
    return 2;
  }

  std::string executable_loc(argv[0]);
  std::string ipc_type(argv[1]); // can be af_unix or af_inet
  std::string ip_mode(argv[2]);  // can be ipv4 or ipv6
  std::string port = argv[3];
  std::string feedback_sock_path = argv[4];
  std::string diag_dir(argv[5]);
  std::string eventing_dir(argv[6]);
  std::string breakpad_on(argv[7]);
  std::string ns_server_port(argv[8]);
  std::string eventing_port(argv[9]);
  std::string debugger_port(argv[10]);
  std::string cert_file(argv[11]);
  std::string client_cert_file(argv[12]);
  std::string client_key_file(argv[13]);
  std::string single_function_mode(argv[14]);

  v8::V8::InitializeICUDefaultLocation(executable_loc.c_str(), nullptr);
  if (breakpad_on == "true")
    setupBreakpad(diag_dir);

  bool single_function_mode_ = false;
  if (single_function_mode == "true") {
    single_function_mode_ = true;
  }

  std::string host_addr_ = Localhost(ip_mode);
  auto cluster = std::make_shared<settings::cluster>(IsIpv6(ip_mode),
      host_addr_, eventing_dir, ns_server_port, eventing_port, debugger_port,
      cert_file, client_cert_file, client_key_file, single_function_mode_);
  client_ = new UDSClient(ipc_type, ip_mode, port, feedback_sock_path,
                          std::move(cluster));
  client_->Start();

  return 0;
}
