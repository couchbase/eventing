// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "client.h"
#include "breakpad.h"

uint64_t timer_responses_sent(0);
uint64_t messages_parsed(0);

std::atomic<int64_t> e_app_worker_setting_lost = {0};
std::atomic<int64_t> e_dcp_lost = {0};
std::atomic<int64_t> e_debugger_lost = {0};
std::atomic<int64_t> e_timer_lost = {0};
std::atomic<int64_t> e_v8_worker_lost = {0};

std::atomic<int64_t> delete_events_lost = {0};
std::atomic<int64_t> timer_events_lost = {0};
std::atomic<int64_t> mutation_events_lost = {0};

extern std::atomic<int64_t> timer_context_size_exceeded_counter;
extern std::atomic<int64_t> timer_callback_missing_counter;

std::atomic<int64_t> uv_try_write_failure_counter = {0};

static void alloc_buffer_main(uv_handle_t *handle, size_t suggested_size,
                              uv_buf_t *buf) {
  std::vector<char> *read_buffer =
      AppWorker::GetAppWorker()->GetReadBufferMain();
  *buf = uv_buf_init(read_buffer->data(), read_buffer->capacity());
}

static void alloc_buffer_feedback(uv_handle_t *handle, size_t suggested_size,
                                  uv_buf_t *buf) {
  std::vector<char> *read_buffer =
      AppWorker::GetAppWorker()->GetReadBufferFeedback();
  *buf = uv_buf_init(read_buffer->data(), read_buffer->capacity());
}

int combineAsciiToInt(std::vector<int> *input) {
  int result = 0;
  for (std::string::size_type i = 0; i < input->size(); i++) {
    if ((*input)[i] < 0) {
      result = result + pow(256, i) * (256 + (*input)[i]);
    } else {
      result = result + pow(256, i) * (*input)[i];
    }
  }
  return result;
}

std::unique_ptr<header_t> ParseHeader(message_t *parsed_message) {
  auto header = flatbuf::header::GetHeader(parsed_message->header.c_str());

  auto verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(parsed_message->header.c_str()),
      parsed_message->header.size());

  bool ok = header->Verify(verifier);

  if (ok) {
    std::unique_ptr<header_t> parsed_header(new header_t);

    parsed_header->event = header->event();
    parsed_header->opcode = header->opcode();
    parsed_header->partition = header->partition();

    parsed_header->metadata = header->metadata()->str();

    return parsed_header;
  }

  return nullptr;
}

std::unique_ptr<message_t> ParseServerMessage(int encoded_header_size,
                                              int encoded_payload_size,
                                              const std::string &message) {
  std::unique_ptr<message_t> parsed_message(new message_t);
  parsed_message->header = message.substr(
      HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE, encoded_header_size);
  parsed_message->payload = message.substr(
      HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE + encoded_header_size,
      encoded_payload_size);

  messages_parsed++;

  return parsed_message;
}

AppWorker *AppWorker::GetAppWorker() {
  static AppWorker worker;
  return &worker;
}

std::vector<char> *AppWorker::GetReadBufferMain() { return &read_buffer_main_; }

std::vector<char> *AppWorker::GetReadBufferFeedback() {
  return &read_buffer_feedback_;
}

void AppWorker::InitTcpSock(const std::string &function_name,
                            const std::string &function_id,
                            const std::string &user_prefix,
                            const std::string &appname, const std::string &addr,
                            const std::string &worker_id, int bsize, int fbsize,
                            int feedback_port, int port) {
  uv_tcp_init(&feedback_loop_, &feedback_tcp_sock_);
  uv_tcp_init(&main_loop_, &tcp_sock_);

  if (IsIPv6()) {
    uv_ip6_addr(addr.c_str(), feedback_port, &feedback_server_sock_.sock6);
    uv_ip6_addr(addr.c_str(), port, &server_sock_.sock6);
  } else {
    uv_ip4_addr(addr.c_str(), feedback_port, &feedback_server_sock_.sock4);
    uv_ip4_addr(addr.c_str(), port, &server_sock_.sock4);
  }
  function_name_ = function_name;
  function_id_ = function_id;
  user_prefix_ = user_prefix;
  app_name_ = appname;
  batch_size_ = bsize;
  feedback_batch_size_ = fbsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker with af_inet for appname:" << appname
               << " worker id:" << worker_id << " batch size:" << batch_size_
               << " feedback batch size:" << fbsize
               << " feedback port:" << RS(feedback_port) << " port:" << RS(port)
               << std::endl;

  uv_tcp_connect(&feedback_conn_, &feedback_tcp_sock_,
                 (const struct sockaddr *)&feedback_server_sock_,
                 [](uv_connect_t *feedback_conn, int status) {
                   AppWorker::GetAppWorker()->OnFeedbackConnect(feedback_conn,
                                                                status);
                 });

  uv_tcp_connect(&conn_, &tcp_sock_, (const struct sockaddr *)&server_sock_,
                 [](uv_connect_t *conn, int status) {
                   AppWorker::GetAppWorker()->OnConnect(conn, status);
                 });

  std::thread f_thr(&AppWorker::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr_ = std::move(f_thr);

  std::thread m_thr(&AppWorker::StartMainUVLoop, this);
  main_uv_loop_thr_ = std::move(m_thr);
}

void AppWorker::InitUDS(const std::string &function_name,
                        const std::string &function_id,
                        const std::string &user_prefix,
                        const std::string &appname, const std::string &addr,
                        const std::string &worker_id, int bsize, int fbsize,
                        std::string feedback_sock_path,
                        std::string uds_sock_path) {
  uv_pipe_init(&feedback_loop_, &feedback_uds_sock_, 0);
  uv_pipe_init(&main_loop_, &uds_sock_, 0);

  function_name_ = function_name;
  function_id_ = function_id;
  user_prefix_ = user_prefix;
  app_name_ = appname;
  batch_size_ = bsize;
  feedback_batch_size_ = fbsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker with af_unix for appname:" << appname
               << " worker id:" << worker_id << " batch size:" << batch_size_
               << " feedback batch size:" << fbsize
               << " feedback uds path:" << RS(feedback_sock_path)
               << " uds_path:" << RS(uds_sock_path) << std::endl;

  uv_pipe_connect(
      &feedback_conn_, &feedback_uds_sock_, feedback_sock_path.c_str(),
      [](uv_connect_t *feedback_conn, int status) {
        AppWorker::GetAppWorker()->OnFeedbackConnect(feedback_conn, status);
      });

  uv_pipe_connect(&conn_, &uds_sock_, uds_sock_path.c_str(),
                  [](uv_connect_t *conn, int status) {
                    AppWorker::GetAppWorker()->OnConnect(conn, status);
                  });

  std::thread f_thr(&AppWorker::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr_ = std::move(f_thr);

  std::thread m_thr(&AppWorker::StartMainUVLoop, this);
  main_uv_loop_thr_ = std::move(m_thr);
}

void AppWorker::OnConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    LOG(logInfo) << "Client connected" << std::endl;

    uv_read_start(conn->handle, alloc_buffer_main,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });
    conn_handle_ = conn->handle;
  } else {
    LOG(logError) << "Connection failed with error:" << uv_strerror(status)
                  << std::endl;
  }
}

void AppWorker::OnFeedbackConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    LOG(logInfo) << "Client connected on feedback channel" << std::endl;

    uv_read_start(conn->handle, alloc_buffer_feedback,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });
    feedback_conn_handle_ = conn->handle;

  } else {
    LOG(logError) << "Connection failed with error:" << uv_strerror(status)
                  << std::endl;
  }
}

void AppWorker::OnRead(uv_stream_t *stream, ssize_t nread,
                       const uv_buf_t *buf) {
  if (nread > 0) {
    AppWorker::GetAppWorker()->ParseValidChunk(stream, nread, buf->base);
  } else if (nread == 0) {
    next_message_.clear();
  } else {
    if (nread != UV_EOF) {
      LOG(logError) << "Read error, err code: " << uv_err_name(nread)
                    << std::endl;
    }
    AppWorker::GetAppWorker()->ParseValidChunk(stream, next_message_.length(),
                                               next_message_.c_str());
    next_message_.clear();
    uv_read_stop(stream);
  }
}

void AppWorker::ParseValidChunk(uv_stream_t *stream, int nread,
                                const char *buf) {
  std::string buf_base;
  for (int i = 0; i < nread; i++) {
    buf_base += buf[i];
  }

  if (next_message_.length() > 0) {
    buf_base = next_message_ + buf_base;
    next_message_.clear();
  }

  for (; buf_base.length() > HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE;) {
    std::vector<int> header_entries, payload_entries;
    int encoded_header_size, encoded_payload_size;

    for (int i = 0; i < HEADER_FRAGMENT_SIZE; i++) {
      header_entries.push_back(int(buf_base[i]));
    }
    encoded_header_size = combineAsciiToInt(&header_entries);

    for (int i = HEADER_FRAGMENT_SIZE;
         i < HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE; i++) {
      payload_entries.push_back(int(buf_base[i]));
    }
    encoded_payload_size = combineAsciiToInt(&payload_entries);

    std::string::size_type message_size =
        HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE + encoded_header_size +
        encoded_payload_size;

    if (buf_base.length() < message_size) {
      next_message_.assign(buf_base);
      return;
    } else {
      std::string chunk_to_parse = buf_base.substr(0, message_size);

      std::unique_ptr<message_t> parsed_message = ParseServerMessage(
          encoded_header_size, encoded_payload_size, chunk_to_parse);

      if (parsed_message) {
        message_t *pmessage = parsed_message.release();
        std::unique_ptr<header_t> parsed_header = ParseHeader(pmessage);

        if (parsed_header) {
          header_t *pheader = parsed_header.release();
          RouteMessageWithResponse(pheader, pmessage);

          if (messages_processed_counter >= batch_size_ || msg_priority_) {

            messages_processed_counter = 0;

            // Reset the message priority flag
            msg_priority_ = false;
            if (!resp_msg_->msg.empty()) {
              flatbuffers::FlatBufferBuilder builder;

              auto flatbuf_msg = builder.CreateString(resp_msg_->msg.c_str());
              auto r = flatbuf::response::CreateResponse(
                  builder, resp_msg_->msg_type, resp_msg_->opcode, flatbuf_msg);
              builder.Finish(r);

              uint32_t s = builder.GetSize();
              char *size = (char *)&s;
              FlushToConn(stream, size, SIZEOF_UINT32);

              // Write payload to socket
              std::string msg((const char *)builder.GetBufferPointer(),
                              builder.GetSize());
              FlushToConn(stream, (char *)msg.c_str(), msg.length());

              // Reset the values
              resp_msg_->msg.clear();
              resp_msg_->msg_type = 0;
              resp_msg_->opcode = 0;
            }

            // Flush the aggregate item count in queues for all running V8
            // worker instances
            if (!workers_.empty()) {
              int64_t agg_queue_size = 0, feedback_queue_size = 0,
                      agg_queue_memory = 0;
              for (const auto &w : workers_) {
                agg_queue_size += w.second->worker_queue_->Count();
                feedback_queue_size += w.second->timer_queue_->Count();
                agg_queue_memory += w.second->worker_queue_->Size() +
                                    w.second->timer_queue_->Size();
              }

              std::ostringstream queue_stats;
              queue_stats << R"({"agg_queue_size":)";
              queue_stats << agg_queue_size << R"(, "feedback_queue_size":)";
              queue_stats << feedback_queue_size << R"(, "agg_queue_memory":)";
              queue_stats << agg_queue_memory << "}";

              flatbuffers::FlatBufferBuilder builder;
              auto flatbuf_msg = builder.CreateString(queue_stats.str());
              auto r = flatbuf::response::CreateResponse(
                  builder, mV8_Worker_Config, oQueueSize, flatbuf_msg);
              builder.Finish(r);

              uint32_t s = builder.GetSize();
              char *size = (char *)&s;
              FlushToConn(stream, size, SIZEOF_UINT32);

              // Write payload to socket
              std::string msg((const char *)builder.GetBufferPointer(),
                              builder.GetSize());
              FlushToConn(stream, (char *)msg.c_str(), msg.length());
            }
          }
        }
      }
    }
    buf_base.erase(0, message_size);
  }

  if (buf_base.length() > 0) {
    next_message_.assign(buf_base);
  }
}

void AppWorker::FlushToConn(uv_stream_t *stream, char *msg, int length) {
  auto buffer = uv_buf_init(msg, length);

  int bytes_written = 0;
  auto wrapper = buffer;

  while (bytes_written < buffer.len) {
    auto rc = uv_try_write(stream, &wrapper, 1);
    if (rc == UV_EAGAIN) {
      continue;
    }

    if (rc < 0) {
      LOG(logError) << " uv_try_write failed while flushing payload content"
                       ",bytes_written: "
                    << bytes_written << std::endl;
      uv_try_write_failure_counter++;
      break;
    }

    bytes_written += rc;
    wrapper.base += rc;
    wrapper.len -= rc;
  }
}

std::string ToString(const std::vector<int64_t> &histogram) {
  std::ostringstream out;
  for (std::string::size_type i = 0; i < histogram.size(); i++) {
    if (i == 0) {
      out << "{";
    }

    if (histogram[i] > 0) {
      if ((i > 0) && (out.str().length() > 1)) {
        out << ",";
      }

      if (i == 0) {
        out << R"(")" << HIST_FROM << R"(":)" << histogram[i];
      } else {
        out << R"(")" << i * HIST_WIDTH << R"(":)" << histogram[i];
      }
    }

    if (i == histogram.size() - 1) {
      out << "}";
    }
  }
  return out.str();
}

void AppWorker::RouteMessageWithResponse(header_t *parsed_header,
                                         message_t *parsed_message) {
  std::string key, val, doc_id, callback_fn, doc_ids_cb_fns, compile_resp;
  v8::Platform *platform;
  server_settings_t *server_settings;
  handler_config_t *handler_config;

  int worker_index;
  int64_t latency_buckets, agg_queue_size, feedback_queue_size,
      agg_queue_memory;
  std::vector<int64_t> agg_hgram, worker_hgram;
  std::ostringstream lstats, estats, fstats;
  std::map<int, int64_t> agg_lcb_exceptions;
  std::string::size_type i = 0;
  std::string handler_instance_id;

  const flatbuf::payload::Payload *payload;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuf::payload::VbsThreadMap>>
      *thr_map;

  LOG(logTrace) << "Event: " << static_cast<int16_t>(parsed_header->event)
                << " Opcode: " << static_cast<int16_t>(parsed_header->opcode)
                << std::endl;

  switch (getEvent(parsed_header->event)) {
  case eV8_Worker:
    switch (getV8WorkerOpcode(parsed_header->opcode)) {
    case oDispose:
    case oInit:
      payload = flatbuf::payload::GetPayload(
          (const void *)parsed_message->payload.c_str());

      handler_config = new handler_config_t;
      server_settings = new server_settings_t;

      handler_config->app_name.assign(payload->app_name()->str());
      handler_config->timer_context_size = payload->timer_context_size();
      handler_config->dep_cfg.assign(payload->depcfg()->str());
      handler_config->execution_timeout = payload->execution_timeout();
      handler_config->lcb_inst_capacity = payload->lcb_inst_capacity();
      handler_config->skip_lcb_bootstrap = payload->skip_lcb_bootstrap();
      handler_config->timer_context_size = payload->timer_context_size();
      handler_config->handler_headers =
          ToStringArray(payload->handler_headers());
      handler_config->handler_footers =
          ToStringArray(payload->handler_footers());

      server_settings->checkpoint_interval = payload->checkpoint_interval();
      checkpoint_interval_ =
          std::chrono::milliseconds(server_settings->checkpoint_interval);
      server_settings->debugger_port = payload->debugger_port()->str();
      server_settings->eventing_dir.assign(payload->eventing_dir()->str());
      server_settings->eventing_port.assign(
          payload->curr_eventing_port()->str());
      server_settings->eventing_sslport.assign(
          payload->curr_eventing_sslport()->str());
      server_settings->host_addr.assign(payload->curr_host()->str());
      server_settings->kv_host_port.assign(payload->kv_host_port()->str());

      handler_instance_id = payload->function_instance_id()->str();

      LOG(logDebug) << "Loading app:" << app_name_ << std::endl;

      v8::V8::InitializeICUDefaultLocation("");
      platform = v8::platform::CreateDefaultPlatform();
      v8::V8::InitializePlatform(platform);
      v8::V8::Initialize();

      for (int16_t i = 0; i < thr_count_; i++) {
        V8Worker *w = new V8Worker(platform, handler_config, server_settings,
                                   function_name_, function_id_,
                                   handler_instance_id, user_prefix_);

        LOG(logInfo) << "Init index: " << i << " V8Worker: " << w << std::endl;
        workers_[i] = w;
      }

      delete handler_config;

      msg_priority_ = true;
      break;
    case oLoad:
      LOG(logDebug) << "Loading app code:" << RM(parsed_header->metadata)
                    << std::endl;
      for (int16_t i = 0; i < thr_count_; i++) {
        workers_[i]->V8WorkerLoad(parsed_header->metadata);

        LOG(logInfo) << "Load index: " << i << " V8Worker: " << workers_[i]
                     << std::endl;
      }
      msg_priority_ = true;
      break;
    case oTerminate:
      break;
    case oGetLatencyStats:
      if (workers_.size() > 0) {
        latency_buckets = workers_[0]->histogram_->Buckets();
        agg_hgram.assign(latency_buckets, 0);
      }
      for (const auto &w : workers_) {
        worker_hgram = w.second->histogram_->Hgram();
        for (std::string::size_type i = 0; i < worker_hgram.size(); i++) {
          agg_hgram[i] += worker_hgram[i];
        }
      }

      resp_msg_->msg.assign(ToString(agg_hgram));
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oLatencyStats;
      msg_priority_ = true;
      break;
    case oGetCurlLatencyStats:
      if (workers_.size() > 0) {
        latency_buckets = workers_[0]->curl_latency_->Buckets();
        agg_hgram.assign(latency_buckets, 0);
      }
      for (const auto &w : workers_) {
        worker_hgram = w.second->curl_latency_->Hgram();
        for (std::string::size_type i = 0; i < worker_hgram.size(); i++) {
          agg_hgram[i] += worker_hgram[i];
        }
      }

      resp_msg_->msg.assign(ToString(agg_hgram));
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oCurlLatencyStats;
      msg_priority_ = true;
      break;
    case oGetFailureStats:
      fstats.str(std::string());
      fstats << R"({"bucket_op_exception_count":)";
      fstats << bucket_op_exception_count << R"(, "n1ql_op_exception_count":)";
      fstats << n1ql_op_exception_count << R"(, "timeout_count":)";
      fstats << timeout_count << R"(, "checkpoint_failure_count":)";
      fstats << checkpoint_failure_count << ",";

      fstats << R"("dcp_events_lost": )" << e_dcp_lost << ",";
      fstats << R"("v8worker_events_lost": )" << e_v8_worker_lost << ",";
      fstats << R"("app_worker_setting_events_lost": )"
             << e_app_worker_setting_lost << ",";
      fstats << R"("timer_events_lost": )" << e_timer_lost << ",";
      fstats << R"("debugger_events_lost": )" << e_debugger_lost << ",";
      fstats << R"("mutation_events_lost": )" << mutation_events_lost << ",";
      fstats << R"("timer_context_size_exceeded_counter": )"
             << timer_context_size_exceeded_counter << ",";
      fstats << R"("timer_callback_missing_counter": )"
             << timer_callback_missing_counter << ",";
      fstats << R"("delete_events_lost": )" << delete_events_lost << ",";
      fstats << R"("timer_events_lost": )" << timer_events_lost << ",";
      fstats << R"("timestamp" : ")" << GetTimestampNow() << R"(")";
      fstats << "}";
      LOG(logTrace) << "v8worker failure stats : " << fstats.str() << std::endl;

      resp_msg_->msg.assign(fstats.str());
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oFailureStats;
      msg_priority_ = true;
      break;
    case oGetExecutionStats:
      estats.str(std::string());
      estats << R"({"on_update_success":)";
      estats << on_update_success << R"(, "on_update_failure":)";
      estats << on_update_failure << R"(, "on_delete_success":)";
      estats << on_delete_success << R"(, "on_delete_failure":)";
      estats << on_delete_failure << R"(, "timer_create_failure":)";
      estats << timer_create_failure << R"(, "messages_parsed":)";
      estats << messages_parsed << R"(, "dcp_delete_msg_counter":)";
      estats << dcp_delete_msg_counter << R"(, "dcp_mutation_msg_counter":)";
      estats << dcp_mutation_msg_counter << R"(, "timer_msg_counter":)";
      estats << timer_msg_counter << R"(, "enqueued_dcp_delete_msg_counter":)";
      estats << enqueued_dcp_delete_msg_counter
             << R"(, "enqueued_dcp_mutation_msg_counter":)";
      estats << enqueued_dcp_mutation_msg_counter
             << R"(, "enqueued_timer_msg_counter":)";
      estats << enqueued_timer_msg_counter;
      estats << R"(, "timer_responses_sent":)";
      estats << timer_responses_sent;
      estats << R"(, "uv_try_write_failure_counter":)";
      estats << uv_try_write_failure_counter;
      estats << R"(, "lcb_retry_failure":)" << lcb_retry_failure;

      if (!workers_.empty()) {
        agg_queue_memory = agg_queue_size = feedback_queue_size = 0;
        for (const auto &w : workers_) {
          agg_queue_size += w.second->worker_queue_->Count();
          feedback_queue_size += w.second->timer_queue_->Count();
          agg_queue_memory +=
              w.second->worker_queue_->Size() + w.second->timer_queue_->Size();
        }

        estats << R"(, "agg_queue_size":)" << agg_queue_size;
        estats << R"(, "feedback_queue_size":)" << feedback_queue_size;
        estats << R"(, "agg_queue_memory":)" << agg_queue_memory;
      }

      estats << R"(, "timestamp":")" << GetTimestampNow() << R"("})";
      LOG(logTrace) << "v8worker execution stats:" << estats.str() << std::endl;

      resp_msg_->msg.assign(estats.str());
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oExecutionStats;
      msg_priority_ = true;
      break;
    case oGetCompileInfo:
      LOG(logDebug) << "Compiling app code:" << RM(parsed_header->metadata)
                    << std::endl;
      compile_resp = workers_[0]->CompileHandler(parsed_header->metadata);

      resp_msg_->msg.assign(compile_resp);
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oCompileInfo;
      msg_priority_ = true;
      break;
    case oGetLcbExceptions:
      for (const auto &w : workers_) {
        w.second->ListLcbExceptions(agg_lcb_exceptions);
      }

      estats.str(std::string());
      i = 0;

      for (auto const &entry : agg_lcb_exceptions) {
        if (i == 0) {
          estats << "{";
        }

        if ((i > 0) && (estats.str().length() > 1)) {
          estats << ",";
        }

        estats << R"(")" << entry.first << R"(":)" << entry.second;

        if (i == agg_lcb_exceptions.size() - 1) {
          estats << "}";
        }

        i++;
      }

      resp_msg_->msg.assign(estats.str());
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oLcbExceptions;
      msg_priority_ = true;
      break;
    case oVersion:
    default:
      LOG(logError) << "Opcode " << getV8WorkerOpcode(parsed_header->opcode)
                    << "is not implemented for eV8Worker" << std::endl;
      ++e_v8_worker_lost;
      break;
    }
    break;
  case eDCP:
    payload = flatbuf::payload::GetPayload(
        (const void *)parsed_message->payload.c_str());
    val.assign(payload->value()->str());

    switch (getDCPOpcode(parsed_header->opcode)) {
    case oDelete:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        enqueued_dcp_delete_msg_counter++;
        workers_[worker_index]->Enqueue(parsed_header, parsed_message);
      } else {
        LOG(logError) << "Delete event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++delete_events_lost;
      }
      break;
    case oMutation:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        enqueued_dcp_mutation_msg_counter++;
        workers_[worker_index]->Enqueue(parsed_header, parsed_message);
      } else {
        LOG(logError) << "Mutation event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++mutation_events_lost;
      }
      break;
    default:
      LOG(logError) << "Opcode " << getDCPOpcode(parsed_header->opcode)
                    << "is not implemented for eDCP" << std::endl;
      ++e_dcp_lost;
      break;
    }
    break;
  case eFilter:
    switch (getFilterOpcode(parsed_header->opcode)) {
    case oVbFilter:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->UpdateVbFilter(parsed_header->metadata);
        LOG(logInfo) << "Received filter event from Go "
                     << parsed_header->metadata << std::endl;
        int vb_no = 0;
        int64_t seq_no = 0;
        int skip_ack = 0;
        if (kSuccess ==
            workers_[worker_index]->ParseMetadataWithAck(
                parsed_header->metadata, vb_no, seq_no, skip_ack, true)) {
          auto bucketops_seqno =
              workers_[worker_index]->GetBucketopsSeqno(vb_no);
          SendFilterAck(oVbFilter, mFilterAck, vb_no, bucketops_seqno,
                        skip_ack);
        }
      } else {
        LOG(logError) << "Filter event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case oProcessedSeqNo:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        LOG(logInfo) << "Received update processed seq_no event from Go "
                     << parsed_header->metadata << std::endl;
        int vb_no = 0;
        int64_t seq_no = 0;
        if (kSuccess == workers_[worker_index]->ParseMetadata(
                            parsed_header->metadata, vb_no, seq_no)) {
          workers_[worker_index]->UpdateBucketopsSeqno(vb_no, seq_no);
        }
      }
      break;
    default:
      LOG(logError) << "Opcode " << getFilterOpcode(parsed_header->opcode)
                    << "is not implemented for filtering" << std::endl;
      break;
    }
    break;
  case eTimer:
    switch (getTimerOpcode(parsed_header->opcode)) {
    case oTimer:
      if (workers_[curr_worker_idx_] != nullptr) {
        enqueued_timer_msg_counter++;
        workers_[curr_worker_idx_]->Enqueue(parsed_header, parsed_message);
        curr_worker_idx_ = (curr_worker_idx_ + 1) % thr_count_;
      } else {
        LOG(logError) << "Timer event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++timer_events_lost;
      }
      break;
    default:
      LOG(logError) << "Opcode " << getTimerOpcode(parsed_header->opcode)
                    << "is not implemented for eTimer" << std::endl;
      ++e_timer_lost;
      break;
    }
    break;
  case eApp_Worker_Setting:
    switch (getAppWorkerSettingOpcode(parsed_header->opcode)) {
    case oLogLevel:
      SystemLog::setLogLevel(LevelFromString(parsed_header->metadata));
      LOG(logInfo) << "Configured log level: " << parsed_header->metadata
                   << std::endl;
      msg_priority_ = true;
      break;
    case oWorkerThreadCount:
      LOG(logInfo) << "Worker thread count: " << parsed_header->metadata
                   << std::endl;
      thr_count_ = int16_t(std::stoi(parsed_header->metadata));
      msg_priority_ = true;
      break;
    case oWorkerThreadMap:
      payload = flatbuf::payload::GetPayload(
          (const void *)parsed_message->payload.c_str());
      thr_map = payload->thr_map();
      partition_count_ = payload->partitionCount();
      LOG(logInfo) << "Request for worker thread map, size: " << thr_map->size()
                   << " partition_count: " << partition_count_ << std::endl;

      for (unsigned int i = 0; i < thr_map->size(); i++) {
        int16_t thread_id = thr_map->Get(i)->threadID();

        for (unsigned int j = 0; j < thr_map->Get(i)->partitions()->size();
             j++) {
          auto p_id = thr_map->Get(i)->partitions()->Get(j);
          partition_thr_map_[p_id] = thread_id;
        }
      }
      msg_priority_ = true;
      break;
    case oTimerContextSize:
      timer_context_size = std::stol(parsed_header->metadata);
      LOG(logInfo) << "Setting timer_context_size to " << timer_context_size
                   << std::endl;
      msg_priority_ = true;
      break;
    default:
      LOG(logError) << "Opcode "
                    << getAppWorkerSettingOpcode(parsed_header->opcode)
                    << "is not implemented for eApp_Worker_Setting"
                    << std::endl;
      ++e_app_worker_setting_lost;
      break;
    }
    break;
  case eDebugger:
    switch (getDebuggerOpcode(parsed_header->opcode)) {
    case oDebuggerStart:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->Enqueue(parsed_header, parsed_message);
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger start event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case oDebuggerStop:
      worker_index = partition_thr_map_[parsed_header->partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->Enqueue(parsed_header, parsed_message);
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger stop event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    default:
      LOG(logError) << "Opcode " << getDebuggerOpcode(parsed_header->opcode)
                    << "is not implemented for eDebugger" << std::endl;
      ++e_debugger_lost;
      break;
    }
    break;
  default:
    LOG(logError) << "Unknown command" << std::endl;
    break;
  }
}

void AppWorker::StartMainUVLoop() {
  if (!main_loop_running_) {
    uv_run(&main_loop_, UV_RUN_DEFAULT);
    main_loop_running_ = true;
  }
}

void AppWorker::StartFeedbackUVLoop() {
  if (!feedback_loop_running_) {
    uv_run(&feedback_loop_, UV_RUN_DEFAULT);
    feedback_loop_running_ = true;
  }
}

void AppWorker::WriteResponses() {
  // TODO : Remove this sleep if its use can't be justified
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  auto start = std::chrono::system_clock::now();
  int batch_size = (feedback_batch_size_ & 1) ? (feedback_batch_size_ + 1)
                                              : feedback_batch_size_;
  while (!thread_exit_cond_.load()) {
    auto sleep = true;
    // Update DocTimers Checkpoint
    for (const auto &w : workers_) {
      std::vector<uv_buf_t> messages;
      w.second->GetTimerMessages(messages, batch_size);
      if (messages.empty()) {
        continue;
      }

      sleep = false;
      WriteResponseWithRetry(feedback_conn_handle_, messages, batch_size);
      timer_responses_sent += messages.size() / 2;
      for (auto &buf : messages) {
        delete buf.base;
      }
    }

    if (sleep) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto curr = std::chrono::system_clock::now();
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(curr - start);
    if (elapsed < checkpoint_interval_) {
      continue;
    }

    // Update BucketOps Checkpoint
    for (const auto &w : workers_) {
      std::vector<uv_buf_t> messages;
      std::vector<int> length_prefix_sum;
      w.second->GetBucketOpsMessages(messages);
      if (messages.empty()) {
        continue;
      }

      WriteResponseWithRetry(feedback_conn_handle_, messages, batch_size);
      for (auto &buf : messages) {
        delete buf.base;
      }
    }

    start = curr;
  }
}

void AppWorker::WriteResponseWithRetry(uv_stream_t *handle,
                                       std::vector<uv_buf_t> messages,
                                       size_t max_batch_size) {
  size_t curr_idx = 0, counter = 0;
  while (curr_idx < messages.size()) {
    size_t batch_size = std::min(max_batch_size, messages.size() - curr_idx);
    int bytes_written =
        uv_try_write(handle, messages.data() + curr_idx, batch_size);
    if (bytes_written < 0) {
      uv_try_write_failure_counter++;
      counter++;
      if (counter < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * counter));
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
      }
    } else {
      int written = bytes_written;
      for (size_t idx = curr_idx; idx < messages.size(); idx++, curr_idx++) {
        if (messages[idx].len > written) {
          messages[idx].len -= written;
          messages[idx].base += written;
          break;
        } else {
          written -= messages[idx].len;
        }
      }
      counter = 0;
    }
  }
}

AppWorker::AppWorker()
    : feedback_conn_handle_(nullptr), conn_handle_(nullptr),
      curr_worker_idx_(0) {
  thread_exit_cond_.store(false);
  uv_loop_init(&feedback_loop_);
  uv_loop_init(&main_loop_);
  feedback_loop_async_.data = (void *)&feedback_loop_;
  main_loop_async_.data = (void *)&main_loop_;
  uv_async_init(&feedback_loop_, &feedback_loop_async_, AppWorker::StopUvLoop);
  uv_async_init(&main_loop_, &main_loop_async_, AppWorker::StopUvLoop);

  read_buffer_main_.resize(MAX_BUF_SIZE);
  read_buffer_feedback_.resize(MAX_BUF_SIZE);
  resp_msg_ = new (resp_msg_t);
  msg_priority_ = false;

  feedback_loop_running_ = false;
  main_loop_running_ = false;

  std::thread w_thr(&AppWorker::WriteResponses, this);
  write_responses_thr_ = std::move(w_thr);
  checkpoint_interval_ = std::chrono::milliseconds(1000); // default value
}

AppWorker::~AppWorker() {
  if (feedback_uv_loop_thr_.joinable()) {
    feedback_uv_loop_thr_.join();
  }

  if (main_uv_loop_thr_.joinable()) {
    main_uv_loop_thr_.join();
  }

  if (write_responses_thr_.joinable()) {
    write_responses_thr_.join();
  }

  for (auto &v8worker : workers_) {
    delete v8worker.second;
  }

  uv_loop_close(&feedback_loop_);
  uv_loop_close(&main_loop_);
}

void AppWorker::ReadStdinLoop() {
  auto functor = [](AppWorker *worker, uv_async_t *async1, uv_async_t *async2) {
    std::string token;
    while (!std::cin.eof()) {
      std::cin.clear();
      std::getline(std::cin, token);
    }
    worker->thread_exit_cond_.store(true);
    for (auto &v8worker : worker->workers_) {
      if (v8worker.second != nullptr) {
        v8worker.second->SetThreadExitFlag();
      }
    }
    uv_async_send(async1);
    uv_async_send(async2);
  };
  std::thread thr(functor, this, &feedback_loop_async_, &main_loop_async_);
  stdin_read_thr_ = std::move(thr);
}

void AppWorker::StopUvLoop(uv_async_t *async) {
  uv_loop_t *handle = (uv_loop_t *)async->data;
  uv_stop(handle);
}

void AppWorker::SendFilterAck(int opcode, int msgtype, int vb_no,
                              int64_t seq_no, bool skip_ack) {
  std::ostringstream filter_ack;
  filter_ack << R"({"vb":)";
  filter_ack << vb_no << R"(, "seq":)";
  filter_ack << seq_no << R"(, "skip_ack":)";
  filter_ack << skip_ack << "}";

  resp_msg_->msg.assign(filter_ack.str());
  resp_msg_->msg_type = msgtype;
  resp_msg_->opcode = opcode;
  msg_priority_ = true;
  LOG(logInfo) << "vb: " << vb_no << " seqNo: " << seq_no
               << " skip_ack: " << skip_ack << " sending filter ack to Go"
               << std::endl;
}

int main(int argc, char **argv) {

  if (argc < 11) {
    std::cerr
        << "Need at least 11 arguments: appname, ipc_type, port, feedback_port"
           "worker_id, batch_size, feedback_batch_size, diag_dir, ipv4/6, "
           "breakpad_on, handler_uuid"
        << std::endl;
    return 2;
  }

  SetIPv6(std::string(argv[9]) == "ipv6");

  srand(static_cast<unsigned>(time(nullptr)));

  std::string appname(argv[1]);
  std::string ipc_type(argv[2]); // can be af_unix or af_inet

  std::string feedback_sock_path, uds_sock_path;
  int feedback_port, port;

  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    uds_sock_path.assign(argv[3]);
    feedback_sock_path.assign(argv[4]);
  } else {
    port = atoi(argv[3]);
    feedback_port = atoi(argv[4]);
  }

  std::string worker_id(argv[5]);
  int batch_size = atoi(argv[6]);
  int feedback_batch_size = atoi(argv[7]);
  std::string diag_dir(argv[8]);

  if (strcmp(argv[10], "true") == 0) {
    setupBreakpad(diag_dir);
  }

  std::string user_prefix;
  if (argc >= 13) {
    user_prefix = std::string(argv[12]);
  }

  curl_global_init(CURL_GLOBAL_ALL);
  std::string function_id(argv[11]);
  std::string function_name(argv[1]);
  AppWorker *worker = AppWorker::GetAppWorker();
  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    worker->InitUDS(function_name, function_id, user_prefix, appname,
                    Localhost(false), worker_id, batch_size,
                    feedback_batch_size, feedback_sock_path, uds_sock_path);
  } else {
    worker->InitTcpSock(function_name, function_id, user_prefix, appname,
                        Localhost(false), worker_id, batch_size,
                        feedback_batch_size, feedback_port, port);
  }
  worker->ReadStdinLoop();
  worker->stdin_read_thr_.join();
  worker->main_uv_loop_thr_.join();
  worker->feedback_uv_loop_thr_.join();

  curl_global_cleanup();
}
