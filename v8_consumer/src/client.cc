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

#include <chrono>
#include <string>
#include <thread>

#include "breakpad.h"
#include "client.h"
#include <nlohmann/json.hpp>

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
std::atomic<int64_t> uv_msg_parse_failure = {0};

extern std::atomic<int64_t> timer_context_size_exceeded_counter;
extern std::atomic<int64_t> timer_callback_missing_counter;

std::atomic<int64_t> uv_try_write_failure_counter = {0};

std::string executable_img;

std::string GetFailureStats() {
  nlohmann::json fstats;
  fstats["bucket_op_exception_count"] = bucket_op_exception_count.load();
  fstats["n1ql_op_exception_count"] = n1ql_op_exception_count.load();
  fstats["timeout_count"] = timeout_count.load();
  fstats["checkpoint_failure_count"] = checkpoint_failure_count.load();
  fstats["dcp_events_lost"] = e_dcp_lost.load();
  fstats["v8worker_events_lost"] = e_v8_worker_lost.load();
  fstats["app_worker_setting_events_lost"] = e_app_worker_setting_lost.load();
  fstats["timer_events_lost"] = e_timer_lost.load();
  fstats["debugger_events_lost"] = e_debugger_lost.load();
  fstats["mutation_events_lost"] = mutation_events_lost.load();
  fstats["timer_context_size_exceeded_counter"] =
      timer_context_size_exceeded_counter.load();
  fstats["timer_callback_missing_counter"] =
      timer_callback_missing_counter.load();
  fstats["delete_events_lost"] = delete_events_lost.load();
  fstats["timer_events_lost"] = timer_events_lost.load();
  fstats["curl_non_200_response"] = Curl::GetStats().GetCurlFailureStat();
  fstats["timestamp"] = GetTimestampNow();
  return fstats.dump();
}

std::string GetExecutionStats(const std::map<int16_t, V8Worker *> &workers) {
  nlohmann::json estats;
  estats["on_update_success"] = on_update_success.load();
  estats["on_update_failure"] = on_update_failure.load();
  estats["on_delete_success"] = on_delete_success.load();
  estats["on_delete_failure"] = on_delete_failure.load();
  estats["timer_create_failure"] = timer_create_failure.load();
  estats["messages_parsed"] = messages_parsed;
  estats["dcp_delete_msg_counter"] = dcp_delete_msg_counter.load();
  estats["dcp_mutation_msg_counter"] = dcp_mutation_msg_counter.load();
  estats["timer_msg_counter"] = timer_msg_counter.load();
  estats["timer_create_counter"] = timer_create_counter.load();
  estats["timer_cancel_counter"] = timer_cancel_counter.load();
  estats["enqueued_dcp_delete_msg_counter"] =
      enqueued_dcp_delete_msg_counter.load();
  estats["enqueued_dcp_mutation_msg_counter"] =
      enqueued_dcp_mutation_msg_counter.load();
  estats["enqueued_timer_msg_counter"] = enqueued_timer_msg_counter.load();
  estats["timer_responses_sent"] = timer_responses_sent;
  estats["uv_try_write_failure_counter"] = uv_try_write_failure_counter.load();
  estats["lcb_retry_failure"] = lcb_retry_failure.load();
  estats["dcp_delete_parse_failure"] = dcp_delete_parse_failure.load();
  estats["dcp_mutation_parse_failure"] = dcp_mutation_parse_failure.load();
  estats["filtered_dcp_delete_counter"] = filtered_dcp_delete_counter.load();
  estats["filtered_dcp_mutation_counter"] =
      filtered_dcp_mutation_counter.load();
  if (!workers.empty()) {
    int64_t agg_queue_memory = 0, agg_queue_size = 0;
    for (const auto &w : workers) {
      agg_queue_size += w.second->worker_queue_->GetSize();
      agg_queue_memory += w.second->worker_queue_->GetMemory();
    }

    estats["agg_queue_size"] = agg_queue_size;
    estats["feedback_queue_size"] = 0;
    estats["agg_queue_memory"] = agg_queue_memory;
    estats["processed_events_size"] = processed_events_size.load();
    estats["num_processed_events"] = num_processed_events.load();
  }
  estats["curl"]["get"] = Curl::GetStats().GetCurlGetStat();
  estats["curl"]["post"] = Curl::GetStats().GetCurlPostStat();
  estats["curl"]["delete"] = Curl::GetStats().GetCurlDeleteStat();
  estats["curl"]["head"] = Curl::GetStats().GetCurlHeadStat();
  estats["curl"]["put"] = Curl::GetStats().GetCurlPutStat();
  estats["timestamp"] = GetTimestampNow();
  estats["uv_msg_parse_failure"] = uv_msg_parse_failure.load();
  return estats.dump();
}

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

std::pair<bool, std::unique_ptr<WorkerMessage>>
AppWorker::GetWorkerMessage(int encoded_header_size, int encoded_payload_size,
                            const std::string &msg) {
  messages_parsed++;
  std::unique_ptr<WorkerMessage> worker_msg(new WorkerMessage);

  // Parsing payload
  worker_msg->payload.header = msg.substr(
      HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE, encoded_header_size);
  worker_msg->payload.payload = msg.substr(
      HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE + encoded_header_size,
      encoded_payload_size);

  // Parsing header
  const MessagePayload &payload = worker_msg->payload;
  auto header_flatbuf = flatbuf::header::GetHeader(payload.header.c_str());
  auto verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(payload.header.c_str()),
      payload.header.size());

  if (!header_flatbuf->Verify(verifier)) {
    return {false, std::move(worker_msg)};
  }
  worker_msg->header.event = header_flatbuf->event();
  worker_msg->header.opcode = header_flatbuf->opcode();
  worker_msg->header.partition = header_flatbuf->partition();
  worker_msg->header.metadata = header_flatbuf->metadata()->str();
  return {true, std::move(worker_msg)};
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
  processed_events_size = 0;
  num_processed_events = 0;

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
  processed_events_size = 0;
  num_processed_events = 0;

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

      auto worker_msg = GetWorkerMessage(encoded_header_size,
                                         encoded_payload_size, chunk_to_parse);
      if (worker_msg.first) {
        RouteMessageWithResponse(std::move(worker_msg.second));

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

          // Flush the aggregate item count in queues for all running
          // V8 worker instances
          if (!workers_.empty()) {
            int64_t agg_queue_size = 0, agg_queue_memory = 0;
            for (const auto &w : workers_) {
              agg_queue_size += w.second->worker_queue_->GetSize();
              agg_queue_memory += w.second->worker_queue_->GetMemory();
            }

            std::ostringstream queue_stats;
            queue_stats << R"({"agg_queue_size":)";
            queue_stats << agg_queue_size << R"(, "feedback_queue_size":)";
            queue_stats << 0 << R"(, "agg_queue_memory":)";
            queue_stats << agg_queue_memory << R"(, "processed_events_size":)";
            queue_stats << processed_events_size
                        << R"(, "num_processed_events":)";
            queue_stats << num_processed_events << "}";

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
      } else {
        ++uv_msg_parse_failure;
        // We only need to know the first message which failed to parse as the
        // subsequent messages will fail to get parsed anyway
        if (uv_msg_parse_failure == 1) {
          LOG(logError)
              << "Failed to parse message from uv buffer. Buffer contents : "
              << RU(buf_base) << std::endl;
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

  unsigned bytes_written = 0;
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

    bytes_written += static_cast<unsigned>(rc);
    wrapper.base += rc;
    wrapper.len -= rc;
  }
}

void AppWorker::RouteMessageWithResponse(
    std::unique_ptr<WorkerMessage> worker_msg) {
  std::string key, val, doc_id, callback_fn, doc_ids_cb_fns, compile_resp;
  v8::Platform *platform;
  server_settings_t *server_settings;
  handler_config_t *handler_config;

  int worker_index;
  nlohmann::json estats;
  std::map<int, int64_t> agg_lcb_exceptions;
  std::string handler_instance_id;

  const flatbuf::payload::Payload *payload;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuf::payload::VbsThreadMap>>
      *thr_map;

  LOG(logTrace) << "Event: " << static_cast<int16_t>(worker_msg->header.event)
                << " Opcode: "
                << static_cast<int16_t>(worker_msg->header.opcode) << std::endl;

  switch (getEvent(worker_msg->header.event)) {
  case eV8_Worker:
    switch (getV8WorkerOpcode(worker_msg->header.opcode)) {
    case oDispose:
    case oInit:
      payload = flatbuf::payload::GetPayload(
          (const void *)worker_msg->payload.payload.c_str());

      handler_config = new handler_config_t;
      server_settings = new server_settings_t;

      handler_config->n1ql_prepare_all = payload->n1ql_prepare_all();
      handler_config->app_name.assign(payload->app_name()->str());
      handler_config->lang_compat.assign(
          payload->language_compatibility()->str());
      handler_config->timer_context_size = payload->timer_context_size();
      handler_config->dep_cfg.assign(payload->depcfg()->str());
      handler_config->execution_timeout = payload->execution_timeout();
      handler_config->lcb_retry_count = payload->lcb_retry_count();
      handler_config->lcb_inst_capacity = payload->lcb_inst_capacity();
      handler_config->n1ql_consistency = payload->n1ql_consistency()->str();
      handler_config->skip_lcb_bootstrap = payload->skip_lcb_bootstrap();
      using_timer_ = payload->using_timer();
      handler_config->using_timer = using_timer_;
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

      handler_instance_id = payload->function_instance_id()->str();

      LOG(logDebug) << "Loading app:" << app_name_ << std::endl;

      v8::V8::InitializeICUDefaultLocation(executable_img.c_str(), nullptr);
      platform = v8::platform::CreateDefaultPlatform();
      v8::V8::InitializePlatform(platform);
      v8::V8::Initialize();

      {
        std::lock_guard<std::mutex> lck(workers_map_mutex_);
        for (int16_t i = 0; i < thr_count_; i++) {
          V8Worker *w = new V8Worker(
              platform, handler_config, server_settings, function_name_,
              function_id_, handler_instance_id, user_prefix_, &latency_stats_,
              &curl_latency_stats_, ns_server_port_, num_vbuckets_);

          LOG(logInfo) << "Init index: " << i << " V8Worker: " << w
                       << std::endl;
          workers_[i] = w;
        }

        delete handler_config;

        msg_priority_ = true;
        v8worker_init_done_ = true;
      }
      break;
    case oLoad:
      LOG(logDebug) << "Loading app code:" << RM(worker_msg->header.metadata)
                    << std::endl;
      for (int16_t i = 0; i < thr_count_; i++) {
        workers_[i]->V8WorkerLoad(worker_msg->header.metadata);

        LOG(logInfo) << "Load index: " << i << " V8Worker: " << workers_[i]
                     << std::endl;
      }
      msg_priority_ = true;
      break;
    case oTerminate:
      break;

    case oGetLatencyStats:
      resp_msg_->msg = latency_stats_.ToString();
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oLatencyStats;
      msg_priority_ = true;
      break;

    case oGetCurlLatencyStats:
      resp_msg_->msg = curl_latency_stats_.ToString();
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oCurlLatencyStats;
      msg_priority_ = true;
      break;

    case oInsight:
      resp_msg_->msg = GetInsight();
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oCodeInsights;
      msg_priority_ = true;
      LOG(logDebug) << "Responding with insight " << resp_msg_->msg
                    << std::endl;
      break;

    case oGetFailureStats:
      LOG(logTrace) << "v8worker failure stats : " << GetFailureStats()
                    << std::endl;

      resp_msg_->msg.assign(GetFailureStats());
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oFailureStats;
      msg_priority_ = true;
      break;
    case oGetExecutionStats:
      LOG(logTrace) << "v8worker execution stats:"
                    << GetExecutionStats(workers_) << std::endl;
      resp_msg_->msg.assign(GetExecutionStats(workers_));
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oExecutionStats;
      msg_priority_ = true;
      break;
    case oGetCompileInfo:
      LOG(logDebug) << "Compiling app code:" << RM(worker_msg->header.metadata)
                    << std::endl;
      compile_resp = workers_[0]->Compile(worker_msg->header.metadata);

      resp_msg_->msg.assign(compile_resp);
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oCompileInfo;
      msg_priority_ = true;
      break;
    case oGetLcbExceptions:
      for (const auto &w : workers_) {
        w.second->ListLcbExceptions(agg_lcb_exceptions);
      }

      estats.clear();

      for (auto const &entry : agg_lcb_exceptions) {
        estats[std::to_string(entry.first)] = entry.second;
      }

      resp_msg_->msg.assign(estats.dump());
      resp_msg_->msg_type = mV8_Worker_Config;
      resp_msg_->opcode = oLcbExceptions;
      msg_priority_ = true;
      break;
    case oVersion:
    default:
      LOG(logError) << "Opcode " << getV8WorkerOpcode(worker_msg->header.opcode)
                    << "is not implemented for eV8Worker" << std::endl;
      ++e_v8_worker_lost;
      break;
    }
    break;
  case eDCP:
    payload = flatbuf::payload::GetPayload(
        (const void *)worker_msg->payload.payload.c_str());
    val.assign(payload->value()->str());

    switch (getDCPOpcode(worker_msg->header.opcode)) {
    case oDelete:
      worker_index = partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        enqueued_dcp_delete_msg_counter++;
        workers_[worker_index]->PushBack(std::move(worker_msg));
      } else {
        LOG(logError) << "Delete event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++delete_events_lost;
      }
      break;
    case oMutation:
      worker_index = partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        enqueued_dcp_mutation_msg_counter++;
        workers_[worker_index]->PushBack(std::move(worker_msg));
      } else {
        LOG(logError) << "Mutation event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++mutation_events_lost;
      }
      break;
    default:
      LOG(logError) << "Opcode " << getDCPOpcode(worker_msg->header.opcode)
                    << "is not implemented for eDCP" << std::endl;
      ++e_dcp_lost;
      break;
    }
    break;
  case eFilter:
    switch (getFilterOpcode(worker_msg->header.opcode)) {
    case oVbFilter: {

      worker_index = partition_thr_map_[worker_msg->header.partition];
      auto worker = workers_[worker_index];
      if (worker != nullptr) {
        LOG(logInfo) << "Received filter event from Go "
                     << worker_msg->header.metadata << std::endl;
        int vb_no = 0, skip_ack = 0;
        uint64_t filter_seq_no = 0;
        if (kSuccess ==
            worker->ParseMetadataWithAck(worker_msg->header.metadata, vb_no,
                                         filter_seq_no, skip_ack, true)) {
          auto lck = worker->GetAndLockFilterLock();
          auto last_processed_seq_no = worker->GetBucketopsSeqno(vb_no);
          if (last_processed_seq_no < filter_seq_no) {
            worker->UpdateVbFilter(vb_no, filter_seq_no);
          }
          worker->RemoveTimerPartition(vb_no);
          lck.unlock();
          SendFilterAck(oVbFilter, mFilterAck, vb_no, last_processed_seq_no,
                        skip_ack);
        }
      } else {
        LOG(logError) << "Filter event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
    } break;
    case oProcessedSeqNo:
      worker_index = partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        LOG(logInfo) << "Received update processed seq_no event from Go "
                     << worker_msg->header.metadata << std::endl;
        int vb_no = 0;
        uint64_t seq_no = 0;
        if (kSuccess == workers_[worker_index]->ParseMetadata(
                            worker_msg->header.metadata, vb_no, seq_no)) {
          auto lck = workers_[worker_index]->GetAndLockFilterLock();
          workers_[worker_index]->UpdateBucketopsSeqnoLocked(vb_no, seq_no);
          workers_[worker_index]->AddTimerPartition(vb_no);
        }
      }
      break;
    default:
      LOG(logError) << "Opcode " << getFilterOpcode(worker_msg->header.opcode)
                    << "is not implemented for filtering" << std::endl;
      break;
    }
    break;
  case ePauseConsumer: {
    pause_consumer_.store(true);
    std::unordered_map<int64_t, uint64_t> lps_map;
    for (int16_t idx = 0; idx < thr_count_; ++idx) {
      auto worker = workers_[idx];
      auto lck = worker->GetAndLockFilterLock();
      worker->StopTimerScan();
      auto partitions = worker->GetPartitions();
      for (auto vb : partitions) {
        auto lps = worker->GetBucketopsSeqno(vb);
        worker->UpdateVbFilter(vb, std::numeric_limits<uint64_t>::max());
        worker->RemoveTimerPartition(vb);
        lps_map[vb] = lps;
      }
    }
    SendPauseAck(lps_map);
  } break;
  case eApp_Worker_Setting:
    switch (getAppWorkerSettingOpcode(worker_msg->header.opcode)) {
    case oLogLevel:
      SystemLog::setLogLevel(LevelFromString(worker_msg->header.metadata));
      LOG(logInfo) << "Configured log level: " << worker_msg->header.metadata
                   << std::endl;
      msg_priority_ = true;
      break;
    case oWorkerThreadCount:
      LOG(logInfo) << "Worker thread count: " << worker_msg->header.metadata
                   << std::endl;
      thr_count_ = int16_t(std::stoi(worker_msg->header.metadata));
      msg_priority_ = true;
      break;
    case oWorkerThreadMap:
      payload = flatbuf::payload::GetPayload(
          (const void *)worker_msg->payload.payload.c_str());
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
      timer_context_size = std::stol(worker_msg->header.metadata);
      LOG(logInfo) << "Setting timer_context_size to " << timer_context_size
                   << std::endl;
      msg_priority_ = true;
      break;

    case oVbMap: {
      payload = flatbuf::payload::GetPayload(
          (const void *)worker_msg->payload.payload.c_str());
      auto vb_map = payload->vb_map();
      std::vector<int64_t> vbuckets;
      for (size_t idx = 0; idx < vb_map->size(); ++idx) {
        vbuckets.push_back(vb_map->Get(idx));
      }

      auto partitions = PartitionVbuckets(vbuckets);

      for (int16_t idx = 0; idx < thr_count_; ++idx) {
        auto worker = workers_[idx];
        worker->UpdatePartitions(partitions[idx]);
      }
      std::ostringstream oss;
      std::copy(vbuckets.begin(), vbuckets.end(),
                std::ostream_iterator<int64_t>(oss, " "));

      LOG(logInfo) << "Updating vbucket map, vbmap :" << oss.str() << std::endl;
    } break;

    case oWorkerMemQuota: {
      memory_quota_ = std::stoll(worker_msg->header.metadata);
      msg_priority_ = true;
      break;
    }
    default:
      LOG(logError) << "Opcode "
                    << getAppWorkerSettingOpcode(worker_msg->header.opcode)
                    << "is not implemented for eApp_Worker_Setting"
                    << std::endl;
      ++e_app_worker_setting_lost;
      break;
    }
    break;
  case eDebugger:
    switch (getDebuggerOpcode(worker_msg->header.opcode)) {
    case oDebuggerStart:
      worker_index = partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger start event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case oDebuggerStop:
      worker_index = partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger stop event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    default:
      LOG(logError) << "Opcode " << getDebuggerOpcode(worker_msg->header.opcode)
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

  size_t batch_size = (feedback_batch_size_ & 1) ? (feedback_batch_size_ + 1)
                                                 : feedback_batch_size_;
  while (!thread_exit_cond_.load()) {
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
        delete[] buf.base;
      }
    }
    std::this_thread::sleep_for(checkpoint_interval_);
  }
}

void AppWorker::WriteResponseWithRetry(uv_stream_t *handle,
                                       std::vector<uv_buf_t> messages,
                                       size_t max_batch_size) {
  size_t curr_idx = 0, counter = 0;
  while (curr_idx < messages.size()) {
    size_t batch_size = messages.size() - curr_idx;
    batch_size = std::min(max_batch_size, batch_size);
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
        if (messages[idx].len > static_cast<unsigned>(written)) {
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

AppWorker::AppWorker() : feedback_conn_handle_(nullptr), conn_handle_(nullptr) {
  thread_exit_cond_.store(false);
  pause_consumer_.store(false);
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

  if (event_gen_thr_.joinable()) {
    event_gen_thr_.join();
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

void AppWorker::EventGenLoop() {
  std::this_thread::sleep_for(std::chrono::seconds(2));
  auto evt_generator = [](AppWorker *worker) {
    while (!worker->thread_exit_cond_.load()) {
      {
        std::lock_guard<std::mutex> lck(worker->workers_map_mutex_);

        if (worker->v8worker_init_done_ && !worker->pause_consumer_.load()) {
          // Scan for timers
          if (worker->using_timer_) {
            for (auto &v8_worker : worker->workers_) {
              if (!v8_worker.second->scan_timer_.load()) {
                v8_worker.second->scan_timer_.store(true);
                std::unique_ptr<WorkerMessage> msg(new WorkerMessage);
                msg->header.event = eInternal + 1;
                msg->header.opcode = oScanTimer;
                v8_worker.second->PushFront(std::move(msg));
              }
            }
          }

          // Update the v8 heap size
          for (auto &v8_worker : worker->workers_) {
            if (!v8_worker.second->update_v8_heap_.load()) {
              v8_worker.second->update_v8_heap_.store(true);
              std::unique_ptr<WorkerMessage> msg(new WorkerMessage);
              msg->header.event = eInternal + 1;
              msg->header.opcode = oUpdateV8HeapSize;
              v8_worker.second->PushFront(std::move(msg));
            }
          }

          // Check for memory growth
          int64_t approx_memory = 0;
          for (const auto &v8_worker : worker->workers_) {
            approx_memory += v8_worker.second->worker_queue_->GetMemory() +
                             v8_worker.second->v8_heap_size_;
          }

          for (auto &v8_worker : worker->workers_) {
            if (!v8_worker.second->run_gc_.load() &&
                      (v8_worker.second->v8_heap_size_ > MAX_V8_HEAP_SIZE ||
                       approx_memory > worker->memory_quota_ * 0.8)) {
              v8_worker.second->run_gc_.store(true);
              std::unique_ptr<WorkerMessage> msg(new WorkerMessage);
              msg->header.event = eInternal + 1;
              msg->header.opcode = oRunGc;
              v8_worker.second->PushFront(std::move(msg));
            }
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::seconds(7));
    }
  };
  std::thread thr(evt_generator, this);
  event_gen_thr_ = std::move(thr);
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

std::vector<std::unordered_set<int64_t>>
AppWorker::PartitionVbuckets(const std::vector<int64_t> &vbuckets) const {
  std::vector<std::unordered_set<int64_t>> partitions(thr_count_);
  for (auto vb : vbuckets) {
    auto it = partition_thr_map_.find(vb);
    if (it != end(partition_thr_map_)) {
      partitions[it->second].insert(vb);
    }
  }
  return partitions;
}

void AppWorker::SendPauseAck(
    const std::unordered_map<int64_t, uint64_t> &lps_map) {
  nlohmann::json lps_list;
  for (const auto &[vb, lps] : lps_map) {
    nlohmann::json lps_info;
    lps_info["vb"] = vb;
    lps_info["seq"] = lps;
    lps_list.push_back(lps_info);
  }
  resp_msg_->msg.assign(lps_list.dump());
  resp_msg_->msg_type = mPauseAck;
  msg_priority_ = true;
}

int main(int argc, char **argv) {
  if (argc < 16) {
    std::cerr
        << "Need at least 13 arguments: appname, ipc_type, port, "
           "feedback_port"
           "worker_id, batch_size, feedback_batch_size, diag_dir, ipv4/6, "
           "breakpad_on, handler_uuid, user_prefix, ns_server_port, num_vbuckets, eventing_port"
        << std::endl;
    return 2;
  }

  executable_img = argv[0];
  std::string appname(argv[1]);
  std::string ipc_type(argv[2]); // can be af_unix or af_inet
  std::string port = argv[3];
  std::string feedback_port(argv[4]);
  std::string worker_id(argv[5]);
  auto batch_size = atoi(argv[6]);
  auto feedback_batch_size = atoi(argv[7]);
  std::string diag_dir(argv[8]);
  std::string ip_type(argv[9]);
  std::string breakpad_on(argv[10]);
  std::string function_id(argv[11]);
  std::string user_prefix(argv[12]);
  std::string ns_server_port(argv[13]);
  auto num_vbuckets = atoi(argv[14]);

  srand(static_cast<unsigned>(time(nullptr)));
  curl_global_init(CURL_GLOBAL_ALL);

  SetIPv6(ip_type == "ipv6");

  if (breakpad_on == "true") {
    setupBreakpad(diag_dir);
  }

  AppWorker *worker = AppWorker::GetAppWorker();
  worker->SetNsServerPort(ns_server_port);
  worker->SetNumVbuckets(num_vbuckets);
  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    worker->InitUDS(appname, function_id, user_prefix, appname,
                    Localhost(false), worker_id, batch_size,
                    feedback_batch_size, feedback_port, port);
  } else {
    worker->InitTcpSock(appname, function_id, user_prefix, appname,
                        Localhost(false), worker_id, batch_size,
                        feedback_batch_size, atoi(feedback_port.c_str()),
                        atoi(port.c_str()));
  }

  worker->ReadStdinLoop();
  worker->EventGenLoop();
  worker->stdin_read_thr_.join();
  worker->event_gen_thr_.join();
  worker->main_uv_loop_thr_.join();
  worker->feedback_uv_loop_thr_.join();

  curl_global_cleanup();
  return 0;
}

std::string AppWorker::GetInsight() {
  CodeInsight sum(nullptr);
  for (int16_t i = 0; i < thr_count_; i++) {
    auto &entry = workers_[i]->GetInsight();
    sum.Accumulate(entry);
  }
  return sum.ToJSON();
}
