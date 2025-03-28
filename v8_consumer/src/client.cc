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
#include "lcb_utils.h"

#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>

uint64_t timer_responses_sent(0);
uint64_t messages_parsed(0);

std::string curr_encryption_level("control_or_off");

std::string executable_img;

std::string GetFailureStats() {
  nlohmann::json fstats;
  return fstats.dump();
}

std::string GetExecutionStats(const std::map<int16_t, V8Worker *> &workers,
                              bool test) {
  nlohmann::json estats;
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
                            const std::string &app_location,
                            const std::string &addr,
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
  app_location_ = app_location;
  batch_size_ = bsize;
  feedback_batch_size_ = fbsize;

  LOG(logInfo) << "Starting worker with af_inet for appname:" << app_location
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
                        const std::string &app_location,
                        const std::string &addr, const std::string &worker_id,
                        int bsize, int fbsize, std::string feedback_sock_path,
                        std::string uds_sock_path) {
  uv_pipe_init(&feedback_loop_, &feedback_uds_sock_, 0);
  uv_pipe_init(&main_loop_, &uds_sock_, 0);

  function_name_ = function_name;
  function_id_ = function_id;
  user_prefix_ = user_prefix;
  app_location_ = app_location;
  batch_size_ = bsize;
  feedback_batch_size_ = fbsize;

  LOG(logInfo) << "Starting worker with af_unix for appname:" << app_location
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

void AppWorker::InitVbMapResources() {
  vb_seq_ = std::make_shared<vb_seq_map_t>();
  vb_locks_ = std::make_shared<vb_lock_map_t>();
  for (int i = 0; i < num_vbuckets_; i++) {
    (*vb_seq_)[i] = atomic_ptr_t(new std::atomic<uint64_t>(0));
    (*vb_locks_)[i] = new std::mutex();
  }
  processed_bucketops_ =
      std::make_shared<std::vector<uint64_t>>(num_vbuckets_, 0);
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
  } else if (nread < 0) {
    if (nread != UV_EOF) {
      LOG(logError) << "Read error, err code: " << uv_err_name(nread)
                    << std::endl;
    }

    std::string old_message_ = next_message_;
    next_message_.clear();
    AppWorker::GetAppWorker()->ParseValidChunk(stream, old_message_.length(),
                                               old_message_.c_str());
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

        if (msg_priority_) {

          // Reset the message priority flag
          msg_priority_ = false;
          if (!resp_msg_->msg.empty()) {
            flatbuffers::FlatBufferBuilder builder;

            auto flatbuf_msg = builder.CreateString(resp_msg_->msg.c_str());
            auto r = flatbuf::response::CreateResponse(
                builder, static_cast<int8_t>(resp_msg_->msg_type),
                static_cast<int8_t>(resp_msg_->opcode), flatbuf_msg);
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
            resp_msg_->msg_type = resp_msg_type::Msg_Unknown;
            resp_msg_->opcode =
                v8_worker_config_opcode::V8_Worker_Config_Opcode_Unknown;
          }

          // Flush the aggregate item count in queues for all running
          // V8 worker instances
          if (!workers_.empty()) {
            int64_t agg_queue_size = 0, agg_queue_memory = 0;
            for (const auto &w : workers_) {
              agg_queue_size += w.second->worker_queue_->GetSize();
              agg_queue_memory += w.second->worker_queue_->GetMemory();
              if (w.second->event_processing_ongoing_.load())
                agg_queue_size += 1;
            }

            std::ostringstream queue_stats;

            flatbuffers::FlatBufferBuilder builder;
            auto flatbuf_msg = builder.CreateString(queue_stats.str());
            auto r = flatbuf::response::CreateResponse(
                builder, static_cast<int8_t>(resp_msg_type::mV8_Worker_Config),
                static_cast<int8_t>(v8_worker_config_opcode::oQueueSize),
                flatbuf_msg);
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
        // We only need to know the first message which failed to parse as the
        // subsequent messages will fail to get parsed anyway
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
  std::unique_ptr<v8::Platform> platform;
  server_settings_t *server_settings;
  handler_config_t *handler_config;

  int16_t worker_index;
  nlohmann::json estats;
  std::map<int, int64_t> agg_lcb_exceptions;
  std::string handler_instance_id;

  const flatbuf::payload::Payload *payload;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuf::payload::VbsThreadMap>>
      *thr_map;

  LOG(logTrace) << "Event: " << worker_msg->header.event
                << " Opcode: " << worker_msg->header.opcode << std::endl;

  switch (getEvent(worker_msg->header.event)) {
  case event_type::eOnDeploy:
      workers_[0]->PushBack(std::move(worker_msg));
      break;
  case event_type::eV8_Worker:
    switch (getV8WorkerOpcode(worker_msg->header.opcode)) {
    case v8_worker_opcode::oInit:
      payload = flatbuf::payload::GetPayload(
          (const void *)worker_msg->payload.payload.c_str());

      handler_config = new handler_config_t;
      server_settings = new server_settings_t;

      handler_config->n1ql_prepare_all = payload->n1ql_prepare_all();
      handler_config->app_name.assign(payload->app_name()->str());
      handler_config->bucket.assign(payload->bucket()->str());
      handler_config->scope.assign(payload->scope()->str());
      handler_config->lang_compat.assign(
          payload->language_compatibility()->str());
      handler_config->timer_context_size = payload->timer_context_size();
      handler_config->dep_cfg.assign(payload->depcfg()->str());
      handler_config->execution_timeout = payload->execution_timeout();
      handler_config->cursor_checkpoint_timeout =
          payload->cursor_checkpoint_timeout();
      handler_config->lcb_retry_count = payload->lcb_retry_count();
      handler_config->lcb_timeout = payload->lcb_timeout();
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
      handler_config->num_timer_partitions = payload->num_timer_partitions();
      handler_config->bucket_cache_size = payload->bucket_cache_size();
      handler_config->bucket_cache_age = payload->bucket_cache_age();

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
      server_settings->certFile.assign(payload->certFile()->str());
      curr_encryption_level = payload->encryption_level()->str();

      handler_instance_id = payload->function_instance_id()->str();
      handler_config->curl_max_allowed_resp_size =
          payload->curl_max_allowed_resp_size();

      LOG(logDebug) << "Loading app:" << app_location_ << std::endl;

      v8::V8::InitializeICUDefaultLocation(executable_img.c_str(), nullptr);
      platform = v8::platform::NewDefaultPlatform();
      v8::V8::InitializePlatform(platform.get());
      v8::V8::Initialize();

      {
        std::lock_guard<std::mutex> lck(workers_map_mutex_);
        for (int16_t i = 0; i < thr_count_; i++) {
          V8Worker *w = new V8Worker(
              platform.release(), handler_config, server_settings,
              function_name_, function_id_, handler_instance_id, user_prefix_,
              &latency_stats_, &curl_latency_stats_, ns_server_port_,
              num_vbuckets_, vb_seq_.get(), processed_bucketops_.get(),
              vb_locks_.get(), user_, domain_);

          LOG(logInfo) << "Init index: " << i << " V8Worker: " << w
                       << std::endl;
          workers_[i] = w;
        }

        delete handler_config;

        msg_priority_ = true;
        v8worker_init_done_ = true;
      }
      break;
    case v8_worker_opcode::oTracker:
      for (int16_t i = 0; i < thr_count_; i++) {
        bool enable = false;
        std::istringstream(worker_msg->header.metadata) >> std::boolalpha >>
            enable;
        if (enable) {
          workers_[i]->EnableTracker();
        } else {
          workers_[i]->DisableTracker();
        }
      }
      msg_priority_ = true;
      break;
    case v8_worker_opcode::oLoad:
      LOG(logDebug) << "Loading app code:" << RM(worker_msg->header.metadata)
                    << std::endl;
      for (int16_t i = 0; i < thr_count_; i++) {
        workers_[i]->V8WorkerLoad(worker_msg->header.metadata);

        LOG(logInfo) << "Load index: " << i << " V8Worker: " << workers_[i]
                     << std::endl;
      }
      msg_priority_ = true;
      break;

    case v8_worker_opcode::oGetLatencyStats:
      resp_msg_->msg = latency_stats_.ToString();
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oLatencyStats;
      msg_priority_ = true;
      break;

    case v8_worker_opcode::oGetCurlLatencyStats:
      resp_msg_->msg = curl_latency_stats_.ToString();
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oCurlLatencyStats;
      msg_priority_ = true;
      break;

    case v8_worker_opcode::oInsight:
      resp_msg_->msg = GetInsight();
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oCodeInsights;
      msg_priority_ = true;
      LOG(logDebug) << "Responding with insight " << resp_msg_->msg
                    << std::endl;
      break;

    case v8_worker_opcode::oGetOnDeployStats:
       SendOnDeployAck();
       break;

    case v8_worker_opcode::oGetFailureStats:
      LOG(logTrace) << "v8worker failure stats : " << GetFailureStats()
                    << std::endl;

      resp_msg_->msg.assign(GetFailureStats());
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oFailureStats;
      msg_priority_ = true;
      break;
    case v8_worker_opcode::oGetExecutionStats:
      resp_msg_->msg.assign(
          GetExecutionStats(workers_, (function_name_ == "test")));
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oExecutionStats;
      msg_priority_ = true;
      break;
    case v8_worker_opcode::oGetCompileInfo:
      LOG(logDebug) << "Compiling app code:" << RM(worker_msg->header.metadata)
                    << std::endl;
      compile_resp = workers_[0]->Compile(worker_msg->header.metadata);

      resp_msg_->msg.assign(compile_resp);
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oCompileInfo;
      msg_priority_ = true;
      break;
    case v8_worker_opcode::oGetLcbExceptions:
      for (const auto &w : workers_) {
        w.second->ListLcbExceptions(agg_lcb_exceptions);
      }

      estats.clear();

      for (auto const &entry : agg_lcb_exceptions) {
        estats[std::to_string(entry.first)] = entry.second;
      }

      resp_msg_->msg.assign(estats.dump());
      resp_msg_->msg_type = resp_msg_type::mV8_Worker_Config;
      resp_msg_->opcode = v8_worker_config_opcode::oLcbExceptions;
      msg_priority_ = true;
      break;
    default:
      LOG(logError) << "Opcode " << worker_msg->header.opcode
                    << "is not implemented for eV8Worker" << std::endl;
      break;
    }
    break;
  case event_type::eDCP:
    switch (getDCPOpcode(worker_msg->header.opcode)) {
    case dcp_opcode::oDelete:
      worker_index = current_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
      } else {
        LOG(logError) << "Delete event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case dcp_opcode::oMutation:
      worker_index = current_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
      } else {
        LOG(logError) << "Mutation event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case dcp_opcode::oNoOp:
      worker_index = current_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
      }
      break;
    case dcp_opcode::oDeleteCid:
      worker_index = current_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->UpdateDeletedCid(worker_msg);
        workers_[worker_index]->PushBack(std::move(worker_msg));
      }
      break;
    default:
      LOG(logError) << "Opcode " << worker_msg->header.opcode
                    << "is not implemented for eDCP" << std::endl;
      break;
    }
    break;
  case event_type::eFilter:
    switch (getFilterOpcode(worker_msg->header.opcode)) {
    case filter_opcode::oVbFilter: {
      worker_index = current_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        LOG(logInfo) << "Received filter event from Go "
                     << worker_msg->header.metadata << std::endl;
        auto worker = workers_[worker_index];
        int skip_ack = 0;
        auto [parsed_meta, status] = worker->ParseMetadataWithAck(
            worker_msg->header.metadata, skip_ack, true);
        if (status == kSuccess) {
          auto lck = worker->GetAndLockBucketOpsLock();
          auto last_processed_seq_no =
              worker->GetBucketopsSeqno(parsed_meta->vb);
          if (last_processed_seq_no < (parsed_meta->seq_num)) {
            worker->UpdateVbFilter(parsed_meta->vb, parsed_meta->seq_num);
          }
          worker->RemoveTimerPartition(parsed_meta->vb);
          lck.unlock();
          SendFilterAck(filter_opcode::oVbFilter,
                        resp_msg_type::mFilterAck, parsed_meta->vb,
                        last_processed_seq_no, skip_ack);
        }
      } else {
        LOG(logError) << "Filter event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
    } break;
    case filter_opcode::oProcessedSeqNo:
      worker_index = elected_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        LOG(logInfo) << "Received update processed seq_no event from Go "
                     << worker_msg->header.metadata << std::endl;
        current_partition_thr_map_[worker_msg->header.partition] = worker_index;
        auto [parsed_meta, status] =
            workers_[worker_index]->ParseMetadata(worker_msg->header.metadata);
        if (status == kSuccess) {
          auto lck = workers_[worker_index]->GetAndLockBucketOpsLock();
          workers_[worker_index]->UpdateBucketopsSeqnoLocked(
              parsed_meta->vb, parsed_meta->seq_num);
          workers_[worker_index]->AddTimerPartition(parsed_meta->vb);
        }
      }
      break;
    default:
      LOG(logError) << "Opcode " << worker_msg->header.opcode
                    << "is not implemented for filtering" << std::endl;
      break;
    }
    break;
  case event_type::ePauseConsumer: {
    pause_consumer_.store(true);
    std::unordered_map<int64_t, uint64_t> lps_map;
    LOG(logInfo) << "Received pause event from Go" << std::endl;
    for (int16_t idx = 0; idx < thr_count_; ++idx) {
      auto worker = workers_[idx];
      auto lck = worker->GetAndLockBucketOpsLock();
      worker->StopTimerScan();
      auto partitions = worker->GetPartitions();
      for (auto vb : partitions) {
        auto lps = worker->GetBucketopsSeqno(vb);
        worker->UpdateVbFilter(vb, std::numeric_limits<uint64_t>::max());
        worker->RemoveTimerPartition(vb);
        lps_map[vb] = lps;
      }
    }
    LOG(logInfo) << "Pause event processing complete. Sending ack" << std::endl;
    SendPauseAck(lps_map);
  } break;
  case event_type::eApp_Worker_Setting:
    switch (getAppWorkerSettingOpcode(worker_msg->header.opcode)) {
    case app_worker_setting_opcode::oLogLevel:
      SystemLog::setLogLevel(LevelFromString(worker_msg->header.metadata));
      LOG(logInfo) << "Configured log level: " << worker_msg->header.metadata
                   << std::endl;
      msg_priority_ = true;
      break;
    case app_worker_setting_opcode::oWorkerThreadCount:
      LOG(logInfo) << "Worker thread count: " << worker_msg->header.metadata
                   << std::endl;
      thr_count_ = int16_t(std::stoi(worker_msg->header.metadata));
      msg_priority_ = true;
      break;
    case app_worker_setting_opcode::oWorkerThreadMap:
      // TODO: Depricate oWorkerThreadMap because the new thread_map is being
      // computed in the Appworker
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
          elected_partition_thr_map_[p_id] = thread_id;
        }
      }
      msg_priority_ = true;
      break;
    case app_worker_setting_opcode::oTimerContextSize:
      timer_context_size = std::stol(worker_msg->header.metadata);
      LOG(logInfo) << "Setting timer_context_size to " << timer_context_size
                   << std::endl;
      msg_priority_ = true;
      break;

    case app_worker_setting_opcode::oVbMap: {
      payload = flatbuf::payload::GetPayload(
          (const void *)worker_msg->payload.payload.c_str());
      auto vb_map = payload->vb_map();
      std::vector<int64_t> vbuckets;
      for (size_t idx = 0; idx < vb_map->size(); ++idx) {
        vbuckets.push_back(vb_map->Get(idx));
      }

      // Alter the thread to workerVbs mapping every time we get a new workerVB
      // map
      std::sort(vbuckets.begin(), vbuckets.end());
      int32_t numVbs = vbuckets.size();
      int32_t chuncks = numVbs % thr_count_;
      int32_t vbsPerThread = numVbs / thr_count_;
      int32_t threadId = 0;
      for (int vbIdx = 0; vbIdx < numVbs;) {
        auto idx = vbIdx;
        for (; idx < vbIdx + vbsPerThread; idx++) {
          elected_partition_thr_map_[vbuckets[idx]] = threadId;
        }
        if (chuncks > 0) {
          elected_partition_thr_map_[vbuckets[idx]] = threadId;
          chuncks--;
          idx++;
        }
        threadId++;
        vbIdx = idx;
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

    case app_worker_setting_opcode::oWorkerMemQuota: {
      memory_quota_ = std::stoll(worker_msg->header.metadata);
      msg_priority_ = true;
      break;
    }
    default:
      LOG(logError) << "Opcode "
                    << static_cast<int>(
                           getAppWorkerSettingOpcode(worker_msg->header.opcode))
                    << "is not implemented for eApp_Worker_Setting"
                    << std::endl;
      break;
    }
    break;
  case event_type::eDebugger:
    switch (getDebuggerOpcode(worker_msg->header.opcode)) {
    case debugger_opcode::oDebuggerStart:
      worker_index = elected_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger start event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case debugger_opcode::oDebuggerStop:
      worker_index = elected_partition_thr_map_[worker_msg->header.partition];
      if (workers_[worker_index] != nullptr) {
        workers_[worker_index]->PushBack(std::move(worker_msg));
        msg_priority_ = true;
      } else {
        LOG(logError) << "Debugger stop event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    default:
      LOG(logError) << "Opcode " << worker_msg->header.opcode
                    << "is not implemented for eDebugger" << std::endl;
      break;
    }
    break;
  case event_type::eConfigChange:
    switch (getConfigOpcode(worker_msg->header.opcode)) {
    case config_opcode::oUpdateDisableFeatureList:
      for (auto &v8_worker : workers_) {
        std::unique_ptr<WorkerMessage> msg(new WorkerMessage);
        msg->header.event = worker_msg->header.event;
        msg->header.opcode = worker_msg->header.opcode;
        msg->header.metadata = worker_msg->header.metadata;
        v8_worker.second->PushFront(std::move(msg));
      }
      break;
    case config_opcode::oUpdateEncryptionLevel: {
      auto new_encryption_level = worker_msg->header.metadata;
      if (curr_encryption_level != new_encryption_level) {
        LOG(logInfo) << "Encryption level changed from "
                     << curr_encryption_level << " to " << new_encryption_level
                     << std::endl;
        if (new_encryption_level == "strict" &&
            curr_encryption_level == "control_or_off") {
          // No point in allowing timer store lcb handles to attempt store
          // operations
          LOG(logInfo) << "Disabling timer store lcb handle ops" << std::endl;
          for (auto &v8_worker : workers_)
            v8_worker.second->SetFailFastTimerScans();
        } else if (curr_encryption_level == "strict") {
          // New level will definitely be more relaxed.
          // Allow timer store lcb handles to attempt store operations
          // This is a noop if handles are already in their correct state
          LOG(logInfo)
              << "Re-enabling timer store lcb handle ops if disabled before"
              << std::endl;
          for (auto &v8_worker : workers_)
            v8_worker.second->ResetFailFastTimerScans();
        }
      }
      // TODO : Use the flags to repair lcb handles
      curr_encryption_level = new_encryption_level;
      break;
    }
    default:
      LOG(logError) << "Received invalid debugger opcode" << std::endl;
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
                msg->header.event = static_cast<uint8_t>(event_type::eInternal);
                msg->header.opcode =
                    static_cast<uint8_t>(internal_opcode::oScanTimer);
                v8_worker.second->PushFront(std::move(msg));
              }
            }
          }

          // Update the v8 heap size
          for (auto &v8_worker : worker->workers_) {
            if (!v8_worker.second->update_v8_heap_.load()) {
              v8_worker.second->update_v8_heap_.store(true);
              std::unique_ptr<WorkerMessage> msg(new WorkerMessage);
              msg->header.event = static_cast<uint8_t>(event_type::eInternal);
              msg->header.opcode =
                  static_cast<uint8_t>(internal_opcode::oUpdateV8HeapSize);
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
              msg->header.event = static_cast<uint8_t>(event_type::eInternal);
              msg->header.opcode =
                  static_cast<uint8_t>(internal_opcode::oRunGc);
              v8_worker.second->PushFront(std::move(msg));
            }
          }

          // Log Exception Insights ~ once a minute.
          worker->LogExceptionSummary();
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

void AppWorker::SendFilterAck(filter_opcode opcode, resp_msg_type msgtype, int vb_no,
                              int64_t seq_no, bool skip_ack) {
  std::ostringstream filter_ack;
  filter_ack << R"({"vb":)";
  filter_ack << vb_no << R"(, "seq":)";
  filter_ack << seq_no << R"(, "skip_ack":)";
  filter_ack << skip_ack << "}";

  resp_msg_->msg.assign(filter_ack.str());
  resp_msg_->msg_type = static_cast<resp_msg_type>(msgtype);
  resp_msg_->opcode = static_cast<v8_worker_config_opcode>(opcode);
  msg_priority_ = true;
  LOG(logInfo) << "vb: " << vb_no << " seqNo: " << seq_no
               << " skip_ack: " << skip_ack << " sending filter ack to Go"
               << std::endl;
}

std::vector<std::unordered_set<int64_t>>
AppWorker::PartitionVbuckets(const std::vector<int64_t> &vbuckets) const {
  std::vector<std::unordered_set<int64_t>> partitions(thr_count_);
  for (auto vb : vbuckets) {
    auto it = elected_partition_thr_map_.find(vb);
    if (it != end(elected_partition_thr_map_)) {
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
  resp_msg_->msg_type = resp_msg_type::mPauseAck;
  msg_priority_ = true;
}

/*
int main(int argc, char **argv) {

  if (argc < 16) {
    std::cerr
        << "Need at least 13 arguments: app_location, ipc_type, port, "
           "feedback_port"
           "worker_id, batch_size, feedback_batch_size, diag_dir, ipv4/6, "
           "breakpad_on, handler_uuid, user_prefix, ns_server_port, "
           "num_vbuckets, eventing_port, user, domain"
        << std::endl;
    return 2;
  }

  executable_img = argv[0];
  std::string app_location(argv[1]);
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
  std::string user = argv[15];
  std::string domain = argv[16];

  srand(static_cast<unsigned>(time(nullptr)));
  curl_global_init(CURL_GLOBAL_ALL);

  SetIPv6(ip_type == "ipv6");

  if (breakpad_on == "true") {
    setupBreakpad(diag_dir);
  }

  AppWorker *worker = AppWorker::GetAppWorker();

  worker->SetNsServerPort(ns_server_port);
  worker->SetNumVbuckets(num_vbuckets);
  worker->InitVbMapResources();
  worker->SetOwner(user, domain);

  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    worker->InitUDS(app_location, function_id, user_prefix, app_location,
                    Localhost(false), worker_id, batch_size,
                    feedback_batch_size, feedback_port, port);
  } else {
    worker->InitTcpSock(app_location, function_id, user_prefix, app_location,
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
*/

std::string AppWorker::GetInsight() {
  CodeInsight sum;
  for (int16_t i = 0; i < thr_count_; i++) {
    auto &entry = workers_[i]->GetInsight();
    sum.Accumulate(entry);
  }
  return sum.ToJSON();
}

bool AppWorker::shouldNotLogExceptionSummaryYet() {

  static std::chrono::steady_clock::time_point previous_time =
      std::chrono::steady_clock::now();
  static std::chrono::steady_clock::time_point current_time;

  current_time = std::chrono::steady_clock::now();

  bool needNotLogExceptionSummaryYet =
      (current_time - previous_time) < std::chrono::seconds{60};

  if (needNotLogExceptionSummaryYet) {

    return true;
  }

  previous_time = current_time;

  return false;
}

void AppWorker::LogExceptionSummary() {

  if (shouldNotLogExceptionSummaryYet()) {

    return;
  }

  ExceptionInsight summary;

  for (int16_t i = 0; i < thr_count_; i++) {
    auto &workerExceptionInsight = workers_[i]->GetExceptionInsight();
    summary.AccumulateAndClear(workerExceptionInsight);
  }

  if (thr_count_ > 0) {

    // Piggyback on the logging channel of the first worker thread to
    // log the exception summary.
    workers_[0]->GetExceptionInsight().LogExceptionSummary(summary);
  }
}
