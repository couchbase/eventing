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

uint64_t doc_timer_responses_sent(0);
uint64_t messages_parsed(0);

std::atomic<int64_t> e_app_worker_setting_lost = {0};
std::atomic<int64_t> e_dcp_lost = {0};
std::atomic<int64_t> e_debugger_lost = {0};
std::atomic<int64_t> e_timer_lost = {0};
std::atomic<int64_t> e_v8_worker_lost = {0};

std::atomic<int64_t> cron_timer_events_lost = {0};
std::atomic<int64_t> delete_events_lost = {0};
std::atomic<int64_t> doc_timer_events_lost = {0};
std::atomic<int64_t> mutation_events_lost = {0};

std::atomic<int64_t> uv_try_write_failure_counter = {0};

static void alloc_buffer(uv_handle_t *handle, size_t suggested_size,
                         uv_buf_t *buf) {
  std::vector<char> *read_buffer = AppWorker::GetAppWorker()->GetReadBuffer();
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

  std::unique_ptr<header_t> parsed_header(new header_t);

  if (ok) {
    parsed_header->event = header->event();
    parsed_header->opcode = header->opcode();
    parsed_header->partition = header->partition();

    parsed_header->metadata = header->metadata()->str();
  }

  return parsed_header;
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

std::vector<char> *AppWorker::GetReadBuffer() { return &read_buffer; }

void AppWorker::InitTcpSock(const std::string &appname, const std::string &addr,
                            const std::string &worker_id, int bsize, int fbsize,
                            int feedback_port, int port) {
  uv_tcp_init(&feedback_loop, &feedback_tcp_sock);
  uv_tcp_init(&main_loop, &tcp_sock);

  if (IsIPv6()) {
    uv_ip6_addr(addr.c_str(), feedback_port, &feedback_server_sock.sock6);
    uv_ip6_addr(addr.c_str(), port, &server_sock.sock6);
  } else {
    uv_ip4_addr(addr.c_str(), feedback_port, &feedback_server_sock.sock4);
    uv_ip4_addr(addr.c_str(), port, &server_sock.sock4);
  }

  app_name = appname;
  batch_size = bsize;
  feedback_batch_size = fbsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker with af_inet for appname:" << appname
               << " worker id:" << worker_id << " batch size:" << batch_size
               << " feedback batch size:" << fbsize
               << " feedback port:" << RS(feedback_port) << " port:" << RS(port)
               << std::endl;

  uv_tcp_connect(&feedback_conn, &feedback_tcp_sock,
                 (const struct sockaddr *)&feedback_server_sock,
                 [](uv_connect_t *feedback_conn, int status) {
                   AppWorker::GetAppWorker()->OnFeedbackConnect(feedback_conn,
                                                                status);
                 });

  uv_tcp_connect(&conn, &tcp_sock, (const struct sockaddr *)&server_sock,
                 [](uv_connect_t *conn, int status) {
                   AppWorker::GetAppWorker()->OnConnect(conn, status);
                 });

  std::thread f_thr(&AppWorker::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr = std::move(f_thr);

  std::thread m_thr(&AppWorker::StartMainUVLoop, this);
  main_uv_loop_thr = std::move(m_thr);
}

void AppWorker::InitUDS(const std::string &appname, const std::string &addr,
                        const std::string &worker_id, int bsize, int fbsize,
                        std::string feedback_sock_path,
                        std::string uds_sock_path) {
  uv_pipe_init(&feedback_loop, &feedback_uds_sock, 0);
  uv_pipe_init(&main_loop, &uds_sock, 0);

  app_name = appname;
  batch_size = bsize;
  feedback_batch_size = fbsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker with af_unix for appname:" << appname
               << " worker id:" << worker_id << " batch size:" << batch_size
               << " feedback batch size:" << fbsize
               << " feedback uds path:" << RS(feedback_sock_path)
               << " uds_path:" << RS(uds_sock_path) << std::endl;

  uv_pipe_connect(
      &feedback_conn, &feedback_uds_sock, feedback_sock_path.c_str(),
      [](uv_connect_t *feedback_conn, int status) {
        AppWorker::GetAppWorker()->OnFeedbackConnect(feedback_conn, status);
      });

  uv_pipe_connect(&conn, &uds_sock, uds_sock_path.c_str(),
                  [](uv_connect_t *conn, int status) {
                    AppWorker::GetAppWorker()->OnConnect(conn, status);
                  });

  std::thread f_thr(&AppWorker::StartFeedbackUVLoop, this);
  feedback_uv_loop_thr = std::move(f_thr);

  std::thread m_thr(&AppWorker::StartMainUVLoop, this);
  main_uv_loop_thr = std::move(m_thr);
}

void AppWorker::OnConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    LOG(logInfo) << "Client connected" << std::endl;

    uv_read_start(conn->handle, alloc_buffer,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });
    conn_handle = conn->handle;
  } else {
    LOG(logError) << "Connection failed with error:" << uv_strerror(status)
                  << std::endl;
  }
}

void AppWorker::OnFeedbackConnect(uv_connect_t *conn, int status) {
  if (status == 0) {
    LOG(logInfo) << "Client connected on feedback channel" << std::endl;

    uv_read_start(conn->handle, alloc_buffer,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });
    feedback_conn_handle = conn->handle;

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
    next_message.clear();
  } else {
    if (nread != UV_EOF) {
      LOG(logError) << "Read error, err code: " << uv_err_name(nread)
                    << std::endl;
    }
    AppWorker::GetAppWorker()->ParseValidChunk(stream, next_message.length(),
                                               next_message.c_str());
    next_message.clear();
    uv_read_stop(stream);
  }
}

void AppWorker::ParseValidChunk(uv_stream_t *stream, int nread,
                                const char *buf) {
  std::string buf_base;
  for (int i = 0; i < nread; i++) {
    buf_base += buf[i];
  }

  if (next_message.length() > 0) {
    buf_base = next_message + buf_base;
    next_message.clear();
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
      next_message.assign(buf_base);
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

          if (messages_processed_counter >= batch_size || msg_priority) {

            messages_processed_counter = 0;

            // Reset the message priority flag
            msg_priority = false;

            if (!resp_msg->msg.empty()) {
              flatbuffers::FlatBufferBuilder builder;

              auto msg_offset = builder.CreateString(resp_msg->msg.c_str());
              auto r = flatbuf::response::CreateResponse(
                  builder, resp_msg->msg_type, resp_msg->opcode, msg_offset);
              builder.Finish(r);

              uint32_t s = builder.GetSize();
              char *size = (char *)&s;
              FlushToConn(stream, size, SIZEOF_UINT32);

              // Write payload to socket
              std::string msg((const char *)builder.GetBufferPointer(),
                              builder.GetSize());
              FlushToConn(stream, (char *)msg.c_str(), msg.length());

              // Reset the values
              resp_msg->msg.clear();
              resp_msg->msg_type = 0;
              resp_msg->opcode = 0;
            }

            // Flush the aggregate item count in queues for all running V8
            // worker instances
            if (!workers.empty()) {
              int64_t agg_queue_size = 0, feedback_queue_size = 0,
                      agg_queue_memory = 0;
              for (const auto &w : workers) {
                agg_queue_size += w.second->worker_queue->Count();
                feedback_queue_size += w.second->doc_timer_queue->Count();
                agg_queue_memory += w.second->worker_queue->Size() +
                                    w.second->doc_timer_queue->Size();
              }

              std::ostringstream queue_stats;
              queue_stats << R"({"agg_queue_size":)";
              queue_stats << agg_queue_size << R"(, "feedback_queue_size":)";
              queue_stats << feedback_queue_size << R"(, "agg_queue_memory":)";
              queue_stats << agg_queue_memory << "}";

              flatbuffers::FlatBufferBuilder builder;
              auto msg_offset = builder.CreateString(queue_stats.str());
              auto r = flatbuf::response::CreateResponse(
                  builder, mV8_Worker_Config, oQueueSize, msg_offset);
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
    next_message.assign(buf_base);
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
      handler_config->cron_timers_per_doc = payload->cron_timers_per_doc();
      handler_config->curl_timeout = long(payload->curl_timeout());
      handler_config->dep_cfg.assign(payload->depcfg()->str());
      handler_config->execution_timeout = payload->execution_timeout();
      handler_config->fuzz_offset = payload->fuzz_offset();
      handler_config->lcb_inst_capacity = payload->lcb_inst_capacity();
      handler_config->enable_recursive_mutation =
          payload->enable_recursive_mutation();
      handler_config->skip_lcb_bootstrap = payload->skip_lcb_bootstrap();
      server_settings->checkpoint_interval = payload->checkpoint_interval();
      server_settings->eventing_dir.assign(payload->eventing_dir()->str());
      server_settings->eventing_port.assign(
          payload->curr_eventing_port()->str());
      server_settings->eventing_sslport.assign(
          payload->curr_eventing_sslport()->str());
      server_settings->host_addr.assign(payload->curr_host()->str());
      server_settings->kv_host_port.assign(payload->kv_host_port()->str());

      LOG(logDebug) << "Loading app:" << app_name << std::endl;

      v8::V8::InitializeICU();
      platform = v8::platform::CreateDefaultPlatform();
      v8::V8::InitializePlatform(platform);
      v8::V8::Initialize();

      for (int16_t i = 0; i < thr_count; i++) {
        V8Worker *w = new V8Worker(platform, handler_config, server_settings);

        LOG(logInfo) << "Init index: " << i << " V8Worker: " << w << std::endl;
        workers[i] = w;
      }

      delete handler_config;

      msg_priority = true;
      break;
    case oLoad:
      LOG(logDebug) << "Loading app code:" << RM(parsed_header->metadata)
                    << std::endl;
      for (int16_t i = 0; i < thr_count; i++) {
        workers[i]->V8WorkerLoad(parsed_header->metadata);

        LOG(logInfo) << "Load index: " << i << " V8Worker: " << workers[i]
                     << std::endl;
      }
      msg_priority = true;
      break;
    case oTerminate:
      break;
    case oGetSourceMap:
      resp_msg->msg = workers[0]->source_map_;
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oSourceMap;
      msg_priority = true;
      break;
    case oGetHandlerCode:
      resp_msg->msg = workers[0]->handler_code_;
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oHandlerCode;
      msg_priority = true;
      break;
    case oGetLatencyStats:
      latency_buckets = workers[0]->histogram->Buckets();
      agg_hgram.assign(latency_buckets, 0);
      for (const auto &w : workers) {
        worker_hgram = w.second->histogram->Hgram();
        for (std::string::size_type i = 0; i < worker_hgram.size(); i++) {
          agg_hgram[i] += worker_hgram[i];
        }
      }

      lstats.str(std::string());

      for (std::string::size_type i = 0; i < agg_hgram.size(); i++) {
        if (i == 0) {
          lstats << "{";
        }

        if (agg_hgram[i] > 0) {
          if ((i > 0) && (lstats.str().length() > 1)) {
            lstats << ",";
          }

          if (i == 0) {
            lstats << R"(")" << HIST_FROM << R"(":)" << agg_hgram[i];
          } else {
            lstats << R"(")" << i * HIST_WIDTH << R"(":)" << agg_hgram[i];
          }
        }

        if (i == agg_hgram.size() - 1) {
          lstats << "}";
        }
      }
      resp_msg->msg.assign(lstats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oLatencyStats;
      msg_priority = true;
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
      fstats << R"("delete_events_lost": )" << delete_events_lost << ",";
      fstats << R"("cron_timer_events_lost": )" << cron_timer_events_lost
             << ",";
      fstats << R"("doc_timer_events_lost": )" << doc_timer_events_lost << ",";
      fstats << R"("timestamp" : ")" << GetTimestampNow() << R"(")";
      fstats << "}";
      LOG(logTrace) << "v8worker failure stats : " << fstats.str() << std::endl;

      resp_msg->msg.assign(fstats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oFailureStats;
      msg_priority = true;
      break;
    case oGetExecutionStats:
      estats.str(std::string());
      estats << R"({"on_update_success":)";
      estats << on_update_success << R"(, "on_update_failure":)";
      estats << on_update_failure << R"(, "on_delete_success":)";
      estats << on_delete_success << R"(, "on_delete_failure":)";
      estats << on_delete_failure << R"(, "doc_timer_create_failure":)";
      estats << doc_timer_create_failure << R"(, "messages_parsed":)";
      estats << messages_parsed << R"(, "cron_timer_msg_counter":)";
      estats << cron_timer_msg_counter << R"(, "dcp_delete_msg_counter":)";
      estats << dcp_delete_msg_counter << R"(, "dcp_mutation_msg_counter":)";
      estats << dcp_mutation_msg_counter << R"(, "doc_timer_msg_counter":)";
      estats << doc_timer_msg_counter
             << R"(, "enqueued_cron_timer_msg_counter":)";
      estats << enqueued_cron_timer_msg_counter
             << R"(, "enqueued_dcp_delete_msg_counter":)";
      estats << enqueued_dcp_delete_msg_counter
             << R"(, "enqueued_dcp_mutation_msg_counter":)";
      estats << enqueued_dcp_mutation_msg_counter
             << R"(, "enqueued_doc_timer_msg_counter":)";
      estats << enqueued_doc_timer_msg_counter;
      estats << R"(, "doc_timer_responses_sent":)";
      estats << doc_timer_responses_sent;
      estats << R"(, "uv_try_write_failure_counter":)";
      estats << uv_try_write_failure_counter;

      if (!workers.empty()) {
        agg_queue_memory = agg_queue_size = feedback_queue_size = 0;
        for (const auto &w : workers) {
          agg_queue_size += w.second->worker_queue->Count();
          feedback_queue_size += w.second->doc_timer_queue->Count();
          agg_queue_memory += w.second->worker_queue->Size() +
                              w.second->doc_timer_queue->Size();
        }

        estats << R"(, "agg_queue_size":)" << agg_queue_size;
        estats << R"(, "feedback_queue_size":)" << feedback_queue_size;
        estats << R"(, "agg_queue_memory":)" << agg_queue_memory;
      }

      estats << R"(, "timestamp":")" << GetTimestampNow() << R"("})";
      LOG(logTrace) << "v8worker execution stats:" << estats.str() << std::endl;

      resp_msg->msg.assign(estats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oExecutionStats;
      msg_priority = true;
      break;
    case oGetCompileInfo:
      LOG(logDebug) << "Compiling app code:" << RM(parsed_header->metadata)
                    << std::endl;
      compile_resp = workers[0]->CompileHandler(parsed_header->metadata);

      resp_msg->msg.assign(compile_resp);
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oCompileInfo;
      msg_priority = true;
      break;
    case oGetLcbExceptions:
      for (const auto &w : workers) {
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

      resp_msg->msg.assign(estats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oLcbExceptions;
      msg_priority = true;
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
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        enqueued_dcp_delete_msg_counter++;
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      } else {
        LOG(logError) << "Delete event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++delete_events_lost;
      }
      break;
    case oMutation:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        enqueued_dcp_mutation_msg_counter++;
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
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
  case eTimer:
    switch (getTimerOpcode(parsed_header->opcode)) {
    case oDocTimer:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        enqueued_doc_timer_msg_counter++;
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      } else {
        LOG(logError) << "Doc timer event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++doc_timer_events_lost;
      }
      break;
    case oCronTimer:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        enqueued_cron_timer_msg_counter++;
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      } else {
        LOG(logError) << "Cron timer event lost: worker " << worker_index
                      << " is null" << std::endl;
        ++cron_timer_events_lost;
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
      setLogLevel(LevelFromString(parsed_header->metadata));
      LOG(logInfo) << "Configured log level: " << parsed_header->metadata
                   << std::endl;
      msg_priority = true;
      break;
    case oWorkerThreadCount:
      LOG(logInfo) << "Worker thread count: " << parsed_header->metadata
                   << std::endl;
      thr_count = int16_t(std::stoi(parsed_header->metadata));
      msg_priority = true;
      break;
    case oWorkerThreadMap:
      payload = flatbuf::payload::GetPayload(
          (const void *)parsed_message->payload.c_str());
      thr_map = payload->thr_map();
      partition_count = payload->partitionCount();
      LOG(logInfo) << "Request for worker thread map, size: " << thr_map->size()
                   << " partition_count: " << partition_count << std::endl;

      for (unsigned int i = 0; i < thr_map->size(); i++) {
        int16_t thread_id = thr_map->Get(i)->threadID();

        for (unsigned int j = 0; j < thr_map->Get(i)->partitions()->size();
             j++) {
          auto p_id = thr_map->Get(i)->partitions()->Get(j);
          partition_thr_map[p_id] = thread_id;
        }
      }
      msg_priority = true;
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
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
        msg_priority = true;
      } else {
        LOG(logError) << "Debugger start event lost: worker " << worker_index
                      << " is null" << std::endl;
      }
      break;
    case oDebuggerStop:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
        msg_priority = true;
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
  if (!main_loop_running) {
    uv_run(&main_loop, UV_RUN_DEFAULT);
    main_loop_running = true;
  }
}

void AppWorker::StartFeedbackUVLoop() {
  if (!feedback_loop_running) {
    uv_run(&feedback_loop, UV_RUN_DEFAULT);
    feedback_loop_running = true;
  }
}

void AppWorker::WriteResponses() {
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  while (!thread_exit_cond.load()) {

    if (!workers.empty()) {

      for (const auto &w : workers) {
        auto timer_entry_count = w.second->doc_timer_queue->Count();

        if (timer_entry_count > 0) {

          LOG(logTrace) << "Worker: " << w.second
                        << " doc timer queue size: " << timer_entry_count
                        << std::endl;

          for (auto i = 0; i <= timer_entry_count / feedback_batch_size; i++) {

            std::vector<uv_buf_t> responses;
            int bytes_to_write = 0;

            for (auto j = 0; j < feedback_batch_size; j++) {

              if ((i * feedback_batch_size + j) < timer_entry_count) {

                auto doc_timer_msg = w.second->doc_timer_queue->Pop();

                flatbuffers::FlatBufferBuilder builder;
                auto msg_offset =
                    builder.CreateString(doc_timer_msg.timer_entry);

                auto r = flatbuf::response::CreateResponse(
                    builder, mDoc_Timer_Response, timerResponse, msg_offset);
                builder.Finish(r);

                LOG(logTrace)
                    << "Worker: " << w.second << " flushing doc timer entry: "
                    << doc_timer_msg.timer_entry << std::endl;

                uint32_t s = builder.GetSize();
                char *size = new char[SIZEOF_UINT32];
                char *temp_store = (char *)&s;

                strncpy(size, temp_store, SIZEOF_UINT32);

                auto buf_size = uv_buf_init(size, SIZEOF_UINT32);
                bytes_to_write += buf_size.len;
                responses.push_back(buf_size);

                std::string msg((const char *)builder.GetBufferPointer(),
                                builder.GetSize());

                char *msg_entry = new char[msg.size() + 1];

                // Could have leveraged strcpy or strncpy, but for some reason
                // copied over buffer had invalid entries.
                for (int k = 0; k < int(msg.size()); k++) {
                  msg_entry[k] = msg[k];
                }
                msg_entry[msg.size()] = '\0';

                auto buf_msg = uv_buf_init(msg_entry, msg.length());
                bytes_to_write += buf_msg.len;
                responses.push_back(buf_msg);

                builder.Clear();
              }
            }

            if (responses.size() > 0) {

              int bytes_written = UV_EAGAIN;

              do {
                bytes_written = uv_try_write(
                    feedback_conn_handle, responses.data(), responses.size());

                if (bytes_written == bytes_to_write) {
                  doc_timer_responses_sent += responses.size() / 2;
                  responses.clear();
                  for (const uv_buf_t &entry : responses) {
                    delete entry.base;
                  }
                }

                // When a portion of supplied uv_buf_t's wasn't flushed
                // successfully on the socket. uv_try_write is retried for only
                // those uv_buf_t's that weren't flushed successfully.
                if ((bytes_written < bytes_to_write) && (bytes_written > 0)) {
                  LOG(logInfo)
                      << "Doc timer feedback bytes_written: " << bytes_written
                      << " bytes_to_write: " << bytes_to_write << std::endl;
                  bytes_to_write -= bytes_written;

                  uv_try_write_failure_counter++;

                  std::vector<uv_buf_t> temp_buffers;
                  int buffer_sizes_so_far = 0;
                  int index = -1;

                  // Check at what std::vector index, entries were written
                  // successfully
                  for (auto const &buffer : responses) {
                    if ((int(buffer.len) + buffer_sizes_so_far) >
                        bytes_written) {

                      std::string original_data(buffer.base, buffer.len);
                      std::string pending_data(
                          original_data, (bytes_written - buffer_sizes_so_far));
                      uv_buf_t temp_buffer = uv_buf_init(
                          (char *)pending_data.c_str(), pending_data.size());
                      temp_buffers.push_back(temp_buffer);
                      ++index;
                      doc_timer_responses_sent += index / 2;
                      break;
                    } else {
                      buffer_sizes_so_far += buffer.len;
                      ++index;
                    }
                  }

                  for (std::vector<int>::size_type i = index + 1;
                       i != responses.size(); i++) {
                    temp_buffers.push_back(responses[i]);
                  }
                  responses.swap(temp_buffers);
                }

              } while ((bytes_written == UV_EAGAIN) || (responses.size() > 0));

              std::vector<uv_buf_t>().swap(responses);
            }
          }
        } else {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

AppWorker::AppWorker() : feedback_conn_handle(nullptr), conn_handle(nullptr) {
  thread_exit_cond.store(false);
  uv_loop_init(&feedback_loop);
  uv_loop_init(&main_loop);
  feedback_loop_async.data = (void *)&feedback_loop;
  main_loop_async.data = (void *)&main_loop;
  uv_async_init(&feedback_loop, &feedback_loop_async, AppWorker::StopUvLoop);
  uv_async_init(&main_loop, &main_loop_async, AppWorker::StopUvLoop);

  read_buffer.resize(MAX_BUF_SIZE);
  resp_msg = new (resp_msg_t);
  msg_priority = false;

  feedback_loop_running = false;
  main_loop_running = false;

  std::thread w_thr(&AppWorker::WriteResponses, this);
  write_responses_thr = std::move(w_thr);
}

AppWorker::~AppWorker() {
  if (feedback_uv_loop_thr.joinable()) {
    feedback_uv_loop_thr.join();
  }

  if (main_uv_loop_thr.joinable()) {
    main_uv_loop_thr.join();
  }

  if (write_responses_thr.joinable()) {
    write_responses_thr.join();
  }

  uv_loop_close(&feedback_loop);
  uv_loop_close(&main_loop);
}

void AppWorker::ReadStdinLoop() {
  auto functor = [](AppWorker *worker, uv_async_t *async1, uv_async_t *async2) {
    std::string token;
    while (!std::cin.eof()) {
        std::cin.clear();
        std::getline(std::cin, token);
    }
    worker->thread_exit_cond.store(true);
    uv_async_send(async1);
    uv_async_send(async2);
  };
  std::thread thr(functor, this, &feedback_loop_async, &main_loop_async);
  stdin_read_thr = std::move(thr);
}

void AppWorker::StopUvLoop(uv_async_t *async) {
  uv_loop_t *handle = (uv_loop_t *)async->data;
  uv_stop(handle);
}

int main(int argc, char **argv) {

  if (argc < 8) {
    std::cerr
        << "Need at least 10 arguments: appname, ipc_type, port, feedback_port"
           "worker_id, batch_size, feedback_batch_size, diag_dir, ipv4/6, "
           "breakpad_on"
        << std::endl;
    return 2;
  }

  SetIPv6(std::string(argv[9]) == "ipv6");

  srand(static_cast<unsigned>(time(nullptr)));

  if (isSSE42Supported()) {
    initCrcTable();
  }

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

  curl_global_init(CURL_GLOBAL_ALL);

  setAppName(appname);
  setWorkerID(worker_id);
  AppWorker *worker = AppWorker::GetAppWorker();
  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    worker->InitUDS(appname, Localhost(false), worker_id, batch_size,
                    feedback_batch_size, feedback_sock_path, uds_sock_path);
  } else {
    worker->InitTcpSock(appname, Localhost(false), worker_id, batch_size,
                        feedback_batch_size, feedback_port, port);
  }
  worker->ReadStdinLoop();
  worker->stdin_read_thr.join();
  worker->main_uv_loop_thr.join();
  worker->feedback_uv_loop_thr.join();

  curl_global_cleanup();
}
