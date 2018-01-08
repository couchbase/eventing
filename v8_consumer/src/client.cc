#include "../include/client.h"
#include "../include/breakpad.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <math.h>
#include <sstream>
#include <string>
#include <typeinfo>

static char const *global_program_name;
int messages_processed(0);

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

void AppWorker::RouteMessageWithResponse(header_t *parsed_header,
                                         message_t *parsed_message) {
  std::string key, val, doc_id, callback_fn, doc_ids_cb_fns, compile_resp;
  v8::Platform *platform;
  server_settings_t *server_settings;
  handler_config_t *handler_config;

  int worker_index;
  int64_t latency_buckets, agg_queue_size;
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
      server_settings->host_addr.assign(payload->curr_host()->str());
      server_settings->kv_host_port.assign(payload->kv_host_port()->str());
      server_settings->rbac_pass.assign(payload->rbac_pass()->str());
      server_settings->rbac_user.assign(payload->rbac_user()->str());

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
      LOG(logDebug) << "Loading app code:" << parsed_header->metadata
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
            lstats << "\"" << HIST_FROM << "\":" << agg_hgram[i];
          } else {
            lstats << "\"" << i * HIST_WIDTH << "\":" << agg_hgram[i];
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
      fstats << "{\"bucket_op_exception_count\":";
      fstats << bucket_op_exception_count << ", \"n1ql_op_exception_count\":";
      fstats << n1ql_op_exception_count << ", \"timeout_count\":";
      fstats << timeout_count << ", \"checkpoint_failure_count\":";
      fstats << checkpoint_failure_count << "}";

      resp_msg->msg.assign(fstats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oFailureStats;
      msg_priority = true;
      break;
    case oGetExecutionStats:
      estats.str(std::string());
      estats << "{\"on_update_success\":";
      estats << on_update_success << ", \"on_update_failure\":";
      estats << on_update_failure << ", \"on_delete_success\":";
      estats << on_delete_success << ", \"on_delete_failure\":";
      estats << on_delete_failure << ", \"non_doc_timer_create_failure\":";
      estats << non_doc_timer_create_failure
             << ", \"doc_timer_create_failure\":";
      estats << doc_timer_create_failure;

      if (workers.size() >= 1) {
        agg_queue_size = 0;
        for (const auto &w : workers) {
          agg_queue_size += w.second->QueueSize();
        }

        estats << ", \"agg_queue_size\":" << agg_queue_size;
      }
      estats << "}";

      resp_msg->msg.assign(estats.str());
      resp_msg->msg_type = mV8_Worker_Config;
      resp_msg->opcode = oExecutionStats;
      msg_priority = true;
      break;
    case oGetCompileInfo:
      LOG(logDebug) << "Compiling app code:" << parsed_header->metadata
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

        estats << "\"" << entry.first << "\":" << entry.second;

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
      LOG(logError) << "worker_opcode_unknown encountered" << std::endl;
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
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      }
      break;
    case oMutation:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      }
      break;
    default:
      LOG(logError) << "dcp_opcode_unknown encountered" << std::endl;
      break;
    }
    break;
  case eTimer:
    switch (getTimerOpcode(parsed_header->opcode)) {
    case oDocTimer:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      }
      break;
    case oCronTimer:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
      }
      break;
    default:
      break;
    }
    break;
  case eHTTP:
    switch (getHTTPOpcode(parsed_header->opcode)) {
    case oGet:
    case oPost:
    default:
      LOG(logError) << "http_opcode_unknown encountered" << std::endl;
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
      }
      break;
    case oDebuggerStop:
      worker_index = partition_thr_map[parsed_header->partition];
      if (workers[worker_index] != nullptr) {
        workers[worker_index]->Enqueue(parsed_header, parsed_message);
        msg_priority = true;
      }
      break;
    default:
      break;
    }
    break;
  default:
    LOG(logError) << "Unknown command" << std::endl;
    break;
  }
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

  messages_processed++;

  return parsed_message;
}

static void alloc_buffer(uv_handle_t *handle, size_t suggested_size,
                         uv_buf_t *buf) {
  std::vector<char> *read_buffer = AppWorker::GetAppWorker()->GetReadBuffer();
  *buf = uv_buf_init(read_buffer->data(), read_buffer->capacity());
}

AppWorker::AppWorker() : conn_handle(nullptr), main_loop_running(false) {
  uv_loop_init(&main_loop);
  read_buffer.resize(MAX_BUF_SIZE);
  resp_msg = new (resp_msg_t);
  msg_priority = false;
}

AppWorker::~AppWorker() { uv_loop_close(&main_loop); }

void AppWorker::InitUDS(const std::string &appname, const std::string &addr,
                        const std::string &worker_id, int bsize,
                        std::string uds_sock_path) {
  uv_pipe_init(&main_loop, &uds_sock, 0);

  app_name = appname;
  batch_size = bsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker with uds for appname:" << appname
               << " worker_id:" << worker_id << " batch_size:" << batch_size
               << " uds_path:" << uds_sock_path << std::endl;

  uv_pipe_connect(&conn, &uds_sock, uds_sock_path.c_str(),
                  [](uv_connect_t *conn, int status) {
                    AppWorker::GetAppWorker()->OnConnect(conn, status);
                  });

  if (main_loop_running == false) {
    uv_run(&main_loop, UV_RUN_DEFAULT);
    main_loop_running = true;
  }
}

void AppWorker::InitTcpSock(const std::string &appname, const std::string &addr,
                            const std::string &worker_id, int bsize, int port) {
  uv_tcp_init(&main_loop, &tcp_sock);
  uv_ip4_addr(addr.c_str(), port, &server_sock);

  app_name = appname;
  batch_size = bsize;
  messages_processed_counter = 0;

  LOG(logInfo) << "Starting worker for appname:" << appname
               << " worker_id:" << worker_id << " batch_size:" << batch_size
               << " port:" << port << std::endl;

  uv_tcp_connect(&conn, &tcp_sock, (const struct sockaddr *)&server_sock,
                 [](uv_connect_t *conn, int status) {
                   AppWorker::GetAppWorker()->OnConnect(conn, status);
                 });

  if (main_loop_running == false) {
    uv_run(&main_loop, UV_RUN_DEFAULT);
    main_loop_running = true;
  }
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

              write_req_t *req_size = new (write_req_t);
              req_size->buf = uv_buf_init(size, sizeof(uint32_t));
              uv_write((uv_write_t *)req_size, stream, &req_size->buf, 1,
                       [](uv_write_t *req_size, int status) {
                         AppWorker::GetAppWorker()->OnWrite(req_size, status);
                       });

              // Write payload to socket
              write_req_t *req_msg = new (write_req_t);
              std::string msg((const char *)builder.GetBufferPointer(),
                              builder.GetSize());
              req_msg->buf = uv_buf_init((char *)msg.c_str(), msg.length());
              uv_write((uv_write_t *)req_msg, stream, &req_msg->buf, 1,
                       [](uv_write_t *req_msg, int status) {
                         AppWorker::GetAppWorker()->OnWrite(req_msg, status);
                       });

              // Reset the values
              resp_msg->msg.clear();
              resp_msg->msg_type = 0;
              resp_msg->opcode = 0;
            }

            // Flush the aggregate item count in queues for all running V8
            // worker instances
            if (workers.size() >= 1) {
              int64_t agg_queue_size = 0;
              for (const auto &w : workers) {
                agg_queue_size += w.second->QueueSize();
              }

              std::ostringstream queue_stats;
              queue_stats << "{\"agg_queue_size\":";
              queue_stats << agg_queue_size << "}";

              flatbuffers::FlatBufferBuilder builder;
              auto msg_offset = builder.CreateString(queue_stats.str());
              auto r = flatbuf::response::CreateResponse(
                  builder, mV8_Worker_Config, oQueueSize, msg_offset);
              builder.Finish(r);

              uint32_t s = builder.GetSize();
              char *size = (char *)&s;

              // Write size of payload to socket
              write_req_t *req_size = new (write_req_t);
              req_size->buf = uv_buf_init(size, sizeof(uint32_t));
              uv_write((uv_write_t *)req_size, stream, &req_size->buf, 1,
                       [](uv_write_t *req_size, int status) {
                         AppWorker::GetAppWorker()->OnWrite(req_size, status);
                       });

              // Write payload to socket
              write_req_t *req_msg = new (write_req_t);
              std::string msg((const char *)builder.GetBufferPointer(),
                              builder.GetSize());
              req_msg->buf = uv_buf_init((char *)msg.c_str(), msg.length());
              uv_write((uv_write_t *)req_msg, stream, &req_msg->buf, 1,
                       [](uv_write_t *req_msg, int status) {
                         AppWorker::GetAppWorker()->OnWrite(req_msg, status);
                       });
            }

            messages_processed_counter = 0;
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

void AppWorker::OnWrite(uv_write_t *req, int status) {
  if (status) {
    LOG(logError) << "Write error, err: " << uv_strerror(status) << std::endl;
  }

  write_req_t *wr = (write_req_t *)req;
  delete wr;
}

std::vector<char> *AppWorker::GetReadBuffer() { return &read_buffer; }

AppWorker *AppWorker::GetAppWorker() {
  static AppWorker worker;
  return &worker;
}

int main(int argc, char **argv) {
  global_program_name = argv[0];

  if (argc < 6) {
    std::cerr << "Need at least 6 arguments: appname, ipc_type, port, "
                 "worker_id, batch_size, diag_dir"
              << std::endl;
    return 2;
  }

  srand(static_cast<unsigned>(time(nullptr)));

  if (isSSE42Supported()) {
    initCrcTable();
  }

  std::string appname(argv[1]);
  std::string ipc_type(argv[2]); // can be af_unix or af_inet

  std::string uds_sock_path;
  int port;

  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    uds_sock_path.assign(argv[3]);
  } else {
    port = atoi(argv[3]);
  }

  std::string worker_id(argv[4]);
  int batch_size = atoi(argv[5]);
  std::string diag_dir(argv[6]);

  setupBreakpad(diag_dir);

  setAppName(appname);
  setWorkerID(worker_id);
  AppWorker *worker = AppWorker::GetAppWorker();

  if (std::strcmp(ipc_type.c_str(), "af_unix") == 0) {
    worker->InitUDS(appname, "127.0.0.1", worker_id, batch_size, uds_sock_path);
  } else {
    worker->InitTcpSock(appname, "127.0.0.1", worker_id, batch_size, port);
  }
}
