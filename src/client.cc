#include "../inc/client.h"
#include "../inc/commands.h"
// #include "../inc/binding.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <math.h>
#include <string>
#include <typeinfo>
#include <uv.h>
#include <vector>

#include <rapidjson/document.h>

#define LISTEN_PORT 9091

const size_t MAX_BUF_SIZE = 65536;

const int HEADER_FRAGMENT_SIZE = 4; // uint32
const int PAYLOAD_FRAGMENT_SIZE = 4; // uint32

std::ofstream cinfo_out;
std::ofstream cerror_out;

typedef struct message_s {
  std::string header;
  std::string payload;
} message_t;

typedef struct header_s {
    std::string command;
    std::string metadata;
} header_t;

int messages_processed(0);

char *getTime() {
  using std::chrono::system_clock;

  system_clock::time_point today = system_clock::now();
  std::time_t tt;
  tt = system_clock::to_time_t(today);
  return ctime(&tt);
}

static std::unique_ptr<header_t> ParseHeader(message_t *parsed_message) {
  std::string command, metadata;
  rapidjson::Document header;
  if (header.Parse(parsed_message->header.c_str()).HasParseError()) {
    cerror_out << getTime() << "Failed to parse header" << std::endl;
    return std::unique_ptr<header_t>(nullptr);
  }

  assert(header.IsObject());
  {
    rapidjson::Value &cmd = header["command"];
    rapidjson::Value &meta = header["metadata"];

    command.assign(cmd.GetString());
    metadata.assign(meta.GetString());
  }

  std::unique_ptr<header_t> parsed_header(new header_t);
  parsed_header->command = command;
  parsed_header->metadata = metadata;

  return parsed_header;
}

static void RouteMessageWithoutResponse(header_t *parsed_header,
                                        message_t *parsed_message) {
  std::string message;
  rapidjson::Document payload;

  if (payload.Parse(parsed_message->payload.c_str()).HasParseError()) {
    cerror_out << getTime() << "Failed to parse payload" << std::endl;
    return;
  }

  assert(payload.IsObject());
  {
    rapidjson::Value &msg = payload["message"];
    message.assign(msg.GetString());
  }

  switch (getCommandType(parsed_header->command)) {
  case cDCP:
    switch (getDCPCommandType(parsed_header->metadata)) {
    case mDelete:
    case mMutation:
    case mDCP_Cmd_Unknown:
      cerror_out << getTime() << "dcp_cmd_unknown encountered" << std::endl;
      break;
    }
    break;
  default:
    cerror_out << getTime() << "command:" << parsed_header->command
               << " sent to RouteMessageWithoutResponse and it isn't desirable"
               << std::endl;
  }
}

static std::string RouteMessageWithResponse(header_t *parsed_header,
                                            message_t *parsed_message) {
  std::string message, result;
  rapidjson::Document payload;

  if (payload.Parse(parsed_message->payload.c_str()).HasParseError()) {
    cerror_out << getTime() << "Failed to parse payload" << std::endl;
    return result;
  }

  assert(payload.IsObject());
  {
    rapidjson::Value &msg = payload["message"];
    message.assign(msg.GetString());
  }

  switch (getCommandType(parsed_header->command)) {
  case cV8_Worker:
    switch (getV8WorkerCommandType(parsed_header->metadata)) {
    case mDispose:
    case mInit:
        // v8_init();
    case mLoad:
    case mNew:
    case mTerminate:
    case mVersion:
    case mV8_Worker_Cmd_Unknown:
        cerror_out << getTime() << "worker_cmd_unknown encountered" << std::endl;
        break;
    }
    break;
  case cDCP:
    switch (getDCPCommandType(parsed_header->metadata)) {
    case mDelete:
    case mMutation:
    case mDCP_Cmd_Unknown:
        cerror_out << getTime() << "dcp_cmd_unknown encountered" << std::endl;
        break;
    }
    break;
  case cHTTP:
    switch (getHTTPCommandType(parsed_header->metadata)) {
    case mGet:
    case mPost:
    case mHTTP_Cmd_Unknown:
        cerror_out << getTime() << "http_cmd_unknown encountered" << std::endl;
        break;
    }
    break;
  case cV8_Debug:
    switch (getV8DebugCommandType(parsed_header->metadata)) {
    case mBacktrace:
    case mClear_Breakpoint:
    case mContinue:
    case mEvaluate:
    case mFrame:
    case mList_Breakpoints:
    case mLookup:
    case mSet_Breakpoint:
    case mSource:
    case mStart_Debugger:
    case mStop_Debugger:
    case mV8_Debug_Cmd_Unknown:
        cerror_out << getTime() << "v8_debug_cmd_unknown encountered" << std::endl;
        break;
    }
    break;
  case cUnknown:
    cinfo_out << "Unknown command" << std::endl;
    break;
  }

  result.assign("Hello from the client side\n");
  return result;
}

static std::unique_ptr<message_t>
ParseServerMessage(int encoded_header_size, int encoded_payload_size,
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
  std::vector<char>* read_buffer = AppWorker::GetAppWorker()->GetReadBuffer();
  *buf = uv_buf_init(read_buffer->data(), read_buffer->capacity());
}

AppWorker::AppWorker()
    : main_loop_running(false), conn_handle(nullptr) {
  uv_loop_init(&main_loop);
  read_buffer.resize(MAX_BUF_SIZE);
}

AppWorker::~AppWorker() { uv_loop_close(&main_loop); }

void AppWorker::Init(const std::string &appname, const std::string &addr,
                     int port) {
  uv_tcp_init(&main_loop, &tcp_sock);
  uv_ip4_addr(addr.c_str(), port, &server_sock);

  this->app_name = appname;
  cerror_out << "Starting worker for appname:" << appname << " port:" << port
             << std::endl;

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
    cerror_out << "Client connected" << std::endl;

    uv_read_start(conn->handle, alloc_buffer,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });

    conn_handle = conn->handle;
  } else {
    cerror_out << getTime() << "Connection failed with error:" << uv_strerror(status) << std::endl;
  }
}

int combineAsciiToInt(std::vector<int> *input) {
  int result = 0;
  for (int i = 0; i < input->size(); i++) {
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

  if (buf_base.length() < HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE) {
    next_message.assign(buf_base);
    return;
  } else {
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

    int message_size = HEADER_FRAGMENT_SIZE + PAYLOAD_FRAGMENT_SIZE +
                       encoded_header_size + encoded_payload_size;

    if (buf_base.length() < message_size) {
      next_message.assign(buf_base);
      return;
    } else {
      std::string chunk_to_parse = buf_base.substr(0, message_size);

      std::unique_ptr<message_t> parsed_message = ParseServerMessage(
          encoded_header_size, encoded_payload_size, chunk_to_parse);
      cerror_out << "header_size:" << encoded_header_size << " payload_size "
                 << encoded_payload_size
                 << " messages processed: " << messages_processed << std::endl;

      if (parsed_message) {
        std::unique_ptr<header_t> parsed_header =
            ParseHeader(parsed_message.get());

        if (parsed_header) {
          header_t *pheader = parsed_header.get();
          switch (getCommandType(pheader->command)) {
          case cDCP:
            RouteMessageWithoutResponse(parsed_header.get(),
                                        parsed_message.get());
            break;
          default:
            std::string result = RouteMessageWithResponse(parsed_header.get(),
                                                          parsed_message.get());

            if (!result.empty()) {
              // TODO: replace it with unique_ptr
              write_req_t *req = new (write_req_t);
              std::string response_buf("hello from the client side\n");
              req->buf = uv_buf_init((char *)response_buf.c_str(),
                                     response_buf.length());
              uv_write((uv_write_t *)req, stream, &req->buf, 1,
                       [](uv_write_t *req, int status) {
                         AppWorker::GetAppWorker()->OnWrite(req, status);
                       });
            }
          }
        }
      }
      if (buf_base.length() - message_size > 0) {
        buf_base.erase(0, message_size);
        AppWorker::GetAppWorker()->ParseValidChunk(stream, buf_base.length(),
                                                   buf_base.c_str());
      }
    }
  }
}

void AppWorker::OnRead(uv_stream_t *stream, ssize_t nread,
                       const uv_buf_t *buf) {
  cerror_out << "OnRead callback triggered, nread: " << nread << std::endl;
  if (nread > 0) {
    AppWorker::GetAppWorker()->ParseValidChunk(stream, nread, buf->base);
  } else if (nread == 0) {
    next_message.clear();
  } else {
    if (nread != UV_EOF)
      cerror_out << getTime() << "Read error, err code: " << uv_err_name(nread) << std::endl;
    AppWorker::GetAppWorker()->ParseValidChunk(stream, next_message.length(),
                                               next_message.c_str());
    next_message.clear();
    uv_read_stop(stream);
  }
}

void AppWorker::WriteMessage(Message *msg) {
  uv_write(outgoing_queue.write_bufs.GetNewWriteBuf(), conn_handle,
           msg->GetBuf(), 1, [](uv_write_t *req, int status) {
             AppWorker::GetAppWorker()->OnWrite(req, status);
           });
}

void AppWorker::OnWrite(uv_write_t *req, int status) {
  if (status) {
    cerror_out << getTime() << "Write error, err: " << uv_strerror(status)
               << std::endl;
  }

  write_req_t *wr = (write_req_t *) req;
  delete wr;
}

std::vector<char> *AppWorker::GetReadBuffer() { return &read_buffer; }

AppWorker *AppWorker::GetAppWorker() {
  static AppWorker worker;
  return &worker;
}

int main(int argc, char**argv) {
  std::string info_log_file = "client.info.log";
  std::string error_log_file = "client.error.log";

  cinfo_out.open(info_log_file.c_str());
  cerror_out.open(error_log_file.c_str());

  AppWorker *worker = AppWorker::GetAppWorker();
  int port = atoi(argv[1]);
  worker->Init("credit_score", "127.0.0.1", port);

  cinfo_out.close();
  cerror_out.close();
}
