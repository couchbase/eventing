#include "../inc/client.h"

#include <iostream>
#include <string>
#include <uv.h>
#include <vector>

#include <rapidjson/document.h>

#define LISTEN_PORT 9091

const size_t MAX_BUF_SIZE = 4096;

typedef struct message_s {
  std::string header;
  std::string payload;
} message_t;

static void RouteMessage(message_t *parsed_message) {
    std::string command, metadata, message;
    rapidjson::Document header, payload;
    if (header.Parse(parsed_message->header.c_str()).HasParseError()) {
      std::cerr << "Failed to parse header" << '\n';
      return;
    }

    assert(header.IsObject());
    {
      rapidjson::Value &cmd = header["command"];
      rapidjson::Value &meta = header["metadata"];

      command.assign(cmd.GetString());
      metadata.assign(meta.GetString());
    }

    if (payload.Parse(parsed_message->payload.c_str()).HasParseError()) {
      std::cerr << "Failed to parse payload" << '\n';
      return;
    }

    assert(payload.IsObject());
    {
      rapidjson::Value &msg = payload["message"];
      message.assign(msg.GetString());
    }

    std::cout << "command:" << command << " metadata:" << metadata << " message:" << message << '\n';
}

std::vector<std::string> split(const std::string &message,
                               const std::string &delimiter) {
  std::vector<std::string> result;
  int start_index = 0, next_index;

  std::size_t index = message.find(delimiter);
  while (index != std::string::npos) {

    std::string entry = message.substr(start_index, index - start_index);
    result.push_back(entry);

    start_index = index + delimiter.length();
    index = message.find(delimiter, index + 1);
  }
  result.push_back(message.substr(start_index, message.length() - index));
  return result;
}

static message_t *ParseServerMessage(const std::string &message) {
  int header_size, payload_size;
  std::string delimiter = "\r\n";

  std::vector<std::string> tokens = split(message, delimiter);

  if (tokens.size() != 3) {
    return nullptr;
  } else {
    message_t *parsed_message = new (message_t);
    header_size = atoi(tokens[0].c_str());
    payload_size = atoi(tokens[1].c_str());
    std::string payload_header_chunk = tokens[2];

    parsed_message->header = payload_header_chunk.substr(0, header_size);
    parsed_message->payload =
        payload_header_chunk.substr(header_size, header_size + payload_size);
  return parsed_message;
  }
}

static void HandleChunks(const std::string &message) {
  std::string message_chunk;
  std::string chunk_delimiter ="~@@@";

  std::vector<std::string> tokens = split(message, chunk_delimiter);
  for (auto &token : tokens) {
    message_t *parsed_message = ParseServerMessage(token);
    if (parsed_message != nullptr) {
      RouteMessage(parsed_message);
      free(parsed_message);
    }
  }
}


static void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  std::vector<char>* read_buffer = AppWorker::GetAppWorker()->GetReadBuffer();
  *buf = uv_buf_init(read_buffer->data(), read_buffer->capacity());
}

AppWorker::AppWorker()
    : main_loop_running(false), conn_handle(nullptr), sending_name(true) {
  uv_loop_init(&main_loop);
  read_buffer.resize(MAX_BUF_SIZE);
}

AppWorker::~AppWorker() { uv_loop_close(&main_loop); }

void AppWorker::Init(const std::string &appname, const std::string &addr,
                     int port) {
  uv_tcp_init(&main_loop, &tcp_sock);
  uv_ip4_addr(addr.c_str(), port, &server_sock);

  this->app_name = appname;
  std::cout << "Starting worker for appname:" << appname << " port:" << port
            << '\n';

  AppWorker::GetAppWorker()->SetIndexRead(0);

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
    std::cout << "Client connected" << '\n';

    uv_read_start(conn->handle, alloc_buffer,
                  [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                    AppWorker::GetAppWorker()->OnRead(stream, nread, buf);
                  });

    conn_handle = conn->handle;

    std::shared_ptr<Message> msg = std::make_shared<Message>(Message(app_name));
    outgoing_queue.messages.push(msg);
    AppWorker::GetAppWorker()->WriteMessage(
        outgoing_queue.messages.back().get());
  } else {
    std::cerr << "Connection failed with error:" << uv_strerror(status) << '\n';
  }
}

void AppWorker::OnRead(uv_stream_t *stream, ssize_t nread,
                       const uv_buf_t *buf) {
  std::cout << "OnRead callback triggered, nread: " << nread << '\n';
  if (nread > 0) {
    next_message.append(buf->base);
    HandleChunks(next_message);
    next_message.clear();
  } else if (nread == 0) {
    next_message.clear();
  } else {
    if (nread != UV_EOF)
        std::cerr << "Read error" << uv_err_name(nread) << '\n';
    next_message.clear();
    uv_read_stop(stream);
  }
}

int AppWorker::GetIndexRead() { return index_read; }

void AppWorker::SetIndexRead(int index) { index_read = index; }

void AppWorker::WriteMessage(Message *msg) {
  uv_write(outgoing_queue.write_bufs.GetNewWriteBuf(), conn_handle,
           msg->GetBuf(), 1, [](uv_write_t *req, int status) {
             AppWorker::GetAppWorker()->OnWrite(req, status);
           });
}

void AppWorker::OnWrite(uv_write_t *req, int status) {
  outgoing_queue.messages.pop();
  outgoing_queue.write_bufs.Release();

  if (status == 0) {
    std::string client_msg("Hello from the client side");
    std::shared_ptr<Message> msg =
        std::make_shared<Message>(Message(client_msg));
    outgoing_queue.messages.push(msg);

    AppWorker::GetAppWorker()->WriteMessage(
        outgoing_queue.messages.back().get());
  }
}

std::vector<char> *AppWorker::GetReadBuffer() { return &read_buffer; }

AppWorker *AppWorker::GetAppWorker() {
  static AppWorker worker;
  return &worker;
}

int main() {
   AppWorker *worker = AppWorker::GetAppWorker();
   worker->Init("credit_score", "127.0.0.1", 9091);
}
