#include "../inc/client.h"

#include <iostream>
#include <string>
#include <uv.h>
#include <vector>

#include <rapidjson/document.h>

#define LISTEN_PORT 9091

const size_t MAX_BUF_SIZE = 4096;
const int PAYLOAD_HEADER_SIZE_FRAGMENT = 8;

typedef struct message_s {
  std::string header;
  std::string payload;
} message_t;

static std::string RouteMessage(message_t *parsed_message) {
    std::string command, metadata, message, result;
    rapidjson::Document header, payload;
    if (header.Parse(parsed_message->header.c_str()).HasParseError()) {
      std::cerr << "Failed to parse header" << '\n';
      return result;
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
      return result;
    }

    assert(payload.IsObject());
    {
      rapidjson::Value &msg = payload["message"];
      message.assign(msg.GetString());
    }

    std::cout << "command:" << command << " metadata:" << metadata << " message:" << message << '\n';

    result.assign("Hello from the client side\n");
    return result;
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

    /* std::shared_ptr<Message> msg = std::make_shared<Message>(Message(app_name));
    outgoing_queue.messages.push(msg);
    AppWorker::GetAppWorker()->WriteMessage(
        outgoing_queue.messages.back().get()); */
  } else {
    std::cerr << "Connection failed with error:" << uv_strerror(status) << '\n';
  }
}

void AppWorker::ParseValidChunk(uv_stream_t *stream, int nread, const char *buf) {
  if (nread > PAYLOAD_HEADER_SIZE_FRAGMENT) {

    std::string buf_base(buf);
    std::string substr = buf_base.substr(0, PAYLOAD_HEADER_SIZE_FRAGMENT);

    std::vector<std::string> tokens = split(substr, "\r\n");
    int header_size = atoi(tokens[0].c_str());
    int payload_size = atoi(tokens[1].c_str());

    int message_size =
        header_size + payload_size + PAYLOAD_HEADER_SIZE_FRAGMENT;

    if (nread >= message_size) {
      std::string chunk_to_parse = buf_base.substr(0, message_size);
      message_t *parsed_message = ParseServerMessage(chunk_to_parse);

      if (parsed_message != nullptr) {
        buf_base.erase(0, message_size);
        std::string result = RouteMessage(parsed_message);

        if (!result.empty()) {
            write_req_t *req = (write_req_t *) malloc(sizeof(write_req_t));
            std::string buf_base("Hello from the client side\n");
            req->buf = uv_buf_init((char *)buf_base.c_str(), buf_base.length());
            uv_write((uv_write_t *)req, stream, &req->buf, 1,
                     [](uv_write_t *req, int status) {
                       AppWorker::GetAppWorker()->OnWrite(req, status);
                     });
        }
          if (nread - message_size > 0) {
          AppWorker::GetAppWorker()->ParseValidChunk(stream, nread - message_size,
                                                     buf_base.c_str());
        }
      }

      free(parsed_message);

    } else {
      next_message.append(buf);
    }
  } else {
    next_message.append(buf);
  }
}

void AppWorker::OnRead(uv_stream_t *stream, ssize_t nread,
                       const uv_buf_t *buf) {
  std::cout << "OnRead callback triggered, nread: " << nread << '\n';
  if (nread > 0) {
    AppWorker::GetAppWorker()->ParseValidChunk(stream, nread, buf->base);
  } else if (nread == 0) {
    next_message.clear();
  } else {
    if (nread != UV_EOF)
      std::cerr << "Read error, err code: " << uv_err_name(nread) << '\n';
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
  /* outgoing_queue.messages.pop();
  outgoing_queue.write_bufs.Release();

  if (status == 0) {
    std::string client_msg("Hello from the client side\n");
    std::shared_ptr<Message> msg =
        std::make_shared<Message>(Message(client_msg));
    outgoing_queue.messages.push(msg);

    AppWorker::GetAppWorker()->WriteMessage(
        outgoing_queue.messages.back().get());
  } */


  if (status) {
      std::cerr << "Write error, err: " << uv_strerror(status) << '\n';
  }

  write_req_t *wr = (write_req_t *) req;
  free(wr);
}

std::vector<char> *AppWorker::GetReadBuffer() { return &read_buffer; }

AppWorker *AppWorker::GetAppWorker() {
  static AppWorker worker;
  return &worker;
}

int main(int argc, char**argv) {
   AppWorker *worker = AppWorker::GetAppWorker();
   int port = atoi(argv[1]);
   worker->Init("credit_score", "127.0.0.1", port);
}
