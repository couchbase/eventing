#include "../inc/client.h"

#include <iostream>
#include <uv.h>
#include <vector>

#define LISTEN_PORT 9091

const size_t MAX_BUF_SIZE = 4096;

// uv_loop_t *loop;

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
  if (nread == UV_EOF) {
    uv_read_stop(stream);
  } else if (nread > 0) {
    std::cout << "Got from server:" << buf->base << '\n';
    next_message.append(buf->base);
  } else if (nread == 0) {
    next_message.clear();
  } else {
    std::cerr << "Error reading data" << '\n';
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

/*
void on_read(uv_stream_t *server, ssize_t nread, const uv_buf_t *buf) {
  if (nread > 0) {
      std::cout << "from server: "<<  buf->base << '\n';
  }
  if (nread < 0) {
    if (nread != UV_EOF) {
        std::cerr << "read error" << '\n';
    }
    uv_close((uv_handle_t *)server, NULL);
  }
}

void on_connect(uv_connect_t *req, int status) {
  if (status == 0) {
      std::cout << "Connected to server" << '\n';
    uv_read_start(req->handle, alloc_buffer, on_read);
  } else {
      std::cerr << "error on connect" << '\n';
  }
}

int main(int argc, char **argv) {
  loop = uv_default_loop();

  uv_tcp_t client;
  uv_tcp_init(loop, &client);

  struct sockaddr_in req_addr;
  uv_ip4_addr("0.0.0.0", LISTEN_PORT, &req_addr);

  uv_connect_t connect_req;
  uv_tcp_connect(&connect_req, &client, (const struct sockaddr *)&req_addr,
                 on_connect);
  uv_run(loop, UV_RUN_DEFAULT);

  return 0;
}
*/
