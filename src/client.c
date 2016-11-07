#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#define LISTEN_PORT 9091

uv_loop_t *loop;

void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  buf->base = (char *)malloc(suggested_size);
  buf->len = suggested_size;
}

void on_read(uv_stream_t *server, ssize_t nread, const uv_buf_t *buf) {
  if (nread > 0) {
    fprintf(stdout, "from server: %s\n", buf->base);
  }
  if (nread < 0) {
    if (nread != UV_EOF) {
      fprintf(stderr, "read error\n");
    }
    uv_close((uv_handle_t *)server, NULL);
  }
}

void on_connect(uv_connect_t *req, int status) {
  if (status == -1) {
    fprintf(stderr, "error on connect\n");
    return;
  } else {
    fprintf(stdout, "Connected to server\n");
    uv_read_start(req->handle, alloc_buffer, on_read);
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
