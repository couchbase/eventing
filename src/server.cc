#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#define LISTEN_PORT 9091
#define BACKLOG 100

uv_loop_t *loop;

typedef struct {
  uv_write_t req;
  uv_buf_t buf;
} write_req_t;

void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  buf->base = (char *)malloc(suggested_size);
  buf->len = suggested_size;
}

void on_write_end(uv_write_t *req, int status) {
  if (status == -1) {
    fprintf(stderr, "error on write_end\n");
    return;
  }
  write_req_t *wr = (write_req_t *) req;
  free(wr);
}

void on_new_connection(uv_stream_t *server, int status) {
  if (status < 0) {
    fprintf(stderr, "error creating new connection %s\n", uv_strerror(status));
    return;
  } else {
    fprintf(stdout, "created new connection\n");
  }

  uv_tcp_t *client = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
  uv_tcp_init(loop, client);

  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    fprintf(stdout, "Got one client connection request\n");

    char *message = "Hello from server";
    int len = strlen(message);

    char buffer[len + 1];

    write_req_t *req = (write_req_t *) malloc(sizeof(write_req_t));
    req->buf = uv_buf_init(buffer, sizeof(buffer));
    req->buf.len = len;
    req->buf.base = message;
    for (int i = 0; i < 20; i++) {
      uv_write((uv_write_t *)req, (uv_stream_t *)client, &req->buf, 1,
               on_write_end);
    }
  } else {
    uv_close((uv_handle_t *)client, NULL);
  }
}

int main(int argc, char **argv) {
  loop = uv_default_loop();

  uv_tcp_t server;
  uv_tcp_init(loop, &server);

  struct sockaddr_in addr;
  uv_ip4_addr("0.0.0.0", LISTEN_PORT, &addr);

  uv_tcp_bind(&server, (const struct sockaddr *)&addr, 0);
  int ret = uv_listen((uv_stream_t *)&server, BACKLOG, on_new_connection);
  if (ret) {
    fprintf(stderr, "Failed to listen %s\n", uv_strerror(ret));
    return 1;
  } else {
      fprintf(stdout, "Listening on port: %d\n", LISTEN_PORT);
  }
  uv_run(loop, UV_RUN_DEFAULT);

  return 0;
}
