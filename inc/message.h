#pragma once

#include <queue>
#include <string>
#include <memory>
#include <uv.h>
#include <vector>

typedef std::vector<char> message_buffer;

struct MessageReq {
  uv_write_t request;
};

class Message {
public:
  Message(const std::string &msg);
  uv_buf_t *GetBuf();

protected:
  message_buffer buffer;
  uv_buf_t cached_buffer;
};

class WriteBufPool {
public:
  WriteBufPool();
  uv_write_t *GetNewWriteBuf();
  void Release();

protected:
  std::queue<std::unique_ptr<uv_write_t> > unused_wr_buf_pool;
  std::queue<std::unique_ptr<uv_write_t> > used_wr_buf_pool;
};

class MessagePool {
public:
  std::queue<std::shared_ptr<Message> > messages;
  WriteBufPool write_bufs;
};
