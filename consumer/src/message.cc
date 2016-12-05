#include "../include/message.h"

Message::Message(const std::string &msg) : buffer(msg.begin(), msg.end()) {
  buffer.push_back(0);

  cached_buffer.base = buffer.data();
  cached_buffer.len = buffer.size();
}

uv_buf_t *Message::GetBuf() { return &cached_buffer; }

WriteBufPool::WriteBufPool() {}

uv_write_t *WriteBufPool::GetNewWriteBuf() {
  if (unused_wr_buf_pool.empty()) {
    used_wr_buf_pool.push(
        std::move(std::unique_ptr<uv_write_t>(new uv_write_t)));
  } else {
    used_wr_buf_pool.push(std::move(
        std::unique_ptr<uv_write_t>(unused_wr_buf_pool.front().release())));
    unused_wr_buf_pool.pop();
  }

  return used_wr_buf_pool.back().get();
}

void WriteBufPool::Release() {
  unused_wr_buf_pool.push(std::move(
      std::unique_ptr<uv_write_t>(used_wr_buf_pool.front().release())));
  used_wr_buf_pool.pop();
}
