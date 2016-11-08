#pragma once

#include "message.h"

#include <memory>
#include <queue>
#include <uv.h>

class Worker {
public:
  enum class ReadState { AppNameRead, AppMessageRead };

  Worker();
  ~Worker();

  void SendMessage(Message *msg);
  void FinishMessage();

  void AddToMsg(const char *data, size_t len);

  std::string GetMsg() const;
  std::string GetAppName() const;

  ReadState GetReadState() const;

  std::shared_ptr<uv_tcp_t> conn;

  void Activate();
  void Deactivate();
  bool IsActive() const;

protected:
  std::string app_name;
  std::string next_msg;

  ReadState state;
  bool active;
};

typedef std::map<uv_stream_t *, Worker> session_map_t;
