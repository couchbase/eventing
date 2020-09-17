#ifndef SRC_INSPECTOR_AGENT_H_
#define SRC_INSPECTOR_AGENT_H_

#include "v8-inspector.h"
#include "v8.h"

#include <functional>
#include <memory>
#include <stddef.h>
#include <string>
#include <mutex>

using PostURLCallback = std::function<void(const std::string &url)>;

namespace inspector {

using namespace v8;

class InspectorSessionDelegate {
public:
  virtual ~InspectorSessionDelegate() = default;
  virtual bool WaitForFrontendMessageWhilePaused() = 0;
  virtual void
  SendMessageToFrontend(const v8_inspector::StringView &message) = 0;
};

class InspectorIo;
class CBInspectorClient;

class Agent {
public:
  Agent(std::string host_name, std::string host_name_display,
        std::string file_path, int port, PostURLCallback on_connect);
  ~Agent();

  // Create client_, may create io_ if option enabled
  bool Start(Isolate *isolate, Platform *platform, const char *path);
  // Stop and destroy io_
  void Stop();

  bool IsStarted() { return !!client_; }

  // IO thread started, and client connected
  bool IsConnected();

  void WaitForDisconnect();
  void FatalException(Local<Value> error, v8::Local<v8::Message> message);

  // These methods are called by the WS protocol and JS binding to create
  // inspector sessions.  The inspector responds by using the delegate to send
  // messages back.
  void Connect(InspectorSessionDelegate *delegate);
  void Disconnect();
  void Dispatch(const v8_inspector::StringView &message);
  InspectorSessionDelegate *delegate();

  void RunMessageLoop();
  bool enabled() { return enabled_; }
  void PauseOnNextJavascriptStatement(const std::string &reason);

  InspectorIo *io() { return io_.get(); }

  // Can only be called from the the main thread.
  bool StartIoThread();

  // Calls StartIoThread() from off the main thread.
  void RequestIoThreadStart();

private:
  std::mutex io_thread_mu_;
  std::unique_ptr<CBInspectorClient> client_;
  std::unique_ptr<InspectorIo> io_;
  PostURLCallback on_connect_;
  Platform *platform_;
  Isolate *isolate_;
  bool enabled_;
  int port_;
  std::string path_;
  std::string host_name_;
  std::string host_name_display_;
  std::string file_path_;
};

} // namespace inspector

#endif // SRC_INSPECTOR_AGENT_H_
