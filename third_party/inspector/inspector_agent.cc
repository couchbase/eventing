#include "inspector_agent.h"
#include "inspector_io.h"
#include "libplatform/libplatform.h"
#include "v8-inspector.h"
#include "v8-platform.h"
#include "zlib.h"
#include "validate.h"

#include <cassert>
#include <string.h>
#include <vector>

#ifdef __POSIX__
#include <unistd.h> // setuid, getuid
#endif              // __POSIX__

namespace inspector {
namespace {
using namespace v8;

static uv_async_t start_io_thread_async;

class StartIoTask : public Task {
public:
  explicit StartIoTask(Agent *agent) : agent(agent) {}

  void Run() override { agent->StartIoThread(); }

private:
  Agent *agent;
};

std::unique_ptr<v8_inspector::StringBuffer>
ToProtocolString(Isolate *isolate, Local<Value> value) {
  if (value.IsEmpty() || value->IsNull() || value->IsUndefined() ||
      !value->IsString()) {
    return v8_inspector::StringBuffer::create(v8_inspector::StringView());
  }
  Local<String> string_value = Local<String>::Cast(value);
  size_t len = string_value->Length();
  std::basic_string<uint16_t> buffer(len, '\0');
  string_value->Write(isolate, &buffer[0], 0, len);
  return v8_inspector::StringBuffer::create(
      v8_inspector::StringView(buffer.data(), len));
}

// Called on the main thread.
void StartIoThreadAsyncCallback(uv_async_t *handle) {
  static_cast<Agent *>(handle->data)->StartIoThread();
}

void StartIoInterrupt(Isolate *isolate, void *agent) {
  static_cast<Agent *>(agent)->StartIoThread();
}

// Used in CBInspectorClient::currentTimeMS() below.
const int NANOS_PER_MSEC = 1000000;
const int CONTEXT_GROUP_ID = 1;

class ChannelImpl final : public v8_inspector::V8Inspector::Channel {
public:
  explicit ChannelImpl(v8_inspector::V8Inspector *inspector,
                       InspectorSessionDelegate *delegate)
      : delegate_(delegate) {
    session_ = inspector->connect(1, this, v8_inspector::StringView(), v8_inspector::V8Inspector::ClientTrustLevel::kFullyTrusted);
  }

  virtual ~ChannelImpl() {}

  void dispatchProtocolMessage(const v8_inspector::StringView &message) {
    session_->dispatchProtocolMessage(message);
  }

  bool waitForFrontendMessage() {
    return delegate_->WaitForFrontendMessageWhilePaused();
  }

  void schedulePauseOnNextStatement(const std::string &reason) {
    std::unique_ptr<v8_inspector::StringBuffer> buffer =
        Utf8ToStringView(reason);
    session_->schedulePauseOnNextStatement(buffer->string(), buffer->string());
  }

  InspectorSessionDelegate *delegate() { return delegate_; }

private:
  void
  sendResponse(int callId,
               std::unique_ptr<v8_inspector::StringBuffer> message) override {
    sendMessageToFrontend(message->string());
  }

  void sendNotification(
      std::unique_ptr<v8_inspector::StringBuffer> message) override {
    sendMessageToFrontend(message->string());
  }

  void flushProtocolNotifications() override {}

  void sendMessageToFrontend(const v8_inspector::StringView &message) {
    delegate_->SendMessageToFrontend(message);
  }

  InspectorSessionDelegate *const delegate_;
  std::unique_ptr<v8_inspector::V8InspectorSession> session_;
};

} // namespace

class CBInspectorClient : public v8_inspector::V8InspectorClient {
public:
  CBInspectorClient(Isolate *isolate, Platform *platform)
      : isolate_(isolate), platform_(platform), terminated_(false),
        running_nested_loop_(false) {
    client_ = v8_inspector::V8Inspector::create(isolate_, this);
  }

  void runMessageLoopOnPause(int context_group_id) override {
    validate(channel_ != nullptr);
    if (running_nested_loop_)
      return;
    terminated_ = false;
    running_nested_loop_ = true;
    while (!terminated_ && channel_->waitForFrontendMessage()) {
      while (platform::PumpMessageLoop(platform_, isolate_)) {
      }
    }
    terminated_ = false;
    running_nested_loop_ = false;
  }

  double currentTimeMS() override { return uv_hrtime() * 1.0 / NANOS_PER_MSEC; }

  void contextCreated(Local<Context> context, const std::string &name) {
    std::unique_ptr<v8_inspector::StringBuffer> name_buffer =
        Utf8ToStringView(name);
    v8_inspector::V8ContextInfo info(context, CONTEXT_GROUP_ID,
                                     name_buffer->string());
    client_->contextCreated(info);
  }

  void contextDestroyed(Local<Context> context) {
    client_->contextDestroyed(context);
  }

  void quitMessageLoopOnPause() override { terminated_ = true; }

  void connectFrontend(InspectorSessionDelegate *delegate) {
    validate(channel_ == nullptr);
    channel_ =
        std::unique_ptr<ChannelImpl>(new ChannelImpl(client_.get(), delegate));
  }

  void disconnectFrontend() {
    quitMessageLoopOnPause();
    channel_.reset();
  }

  void dispatchMessageFromFrontend(const v8_inspector::StringView &message) {
    validate(channel_ != nullptr);
    channel_->dispatchProtocolMessage(message);
  }

  Local<Context> ensureDefaultContextInGroup(int contextGroupId) override {
    return isolate_->GetCurrentContext();
  }

  void FatalException(Local<Value> error, Local<Message> message) {
    Local<Context> context = isolate_->GetCurrentContext();

    int script_id = message->GetScriptOrigin().ScriptId();

    Local<StackTrace> stack_trace = message->GetStackTrace();

    if (!stack_trace.IsEmpty() && stack_trace->GetFrameCount() > 0 &&
        script_id == stack_trace->GetFrame(isolate_, 0)->GetScriptId()) {
      script_id = 0;
    }

    const uint8_t DETAILS[] = "Uncaught";

    client_->exceptionThrown(
        context, v8_inspector::StringView(DETAILS, sizeof(DETAILS) - 1), error,
        ToProtocolString(isolate_, message->Get())->string(),
        ToProtocolString(isolate_, message->GetScriptResourceName())->string(),
        message->GetLineNumber(context).FromMaybe(0),
        message->GetStartColumn(context).FromMaybe(0),
        client_->createStackTrace(stack_trace), script_id);
  }

  ChannelImpl *channel() { return channel_.get(); }

private:
  Isolate *isolate_;
  Platform *platform_;
  bool terminated_;
  bool running_nested_loop_;
  std::unique_ptr<v8_inspector::V8Inspector> client_;
  std::unique_ptr<ChannelImpl> channel_;
};

Agent::Agent(std::string host_name, std::string host_name_display,
             std::string file_path, int port, PostURLCallback on_connect)
    : client_(nullptr), on_connect_(on_connect), platform_(nullptr),
      isolate_(nullptr), enabled_(false), port_(port), host_name_(host_name),
      host_name_display_(host_name_display), file_path_(file_path) {}

// Destructor needs to be defined here in implementation file as the header
// does not have full definition of some classes.
Agent::~Agent() {}

bool Agent::Start(Isolate *isolate, Platform *platform, const char *path) {
  path_ = path == nullptr ? "" : path;
  isolate_ = isolate;
  isolate->SetData(1, this);
  client_ = std::unique_ptr<CBInspectorClient>(
      new CBInspectorClient(isolate_, platform));
  client_->contextCreated(isolate_->GetCurrentContext(), "CB debugger context");
  platform_ = platform;
  auto result = uv_async_init(uv_default_loop(), &start_io_thread_async,
                              StartIoThreadAsyncCallback);
  validate(0 == result);
  start_io_thread_async.data = this;
  uv_unref(reinterpret_cast<uv_handle_t *>(&start_io_thread_async));

  // This will return false if listen failed on the inspector port.
  return StartIoThread();
}

bool Agent::StartIoThread() {
  std::lock_guard<std::mutex> l(io_thread_mu_);
  if (io_ != nullptr)
    return true;

  validate(client_ != nullptr);

  enabled_ = true;
  io_ = std::unique_ptr<InspectorIo>(new InspectorIo(
      isolate_, platform_, path_, host_name_, host_name_display_, true,
      file_path_, port_, on_connect_));
  if (!io_->Start()) {
    client_.reset();
    return false;
  }

  return true;
}

void Agent::Stop() {
  if (io_ != nullptr) {
    io_->Stop();
    io_.reset();
  }
}

void Agent::Connect(InspectorSessionDelegate *delegate) {
  enabled_ = true;
  client_->connectFrontend(delegate);
}

bool Agent::IsConnected() { return io_ && io_->IsConnected(); }

void Agent::WaitForDisconnect() {
  validate(client_ != nullptr);
  client_->contextDestroyed(isolate_->GetCurrentContext());
  if (io_ != nullptr) {
    io_->WaitForDisconnect();
  }
}

void Agent::FatalException(Local<Value> error, Local<Message> message) {
  if (!IsStarted())
    return;
  client_->FatalException(error, message);
  WaitForDisconnect();
}

void Agent::Dispatch(const v8_inspector::StringView &message) {
  validate(client_ != nullptr);
  client_->dispatchMessageFromFrontend(message);
}

void Agent::Disconnect() {
  validate(client_ != nullptr);
  client_->disconnectFrontend();
}

void Agent::RunMessageLoop() {
  validate(client_ != nullptr);
  client_->runMessageLoopOnPause(CONTEXT_GROUP_ID);
}

InspectorSessionDelegate *Agent::delegate() {
  validate(client_ != nullptr);
  ChannelImpl *channel = client_->channel();
  if (channel == nullptr)
    return nullptr;
  return channel->delegate();
}

void Agent::PauseOnNextJavascriptStatement(const std::string &reason) {
  ChannelImpl *channel = client_->channel();
  if (channel != nullptr)
    channel->schedulePauseOnNextStatement(reason);
}

void Agent::RequestIoThreadStart() {
  uv_async_send(&start_io_thread_async);
  std::unique_ptr<Task> curr_task =std::unique_ptr<Task>(new StartIoTask(this));
  platform_->CallOnWorkerThread(std::move(curr_task));
  isolate_->RequestInterrupt(StartIoInterrupt, this);
  uv_async_send(&start_io_thread_async);
}

} // namespace inspector
