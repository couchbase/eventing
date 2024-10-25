#ifndef FUNCTION_HANDLER_H
#define FUNCTION_HANDLER_H

#include <atomic>
#include <map>
#include <thread>
#include <v8.h>

#include "messages.h"
#include "settings.h"
#include "v8worker2.h"

#include "../gen/flatbuf/header_v2_generated.h"

const int32_t max_unacked_count = 100;
const int32_t max_unacked_mem = 20 * 1024 * 1024;

class function_worker {
public:
  function_worker(
      std::unique_ptr<v8::Platform> &platform,
      std::shared_ptr<settings::cluster> cluster_setting,
      const std::unique_ptr<messages::worker_request> &worker_request,
      std::shared_ptr<communicator> comm)
      : cluster_setting_(cluster_setting), comm_(comm),
        app_details_(settings::parse_app_details(worker_request->payload)) {

    stats_ = std::make_shared<RuntimeStats>(app_details_->app_instance_id,
                                            app_details_->app_code);
    worker_queue_ =
        new BlockingDeque<std::unique_ptr<messages::worker_request>>();
    thread_exit_cond_ = false;
    event_gen_cond_ = false;
    identifier_ = worker_request->identifier;
    global_settings_ = std::make_shared<settings::global>();
    auto cpp_thread_count = app_details_->settings->cpp_thread_count;
    workers_.resize(cpp_thread_count);
    skipped_msgs_.resize(cpp_thread_count);

    for (int i = 0; i < cpp_thread_count; i++) {
      workers_[i] =
          new V8Worker2(i, stats_, comm_, platform.release(), cluster_setting,
                        global_settings_, app_details_);
      skipped_msgs_[i] = std::make_pair(0, 0);
    }
  };

  // For compilation purpose. Create temporary worker
  function_worker(std::string handlerID,
                  std::unique_ptr<v8::Platform> &platform,
                  std::shared_ptr<communicator> comm)
      : function_name_(handlerID), comm_(comm) {
    workers_.resize(1);
    workers_[0] = new V8Worker2(platform.release());
  };

  ~function_worker() {
    event_gen_cond_.store(true);
    thread_exit_cond_.store(true);
    if (event_gen_thr_.joinable()) {
      event_gen_thr_.join();
    }

    if (route_msg_thr_.joinable()) {
      route_msg_thr_.join();
    }
  }

  void init_event(std::unique_ptr<messages::worker_request> msg);
  std::string stats(messages::stats_opcode opcode);
  inline void push_msg(std::unique_ptr<messages::worker_request> msg,
                       bool priority = false) {
    if (priority) {
      worker_queue_->PushFront(std::move(msg));
      return;
    }
    worker_queue_->PushBack(std::move(msg));
  }

  inline void prepare_destroy_event() {
    // Stop the event generator loop
    event_gen_cond_.store(true);
    for (int index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      workers_[index]->StopTimerScan();
    }
  }

private:
  int start();
  inline std::string compile(std::string app_code) {
    return workers_[0]->CompileHandler(function_name_, app_code);
  };

  std::string function_name_;

  std::thread event_gen_thr_;
  std::thread route_msg_thr_;
  std::atomic<bool> event_gen_cond_{false};
  std::atomic<bool> thread_exit_cond_{false};

  const std::string get_execution_stats();
  const std::string get_failure_stats();
  const std::string get_latency_stats();
  const std::string get_curl_latency_stats();
  const std::string get_lcb_exception_stats();

  const std::shared_ptr<settings::cluster> cluster_setting_;
  std::shared_ptr<communicator> comm_;
  const std::shared_ptr<settings::app_details> app_details_;
  std::shared_ptr<settings::global> global_settings_;

  std::map<uint16_t, int> vb_map_;
  std::vector<V8Worker2 *> workers_;

  std::string get_acked_bytes() {
    std::ostringstream ss;
    for (int index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      auto [w_ack_count, w_ack_mem, dcp_events_executed] = workers_[index]->ack_bytes();
      auto [skipped_count, skipped_bytes] = skipped_msgs_[index];
      auto acked_count = w_ack_count + skipped_count;
      auto acked_bytes = w_ack_mem + skipped_bytes;
      ss << static_cast<uint8_t>(acked_bytes >> 24)
         << static_cast<uint8_t>(acked_bytes >> 16)
         << static_cast<uint8_t>(acked_bytes >> 8)
         << static_cast<uint8_t>(acked_bytes);

      ss << static_cast<uint8_t>(acked_count >> 24)
         << static_cast<uint8_t>(acked_count >> 16)
         << static_cast<uint8_t>(acked_count >> 8)
         << static_cast<uint8_t>(acked_count);

      ss << static_cast<uint8_t>(dcp_events_executed >> 56)
         << static_cast<uint8_t>(dcp_events_executed >> 48)
         << static_cast<uint8_t>(dcp_events_executed >> 40)
         << static_cast<uint8_t>(dcp_events_executed >> 32)
         << static_cast<uint8_t>(dcp_events_executed >> 24)
         << static_cast<uint8_t>(dcp_events_executed >> 16)
         << static_cast<uint8_t>(dcp_events_executed >> 8)
         << static_cast<uint8_t>(dcp_events_executed);

      skipped_msgs_[index].first = 0;
      skipped_msgs_[index].second = 0;
    }
    unacked_count = 0;
    unacked_mem = 0;
    return ss.str();
  }

  BlockingDeque<std::unique_ptr<messages::worker_request>> *worker_queue_;
  std::shared_ptr<RuntimeStats> stats_;
  std::vector<std::pair<int32_t, int32_t>> skipped_msgs_;
  std::vector<uint8_t> identifier_;
  int32_t unacked_mem{0}, unacked_count{0};
  void EventGenLoop();
  void RouteMessage();

  void dynamic_setting(std::unique_ptr<messages::worker_request> msg);
  void vb_setting(std::unique_ptr<messages::worker_request> msg);
  void lifecycle(std::unique_ptr<messages::worker_request> msg);
  void dcp_message(std::unique_ptr<messages::worker_request> msg);
  void global_settings(std::unique_ptr<messages::worker_request> msg);
};

#endif // function_handler
