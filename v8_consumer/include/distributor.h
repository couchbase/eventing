#ifndef EVENTING_DISTRIBUTOR_H
#define EVENTING_DISTRIBUTOR_H

#include <thread>

#include "blocking_deque.h"
#include "function_handler.h"
#include "messages.h"
#include "settings.h"

namespace distributor {
class distributor {
public:
  distributor() = default;
  virtual ~distributor() {}

  distributor(distributor &&) = delete;
  distributor(const distributor &) = delete;
  distributor &operator=(distributor &&) = delete;
  distributor &operator=(const distributor &) = delete;

  virtual void push_msg(std::unique_ptr<messages::worker_request>) = 0;
};

class eventingDist : public distributor {
public:
  eventingDist(std::shared_ptr<communicator> comm,
               std::shared_ptr<settings::cluster> cluster_setting);
  ~eventingDist() {
    if (thread_.joinable()) {
      thread_.join();
    }
    delete worker_queue_;
  }

  eventingDist(eventingDist &&) = delete;
  eventingDist(const eventingDist &) = delete;
  eventingDist &operator=(eventingDist &&) = delete;
  eventingDist &operator=(const eventingDist &) = delete;

  void push_msg(std::unique_ptr<messages::worker_request> wReq);

private:
  void start();

  std::unique_ptr<v8::Platform> platform_;
  std::shared_ptr<settings::cluster> cluster_setting_;
  std::shared_ptr<communicator> comm_;
  BlockingDeque<std::unique_ptr<messages::worker_request>> *worker_queue_;
  std::thread thread_;

  std::map<std::string, function_worker *> worker_list_;
  std::atomic<bool> terminator_{false};
};

}; // namespace distributor
#endif
