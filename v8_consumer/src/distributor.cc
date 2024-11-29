#include <future>
#include <libplatform/libplatform.h>
#include <thread>
#include <v8.h>

#include "../gen/flatbuf/header_v2_generated.h"
#include "distributor.h"

distributor::eventingDist::eventingDist(
    std::shared_ptr<communicator> comm,
    std::shared_ptr<settings::cluster> cluster_setting)
    : cluster_setting_(cluster_setting), comm_(comm) {

  platform_ = v8::platform::NewDefaultPlatform();
  v8::V8::InitializePlatform(platform_.get());
  v8::V8::Initialize();

  worker_queue_ =
      new BlockingDeque<std::unique_ptr<messages::worker_request>>();
  thread_ = std::thread(&eventingDist::start, this);
}

void distributor::eventingDist::push_msg(
    std::unique_ptr<messages::worker_request> task) {
  worker_queue_->PushBack(std::move(task));
}

void distributor::eventingDist::start() {
  while (!terminator_.load()) {
    std::unique_ptr<messages::worker_request> wRequest;
    if (!worker_queue_->PopFront(wRequest)) {
      continue;
    }

    switch (wRequest->event) {
    case messages::eInitEvent: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      switch (wRequest->opcode) {
      case messages::eInitHandler: {
        auto fw =
            new function_worker(platform_, cluster_setting_, wRequest, comm_);

        auto send_msg = std::unique_ptr<messages::worker_request>(
            new messages::worker_request);
        send_msg->event = wRequest->event;
        send_msg->opcode = wRequest->opcode;
        send_msg->identifier = std::move(wRequest->identifier);

        fw->init_event(std::move(wRequest));
        worker_list_[handlerID] = fw;
        comm_->send_message(send_msg, "", "", "");
      } break;

      case messages::eCompileHandler: {
        auto fw = new function_worker(handlerID, platform_, comm_);
        fw->init_event(std::move(wRequest));
      } break;

      case messages::eDebugHandlerStart:
      case messages::eDebugHandlerStop: {
        auto handlerID = messages::getHandlerID(wRequest->identifier);
        auto worker = worker_list_[handlerID];
        if (worker == nullptr) {
          continue;
        }
        worker->push_msg(std::move(wRequest));
      } break;

      case messages::eOnDeployHandler: {
        auto handlerID = messages::getHandlerID(wRequest->identifier);
        auto worker = worker_list_[handlerID];
        if (worker == nullptr) {
          continue;
        }
        worker->push_msg(std::move(wRequest));
      } break;
      }
    } break;

    case messages::eHandlerDynamicSettings: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }
      worker->push_msg(std::move(wRequest), true);
    } break;

    case messages::eVbSettings: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }
      worker->push_msg(std::move(wRequest));
    } break;

    case messages::eLifeCycleChange: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }

      switch (wRequest->opcode) {
      case messages::eDestroy: {
        worker->prepare_destroy_event();
        worker_list_.erase(handlerID);
      } break;

      default:
        break;
      }
      worker->push_msg(std::move(wRequest));
    } break;

    case messages::eStatsEvent: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }
      auto stats = worker->stats((messages::stats_opcode)wRequest->opcode);
      comm_->send_message(wRequest, "", "", stats);
    } break;

    case messages::eDcpEvent: {
      auto handlerID = messages::getHandlerIDForDcp(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }
      worker->push_msg(std::move(wRequest));
    } break;

    case messages::eGlobalConfigChange: {
      auto handlerID = messages::getHandlerID(wRequest->identifier);
      auto worker = worker_list_[handlerID];
      if (worker == nullptr) {
        continue;
      }
      worker->push_msg(std::move(wRequest), true);
    } break;

    default: {
    } break;
    }
  }
}
