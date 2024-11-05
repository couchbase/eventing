#include <sstream>

#include "function_handler.h"
#include "messages.h"

const int64_t process_mem_limit_byte = 90'000'000;

int function_worker::start() {
  for (int i = 0; i < app_details_->settings->cpp_thread_count; i++) {
    workers_[i]->Start();
  }

  std::thread event_thr(&function_worker::EventGenLoop, this);
  event_gen_thr_ = std::move(event_thr);

  std::thread r_thr(&function_worker::RouteMessage, this);
  route_msg_thr_ = std::move(r_thr);

  LOG(logInfo) << "function_woker:start[" << app_details_->location->bucket_name
               << "/" << app_details_->location->scope_name << "/"
               << app_details_->location->app_name << "] started " << std::endl;
  return 0;
}

void function_worker::init_event(
    std::unique_ptr<messages::worker_request> msg) {
  switch (msg->opcode) {
  case messages::eInitHandler: {
    start();
    comm_->send_message(msg, "", "", "");
  } break;
  case messages::eCompileHandler: {
    auto app_code = messages::get_value(msg->payload);
    auto result = compile(app_code);
    comm_->send_message(msg, "", "", result);
  } break;
  case messages::eDebugHandlerStart:
  case messages::eDebugHandlerStop:
    workers_[0]->push_msg(std::move(msg), true);
    break;
  case messages::eOnDeployHandler:
    // We choose the first v8 worker of this function handler to perform OnDeploy
    workers_[0]->push_msg(std::move(msg));
    break;
  }
}

void function_worker::dynamic_setting(
    std::unique_ptr<messages::worker_request> msg) {
  switch (msg->opcode) {
  case messages::eLogLevelChange:
    break;

  case messages::eTimeContextSize:
    auto value = messages::get_value(msg->payload);
    auto timerContextSize =
        (uint32_t) static_cast<unsigned char>(value[0]) << 24 |
        (uint32_t) static_cast<unsigned char>(value[1]) << 16 |
        (uint32_t) static_cast<unsigned char>(value[2]) << 8 |
        (uint32_t) static_cast<unsigned char>(value[3]);
    app_details_->settings->timer_context_size = timerContextSize;
    for (int index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      std::unique_ptr<messages::worker_request> refresh_msg(
          new messages::worker_request);
      refresh_msg->event = msg->event;
      refresh_msg->opcode = msg->opcode;
      workers_[index]->push_msg(std::move(refresh_msg), true);
    }
    break;
  }
}

void function_worker::vb_setting(
    std::unique_ptr<messages::worker_request> msg) {
  auto extras = messages::get_extras(msg->payload);
  switch (msg->opcode) {
  case messages::eVbMap: {
    int index = 0;

    for (size_t i = 0; i < extras.length();) {
      auto vbMapLength = (uint16_t) static_cast<unsigned char>(extras[i]) << 8 |
                         (uint16_t) static_cast<unsigned char>(extras[i + 1]);
      i = i + 2;

      for (auto count = 0; count < vbMapLength; count++) {
        uint16_t vb = (uint16_t) static_cast<unsigned char>(extras[i]) << 8 |
                      (uint16_t) static_cast<unsigned char>(extras[i + 1]);
        i = i + 2;
        vb_map_[vb] = index;
      }

      index++;
    }
  } break;

  case messages::eAddVb: {
    auto vb = (uint16_t) static_cast<unsigned char>(extras[0]) << 8 |
              (uint16_t) static_cast<unsigned char>(extras[1]);
    auto seq = (uint64_t) static_cast<unsigned char>(extras[2]) << 56 |
               (uint64_t) static_cast<unsigned char>(extras[3]) << 48 |
               (uint64_t) static_cast<unsigned char>(extras[4]) << 40 |
               (uint64_t) static_cast<unsigned char>(extras[5]) << 32 |
               (uint64_t) static_cast<unsigned char>(extras[6]) << 24 |
               (uint64_t) static_cast<unsigned char>(extras[7]) << 16 |
               (uint64_t) static_cast<unsigned char>(extras[8]) << 8 |
               (uint64_t) static_cast<unsigned char>(extras[9]);
    auto vbuuid = (uint64_t) static_cast<unsigned char>(extras[10]) << 56 |
                  (uint64_t) static_cast<unsigned char>(extras[11]) << 48 |
                  (uint64_t) static_cast<unsigned char>(extras[12]) << 40 |
                  (uint64_t) static_cast<unsigned char>(extras[13]) << 32 |
                  (uint64_t) static_cast<unsigned char>(extras[14]) << 24 |
                  (uint64_t) static_cast<unsigned char>(extras[15]) << 16 |
                  (uint64_t) static_cast<unsigned char>(extras[16]) << 8 |
                  (uint64_t) static_cast<unsigned char>(extras[17]);

    auto index = vb_map_[vb];
    workers_[index]->update_seq_for_vb(vb, vbuuid, seq);
  } break;

  case messages::eFilterVb: {
    auto vb = (uint16_t) static_cast<unsigned char>(extras[0]) << 8 |
              (uint16_t) static_cast<unsigned char>(extras[1]);

    auto index = vb_map_[vb];
    vb_map_.erase(vb);
    auto [p_seq, p_vbuuid, executing] =
        workers_[index]->AddFilterEvent(vb, std::move(msg->payload));

    if (executing) {
      LOG(logInfo) << "function_woker:vbFilter[" << app_details_->location->bucket_name
               << "/" << app_details_->location->scope_name << "/"
               << app_details_->location->app_name << ":" << index << "]" << " vb: " << vb << "still executing" << " adding callback function" << std::endl;
      msg->payload.resize(18);
      msg->payload[0] = static_cast<uint8_t>(vb >> 8);
      msg->payload[1] = static_cast<uint8_t>(vb);
      msg->payload[2] = static_cast<uint8_t>(p_seq >> 56);
      msg->payload[3] = static_cast<uint8_t>(p_seq >> 48);
      msg->payload[4] = static_cast<uint8_t>(p_seq >> 40);
      msg->payload[5] = static_cast<uint8_t>(p_seq >> 32);
      msg->payload[6] = static_cast<uint8_t>(p_seq >> 24);
      msg->payload[7] = static_cast<uint8_t>(p_seq >> 16);
      msg->payload[8] = static_cast<uint8_t>(p_seq >> 8);
      msg->payload[9] = static_cast<uint8_t>(p_seq);
      msg->payload[10] = static_cast<uint8_t>(p_vbuuid >> 56);
      msg->payload[11] = static_cast<uint8_t>(p_vbuuid >> 48);
      msg->payload[12] = static_cast<uint8_t>(p_vbuuid >> 40);
      msg->payload[13] = static_cast<uint8_t>(p_vbuuid >> 32);
      msg->payload[14] = static_cast<uint8_t>(p_vbuuid >> 24);
      msg->payload[15] = static_cast<uint8_t>(p_vbuuid >> 16);
      msg->payload[16] = static_cast<uint8_t>(p_vbuuid >> 8);
      msg->payload[17] = static_cast<uint8_t>(p_vbuuid);

      // assign the sending back of msg to v8worker
      workers_[index]->push_msg(std::move(msg), true);
      return;
    }

    std::ostringstream ss;
    ss << static_cast<uint8_t>(vb >> 8) << static_cast<uint8_t>(vb);
    ss << static_cast<uint8_t>(p_seq >> 56) << static_cast<uint8_t>(p_seq >> 48)
       << static_cast<uint8_t>(p_seq >> 40) << static_cast<uint8_t>(p_seq >> 32)
       << static_cast<uint8_t>(p_seq >> 24) << static_cast<uint8_t>(p_seq >> 16)
       << static_cast<uint8_t>(p_seq >> 8) << static_cast<uint8_t>(p_seq);
    ss << static_cast<uint8_t>(p_vbuuid >> 56)
       << static_cast<uint8_t>(p_vbuuid >> 48)
       << static_cast<uint8_t>(p_vbuuid >> 40)
       << static_cast<uint8_t>(p_vbuuid >> 32)
       << static_cast<uint8_t>(p_vbuuid >> 24)
       << static_cast<uint8_t>(p_vbuuid >> 16)
       << static_cast<uint8_t>(p_vbuuid >> 8) << static_cast<uint8_t>(p_vbuuid);

    flatbuffers::FlatBufferBuilder builder;
    auto msg_offset = builder.CreateString(ss.str());
    auto r = flatbuf::header_v2::CreateHeaderV2(builder, msg_offset, 0, 0);
    builder.Finish(r);

    uint32_t size = builder.GetSize();
    std::string sMsg((const char *)builder.GetBufferPointer(), size);

    uint64_t meta = messages::getMetadata(msg, size);
    comm_->send_message(meta, msg->identifier, sMsg.c_str(), size);

  } break;
  }
}

void function_worker::lifecycle(std::unique_ptr<messages::worker_request> msg) {
  switch (msg->opcode) {
  case messages::eDestroy:
    thread_exit_cond_.store(true);
    event_gen_cond_.store(true);
    for (int index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      std::unique_ptr<messages::worker_request> destroy_msg(
          new messages::worker_request);
      workers_[index]->StopTimerScan();
      destroy_msg->event = msg->event;
      destroy_msg->opcode = msg->opcode;
      workers_[index]->push_msg(std::move(destroy_msg));
    }
    comm_->send_message(msg, "", "", "");
    break;
  }
}

void function_worker::dcp_message(
    std::unique_ptr<messages::worker_request> msg) {
  auto meta_info = messages::get_extras_dcp(msg->identifier, "", true);
  auto index = vb_map_.find(meta_info.vb);
  if (index == vb_map_.end()) {
    // We can send the acked msg here but wait for stats to send back the
    // response This is probably happen during rebalance phase and won't happen
    // during normal phase
    skipped_msgs_[meta_info.workerid].first++;
    skipped_msgs_[meta_info.workerid].second += msg->GetSize();
    unacked_count++;
    unacked_mem = unacked_mem + msg->GetSize();
    return;
  }

  auto worker = workers_[index->second];
  switch (msg->opcode) {
  case messages::eDcpMutation:
  case messages::eDcpNoOp:
  case messages::eDcpDeletion: {
    unacked_count++;
    unacked_mem = unacked_mem + msg->GetSize();
    worker->push_msg(std::move(msg));
  } break;

  case messages::eDcpCollectionDelete: {
    worker->AddCidFilterEvent(meta_info.cid);
    worker->push_msg(std::move(msg));
  } break;
  }

  if (unacked_count < max_unacked_count && unacked_mem < max_unacked_mem) {
    return;
  }

  auto acked_bytes = get_acked_bytes();
  flatbuffers::FlatBufferBuilder builder;
  auto msg_offset = builder.CreateString(acked_bytes);
  auto r = flatbuf::header_v2::CreateHeaderV2(builder, 0, 0, msg_offset);
  builder.Finish(r);

  uint32_t size = builder.GetSize();
  std::string sMsg((const char *)builder.GetBufferPointer(), size);

  uint64_t meta = messages::getMsgMeta(
      messages::eInternalComm, messages::eAckBytes, identifier_.size(), size);
  comm_->send_message(meta, identifier_, sMsg.c_str(), size);
}

void function_worker::global_settings(
    std::unique_ptr<messages::worker_request> msg) {
  switch (msg->opcode) {
  case messages::eFeatureMatrix:
    auto value = messages::get_value(msg->payload);
    auto featureMap = (uint32_t) static_cast<unsigned char>(value[0]) << 24 |
                      (uint32_t) static_cast<unsigned char>(value[1]) << 16 |
                      (uint32_t) static_cast<unsigned char>(value[2]) << 8 |
                      (uint32_t) static_cast<unsigned char>(value[3]);
    global_settings_->feature_matrix_.store(featureMap);

    for (int index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      std::unique_ptr<messages::worker_request> refresh_msg(
          new messages::worker_request);
      refresh_msg->event = msg->event;
      refresh_msg->opcode = msg->opcode;
      workers_[index]->push_msg(std::move(refresh_msg), true);
    }
    break;
  }
}

std::string function_worker::stats(messages::stats_opcode opcode) {
  std::string stats;

  switch (opcode) {
  case messages::eLatencyStats: {
    stats = get_latency_stats();
  } break;

  case messages::eInsight: {
    stats = stats_->GetCodeInsight();
  } break;

  case messages::eFailureStats: {
    stats = get_failure_stats();
  } break;

  case messages::eExecutionStats: {
    stats = get_execution_stats();
  } break;

  case messages::eLcbExceptions: {
    stats = get_lcb_exception_stats();
  } break;

  case messages::eCurlLatencyStats: {
    stats = get_curl_latency_stats();
  } break;

  case messages::eProcessedEvents: {
    std::ostringstream oss;
    for (auto index = 0; index < app_details_->settings->cpp_thread_count;
         index++) {
      oss << workers_[index]->get_vb_details_seq();
    }
    stats = oss.str();

  } break;

  case messages::eAckEvents: {
    stats = get_acked_bytes();
  } break;

  case messages::eAllStats: {
    nlohmann::json all_stats;
    all_stats["execution_stats"] = get_execution_stats();
    all_stats["failure_stats"] = get_failure_stats();
    all_stats["latency_stats"] = get_latency_stats();
    all_stats["curl_latency_stats"] = get_curl_latency_stats();
    all_stats["lcb_exception_stats"] = get_lcb_exception_stats();
    stats = all_stats.dump();
  } break;
  }

  return stats;
}

const int64_t diff_gc_ran = 30;

void function_worker::RouteMessage() {
  while (!thread_exit_cond_.load()) {
    std::unique_ptr<messages::worker_request> wRequest;
    if (!worker_queue_->PopFront(wRequest)) {
      continue;
    }

    switch (wRequest->event) {
    case messages::eInitEvent: {
      init_event(std::move(wRequest));
    } break;

    case messages::eHandlerDynamicSettings: {
      dynamic_setting(std::move(wRequest));
    } break;

    case messages::eVbSettings: {
      vb_setting(std::move(wRequest));
    } break;

    case messages::eLifeCycleChange: {
      lifecycle(std::move(wRequest));
    } break;

    case messages::eDcpEvent: {
      dcp_message(std::move(wRequest));
    } break;

    case messages::eGlobalConfigChange: {
      global_settings(std::move(wRequest));
    } break;

    default: {
    } break;
    }
  }
}

void function_worker::EventGenLoop() {
  auto last_ran_gc_ = GetUnixTime();
  while (!event_gen_cond_.load()) {
    // Scan for timers
    if (this->app_details_->usingTimer) {
      for (auto &v8_worker : this->workers_) {
        if (!v8_worker->scan_timer_.load()) {
          v8_worker->scan_timer_.store(true);
          std::unique_ptr<messages::worker_request> msg(
              new messages::worker_request);
          msg->event = messages::eInternal;
          msg->opcode = messages::eScanTimer;
          v8_worker->push_msg(std::move(msg), true);
        }
      }
    }

    // Update the v8 heap size
    for (auto &v8_worker : this->workers_) {
      if (!v8_worker->update_v8_heap_.load()) {
        v8_worker->update_v8_heap_.store(true);
        std::unique_ptr<messages::worker_request> msg(
            new messages::worker_request);
        msg->event = messages::eInternal;
        msg->opcode = messages::eUpdateV8HeapSize;
        v8_worker->push_msg(std::move(msg), true);
      }
    }

    // Check for memory growth
    int64_t approx_memory = 0;
    for (const auto &v8_worker : this->workers_) {
      approx_memory += v8_worker->GetMemory();
    }

    auto curr_time_stamp = GetUnixTime();
    for (auto &v8_worker : this->workers_) {
      if (!v8_worker->mem_check_.load() &&
          (v8_worker->v8_heap_size_ > MAX_V8_HEAP_SIZE ||
           approx_memory > process_mem_limit_byte ||
           (curr_time_stamp - last_ran_gc_) > diff_gc_ran)) {
        last_ran_gc_ = curr_time_stamp;
        v8_worker->mem_check_.store(true);
        std::unique_ptr<messages::worker_request> msg(
            new messages::worker_request);
        msg->event = messages::eInternal;
        msg->opcode = messages::eMemCheck;
        v8_worker->push_msg(std::move(msg), true);
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(7));
  }
}

// Stats
const std::string function_worker::get_execution_stats() {
  /*
    nlohmann::json estats;
    int64_t init = 0;

    estats["on_update_success"] = init;
    estats["on_update_failure"] = init;
    estats["on_delete_success"] = init;
    estats["on_delete_failure"] = init;
    estats["no_op_counter"] = init;
    estats["timer_callback_success"] = init;
    estats["timer_callback_failure"] = init;
    estats["timer_create_failure"] = init;
    estats["messages_parsed"] = init;
    estats["dcp_delete_msg_counter"] = init;
    estats["dcp_mutation_msg_counter"] = init;
    estats["timer_msg_counter"] = init;
    estats["timer_create_counter"] = init;
    estats["timer_cancel_counter"] = init;
    estats["lcb_retry_failure"] = init;
    estats["filtered_dcp_delete_counter"] = init;
    estats["filtered_dcp_mutation_counter"] = init;
    estats["num_processed_events"] = init;
    estats["curl"]["get"] = init;
    estats["curl"]["post"] = init;
    estats["curl"]["delete"] = init;
    estats["curl"]["head"] = init;
    estats["curl"]["put"] = init;
    estats["curl_success_count"] = init;

    for (auto index = 0; index < app_details_->settings->cpp_thread_count;
    index++) { estats["on_update_success"] =
    estats["on_update_success"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("on_update_success");
      estats["on_update_failure"] =
          estats["on_update_failure"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("on_update_failure");
      estats["on_delete_success"] =
          estats["on_delete_success"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("on_delete_success");
      estats["on_delete_failure"] =
          estats["on_delete_failure"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("on_delete_failure");
      estats["no_op_counter"] =
          estats["no_op_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("no_op_counter");
      estats["timer_callback_success"] =
          estats["timer_callback_success"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_callback_success");
      estats["timer_callback_failure"] =
          estats["timer_callback_failure"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_callback_failure");
      estats["timer_create_failure"] =
          estats["timer_create_failure"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_create_failure");
      estats["messages_parsed"] =
          estats["messages_parsed"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("messages_parsed");
      estats["dcp_delete_msg_counter"] =
          estats["dcp_delete_msg_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("dcp_delete_msg_counter");
      estats["dcp_mutation_msg_counter"] =
          estats["dcp_mutation_msg_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("dcp_mutation_msg_counter");
      estats["timer_msg_counter"] =
          estats["timer_msg_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_msg_counter");
      estats["timer_create_counter"] =
          estats["timer_create_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_create_counter");
      estats["timer_cancel_counter"] =
          estats["timer_cancel_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("timer_cancel_counter");
      estats["lcb_retry_failure"] =
          estats["lcb_retry_failure"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("lcb_retry_failure");
      estats["filtered_dcp_delete_counter"] =
          estats["filtered_dcp_delete_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat(
              "filtered_dcp_delete_counter");
      estats["filtered_dcp_mutation_counter"] =
          estats["filtered_dcp_mutation_counter"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat(
              "filtered_dcp_mutation_counter");
      estats["num_processed_events"] =
          estats["num_processed_events"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("num_processed_events");
      estats["curl"]["get"] =
          estats["curl"]["get"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl.get");
      estats["curl"]["post"] =
          estats["curl"]["post"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl.post");
      estats["curl"]["delete"] =
          estats["curl"]["delete"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl.delete");
      estats["curl"]["head"] =
          estats["curl"]["head"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl.head");
      estats["curl"]["put"] =
          estats["curl"]["put"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl.put");
      estats["curl_success_count"] =
          estats["curl_success_count"].get<int64_t>() +
          workers_[index]->stats_->GetExecutionStat("curl_success_count");
    }
    return estats.dump();
  */

  return stats_->GetExecutionStats();
}

const std::string function_worker::get_failure_stats() {
  /*
    nlohmann::json fstats;
    int64_t init = 0;

    fstats["bucket_op_exception_count"] = init;
    fstats["bucket_op_cache_miss_count"] = init;
    fstats["bucket_cache_overflow_count"] = init;
    fstats["bkt_ops_cas_mismatch_count"] = init;
    fstats["n1ql_op_exception_count"] = init;
    fstats["timeout_count"] = init;
    fstats["debugger_events_lost"] = init;
    fstats["timer_context_size_exceeded_counter"] = init;
    fstats["timer_callback_missing_counter"] = init;
    fstats["curl_non_200_response"] = init;
    fstats["curl_timeout_count"] = init;
    fstats["curl_failure_count"] = init;
    fstats["curl_max_resp_size_exceeded"] = init;

    for (auto index = 0; index < app_details_->settings->cpp_thread_count;
    index++) { fstats["bucket_op_exception_count"] =
          fstats["bucket_op_exception_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("bucket_op_exception_count");
      fstats["bucket_op_cache_miss_count"] =
          fstats["bucket_op_cache_miss_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("bucket_op_cache_miss_count");
      fstats["bucket_cache_overflow_count"] =
          fstats["bucket_cache_overflow_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("bucket_cache_overflow_count");
      fstats["bkt_ops_cas_mismatch_count"] =
          fstats["bkt_ops_cas_mismatch_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("bkt_ops_cas_mismatch_count");
      fstats["n1ql_op_exception_count"] =
          fstats["n1ql_op_exception_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("n1ql_op_exception_count");

      fstats["timeout_count"] =
          fstats["timeout_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("timeout_count");
      fstats["debugger_events_lost"] =
          fstats["debugger_events_lost"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("debugger_events_lost");
      fstats["timer_context_size_exceeded_counter"] =
          fstats["timer_context_size_exceeded_counter"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat(
              "timer_context_size_exceeded_counter");
      fstats["timer_callback_missing_counter"] =
          fstats["timer_callback_missing_counter"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat(
              "timer_callback_missing_counter");

      fstats["curl_non_200_response"] =
          fstats["curl_non_200_response"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("curl_non_200_response");
      fstats["curl_timeout_count"] =
          fstats["curl_timeout_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("curl_timeout_count");
      fstats["curl_failure_count"] =
          fstats["curl_failure_count"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("curl_failure_count");
      fstats["curl_max_resp_size_exceeded"] =
          fstats["curl_max_resp_size_exceeded"].get<int64_t>() +
          workers_[index]->stats_->GetFailureStat("curl_max_resp_size_exceeded");
    }

    return fstats.dump();
  */

  return stats_->GetFailureStats();
}

const std::string function_worker::get_latency_stats() {
  return stats_->GetLatencyStats();
}

const std::string function_worker::get_curl_latency_stats() {
  return stats_->GetCurlLatencyStats();
}

const std::string function_worker::get_lcb_exception_stats() { return ""; }
