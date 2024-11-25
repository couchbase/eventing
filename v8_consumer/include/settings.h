#ifndef SETTINGS_H
#define SETTINGS_H

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "curl.h"
#include "messages.h"
#include "utils.h"

#define SECS_TO_NS 1000 * 1000 * 1000ULL

const size_t MAX_V8_HEAP_SIZE = 1 * 1024 * 1024 * 1024;

/*
enum RETURN_CODE {
  kSuccess = 0,
  kFailedToCompileJs,
  kNoHandlersDefined,
  kFailedInitBucketHandle,
  kOnUpdateCallFail,
  kOnDeleteCallFail,
  kToLocalFailed,
  kJSONParseFailed
};
*/

namespace settings {

struct app_setting {
  uint64_t timeout;                         // execution_timeout
  int cpp_thread_count;                     // cpp_thread_count
  std::string lang_compatibility;           // language_compatibility
  uint32_t lcb_inst_capacity;               // lcb_inst_capacity
  uint32_t lcb_retry_count;                 // lcb_retry_count
  uint64_t lcb_timeout;                     // lcb_timeout
  std::string n1ql_consistency;             // n1ql_consistency
  uint16_t num_timer_partitions;            // num_timer_partitions
  std::string log_level;                    // log_level
  uint32_t timer_context_size;              // timer_context_size
  bool n1ql_prepare_all;                    // n1ql_prepare_all
  std::vector<std::string> handler_headers; // handler_headers
  std::vector<std::string> handler_footers; // handler_footers
  uint64_t bucket_cache_size;               // bucket_cache_size
  uint64_t bucket_cache_age;                // bucket_cache_age
  uint64_t curl_max_allowed_resp_size;      // curl_max_allowed_resp_size
  bool cursor_aware;                        // cursor_aware
};

struct global {
  global() { feature_matrix_ = Feature_Curl; }

  std::atomic<uint32_t> feature_matrix_{Feature_Curl};
};

struct app_location {
  std::string bucket_name;
  std::string scope_name;
  std::string app_name;
};

struct owner {
  std::string user_;
  std::string domain_;
};

enum binding_type : uint8_t { bucket_binding, curl_binding, constant_binding };

struct deployment_config {
  std::string metadata_bucket;
  std::string metadata_scope;
  std::string metadata_collection;
  std::string source_bucket;
  std::string source_scope;
  std::string source_collection;

  std::unordered_map<std::string,
                     std::unordered_map<std::string, std::vector<std::string>>>
      component_configs;
  std::vector<CurlBinding> curl_bindings;
  std::vector<std::pair<std::string, std::string>> constant_bindings;
};

struct app_details {
  app_location *location;
  std::string app_code;
  uint32_t app_id;
  std::string app_instance_id;
  app_setting *settings;
  deployment_config *dConfig;

  bool usingTimer;
  uint16_t num_vbuckets;
  owner *app_owner;
};

struct cluster {
  cluster(std::string local_address_, std::string eventing_dir_,
          std::string ns_server_port_, std::string eventing_port_,
          std::string debugger_port_, std::string cert_file)
      : local_address_(local_address_), eventing_dir_(eventing_dir_),
        ns_server_port_(ns_server_port_), eventing_port_(eventing_port_),
        debugger_port_(debugger_port_), cert_file_(cert_file) {}

  std::string local_address_;
  std::string eventing_dir_;
  std::string ns_server_port_;
  std::string eventing_port_;
  std::string debugger_port_;
  std::string cert_file_;
};

std::shared_ptr<settings::app_details>
parse_app_details(const std::vector<uint8_t> &payload);

} // namespace settings

#endif
