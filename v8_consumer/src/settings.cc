#include "settings.h"
#include <nlohmann/json.hpp>

namespace settings {
deployment_config *ParseDeployment(const std::string &metadata_str) {
  deployment_config *config = new deployment_config();

  auto metadata = nlohmann::json::parse(metadata_str, nullptr, false);
  if (metadata.is_discarded()) {
    return nullptr;
  }

  config->metadata_bucket =
      metadata["depcfg"]["meta_keyspace"]["bucket_name"].get<std::string>();
  config->metadata_scope =
      metadata["depcfg"]["meta_keyspace"]["scope_name"].get<std::string>();
  config->metadata_collection =
      metadata["depcfg"]["meta_keyspace"]["collection_name"].get<std::string>();
  config->source_bucket =
      metadata["depcfg"]["source_keyspace"]["bucket_name"].get<std::string>();
  config->source_scope =
      metadata["depcfg"]["source_keyspace"]["scope_name"].get<std::string>();
  config->source_collection =
      metadata["depcfg"]["source_keyspace"]["collection_name"]
          .get<std::string>();

  std::unordered_map<std::string, std::vector<std::string>> buckets_info;
  std::vector<std::string> bucket_alias;

  auto bindings = metadata["bindings"];
  for (size_t idx = 0; idx < bindings.size(); ++idx) {
    nlohmann::json binding = bindings.at(idx);
    switch (binding["binding_type"].get<uint8_t>()) {
    case bucket_binding: {
      auto bucket = binding["bucket_binding"];
      std::vector<std::string> bucket_info;
      bucket_info.push_back(
          bucket["keyspace"]["bucket_name"].get<std::string>());
      bucket_info.push_back(
          bucket["keyspace"]["scope_name"].get<std::string>());
      bucket_info.push_back(
          bucket["keyspace"]["collection_name"].get<std::string>());
      bucket_info.push_back(bucket["alias"].get<std::string>());
      bucket_info.push_back(bucket["access"].get<std::string>());
      buckets_info[bucket["alias"].get<std::string>()] = bucket_info;
    } break;

    case curl_binding:
      config->curl_bindings.emplace_back(binding["curl_binding"]);
      break;

    case constant_binding:
      config->constant_bindings.push_back(
          {binding["constant_binding"]["value"].get<std::string>(),
           binding["constant_binding"]["literal"].get<std::string>()});
      break;
    }
  }
  config->component_configs["buckets"] = buckets_info;

  return config;
}

std::shared_ptr<app_details>
parse_app_details(const std::vector<uint8_t> &payload) {
  auto app_bytes = messages::get_value(payload);

  app_details *app = new app_details();

  nlohmann::json parsed_app;
  auto metadata = nlohmann::json::parse(app_bytes, nullptr, false);
  if (metadata.is_discarded()) {
    return nullptr;
  }

  app_location *location = new app_location();
  location->bucket_name =
      metadata["app_location"]["function_scope"]["bucket_name"]
          .get<std::string>();
  location->scope_name =
      metadata["app_location"]["function_scope"]["scope_name"]
          .get<std::string>();
  location->app_name = metadata["app_location"]["app_name"].get<std::string>();
  app->location = location;

  app->app_code = metadata["app_code"].get<std::string>();
  app->app_instance_id = metadata["function_instance_id"].get<std::string>();
  app->app_id = metadata["function_id"].get<uint32_t>();

  app_setting *settings = new app_setting();
  settings->timeout = metadata["settings"]["execution_timeout"].get<uint64_t>();

  settings->cpp_thread_count =
      metadata["settings"]["cpp_worker_thread_count"].get<uint32_t>();

  settings->log_level = metadata["settings"]["log_level"].get<std::string>();
  settings->lang_compatibility =
      metadata["settings"]["language_compatibility"].get<std::string>();
  settings->lcb_inst_capacity =
      metadata["settings"]["lcb_inst_capacity"].get<uint32_t>();
  settings->lcb_retry_count =
      metadata["settings"]["lcb_retry_count"].get<uint32_t>();
  settings->lcb_timeout = metadata["settings"]["lcb_timeout"].get<uint64_t>();
  settings->n1ql_consistency =
      metadata["settings"]["n1ql_consistency"].get<std::string>();
  settings->num_timer_partitions =
      metadata["settings"]["num_timer_partitions"].get<uint16_t>();
  settings->log_level = metadata["settings"]["log_level"].get<std::string>();
  settings->timer_context_size =
      metadata["settings"]["timer_context_size"].get<uint32_t>();
  settings->n1ql_prepare_all =
      metadata["settings"]["n1ql_prepare_all"].get<bool>();
  settings->handler_headers =
      metadata["settings"]["handler_headers"].get<std::vector<std::string>>();
  settings->handler_footers =
      metadata["settings"]["handler_footers"].get<std::vector<std::string>>();
  settings->bucket_cache_size =
      metadata["settings"]["bucket_cache_size"].get<uint64_t>();
  settings->bucket_cache_age =
      metadata["settings"]["bucket_cache_age"].get<uint64_t>();
  settings->curl_max_allowed_resp_size =
      metadata["settings"]["curl_max_allowed_resp_size"].get<uint64_t>();
  settings->cursor_aware = metadata["settings"]["cursor_aware"].get<bool>();

  app->settings = settings;

  app->dConfig = ParseDeployment(app_bytes);
  app->usingTimer = metadata["metainfo"]["IsUsingTimer"].get<bool>();
  app->num_vbuckets =
      metadata["metainfo"]["SourceID"]["NumVbuckets"].get<uint16_t>();

  auto appOwner = new owner();
  appOwner->user_ = metadata["owner"]["User"].get<std::string>();
  appOwner->domain_ = metadata["owner"]["Domain"].get<std::string>();
  app->app_owner = appOwner;

  return std::shared_ptr<settings::app_details>(app);
}

}; // namespace settings
