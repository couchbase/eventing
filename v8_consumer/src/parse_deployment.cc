#include "../include/parse_deployment.h"

deployment_config *ParseDeployment(const char *app_code) {
  deployment_config *config = new deployment_config();

  auto app_cfg = cfg::GetConfig((const void *)app_code);

  auto dep_cfg = app_cfg->depCfg();
  config->metadata_bucket = dep_cfg->metadataBucket()->str();
  config->source_bucket = dep_cfg->sourceBucket()->str();

  auto buckets = dep_cfg->buckets();

  std::map<std::string, std::vector<std::string>> buckets_info;
  for (unsigned int i = 0; i < buckets->size(); i++) {
    std::vector<std::string> bucket_info;
    bucket_info.push_back(buckets->Get(i)->bucketName()->str());
    bucket_info.push_back(buckets->Get(i)->alias()->str());

    buckets_info[buckets->Get(i)->alias()->str()] = bucket_info;
  }

  config->component_configs["buckets"] = buckets_info;

  return config;
}
