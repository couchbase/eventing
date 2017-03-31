#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#ifndef STANDALONE_BUILD
extern void assert(int);
#else
#include <cassert>
#endif

#include "../../flatbuf/include/cfg_schema_generated.h"

typedef struct deployment_config_s {
  std::string metadata_bucket;
  std::string source_bucket;
  std::map<std::string, std::map<std::string, std::vector<std::string>>>
      component_configs;
} deployment_config;

deployment_config *ParseDeployment(const char *app_name);
