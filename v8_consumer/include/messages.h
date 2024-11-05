#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdint.h>
#include <vector>

#include "../gen/flatbuf/header_v2_generated.h"
#include "utils.h"

const int64_t DCP_MESSAGE_SUB = 23;

namespace messages {

enum datatype { tRaw, tJson };

enum event {
  eInitEvent,
  eHandlerDynamicSettings,
  eVbSettings,
  eLifeCycleChange,
  eStatsEvent,
  eDcpEvent,
  eGlobalConfigChange,
  eInternalComm,
  eInternal
};

enum init_opcode {
  eCompileHandler,
  eInitHandler,
  eDebugHandlerStart,
  eDebugHandlerStop,
  eOnDeployHandler
};

enum dynamic_opcode { eLogLevelChange, eTimeContextSize };

enum vb_setting_opcode { eVbMap, eAddVb, eFilterVb };

enum lifecycle_opcode { ePause, eResume, eDestroy };

enum stats_opcode {
  eLatencyStats,
  eInsight,
  eFailureStats,
  eExecutionStats,
  eLcbExceptions,
  eCurlLatencyStats,
  eProcessedEvents,
  eAckEvents,
  eAllStats
};

enum msg_opcode { eDcpMutation, eDcpDeletion, eDcpNoOp, eDcpCollectionDelete };

enum global_opcode { eFeatureMatrix };

enum internal_comm_opcode { eAckBytes };

enum internal_opcode {
  eScanTimer,
  eDeleteFilter,
  eMemCheck,
  eUpdateV8HeapSize
};

struct worker_request {
  messages::event event;
  int opcode;
  std::vector<uint8_t> identifier;
  std::vector<uint8_t> payload;

  inline size_t GetSize() { return payload.size(); }
};

inline event getEventCode(const std::vector<uint8_t> &metadata) {
  return (event)((int)(metadata[0]));
}

inline int getOpcode(const std::vector<uint8_t> &metadata) {
  return (int)(metadata[1]);
}

inline std::pair<uint64_t, uint64_t>
getMessageSize(const std::vector<uint8_t> &metadata) {
  uint64_t id_size = (uint64_t)(metadata[2]);
  uint64_t msg_size = (uint64_t)(metadata[3]) << 32 |
                      (uint64_t)(metadata[4]) << 24 |
                      (uint64_t)(metadata[5]) << 16 |
                      (uint64_t)(metadata[6]) << 8 | (uint64_t)metadata[7];

  return {id_size, msg_size};
}

inline uint64_t getMsgMeta(messages::event event, int opcode, size_t idSize,
                           size_t payloadSize) {
  uint64_t metadata = (uint64_t)(event) << 56 | (uint64_t)(opcode) << 48 |
                      (uint64_t)(idSize) << 40 | uint64_t(payloadSize);
  return metadata;
}

inline uint64_t getMetadata(const std::unique_ptr<worker_request> &wRequest,
                            size_t size) {
  return getMsgMeta(wRequest->event, wRequest->opcode,
                    wRequest->identifier.size(), size);
}

inline std::string getHandlerID(const std::vector<uint8_t> &identifier) {
  std::string str(identifier.begin(), identifier.end());
  return str;
}

inline std::string getHandlerIDForDcp(const std::vector<uint8_t> &identifier) {
  auto till = identifier.size() - DCP_MESSAGE_SUB;
  std::string str(identifier.begin(), identifier.begin() + till);
  return str;
}

struct meta_info {
  uint64_t seq{0};
  uint64_t vbuuid{0};
  uint32_t cid{0};
  uint32_t expiry{0};
  uint16_t vb{0};
  uint8_t workerid{0};
  uint8_t datatype{0};

  bool cursor_present{false};
  uint64_t root_cas{0}, cas{0};
  std::vector<std::string> stale_cursors;
  std::string key{""};
  std::string scope_name{""};
  std::string collection_name{""};
};

inline meta_info get_extras_dcp(const std::vector<uint8_t> &identifier,
                                const std::string &extras,
                                bool vb_only = false) {
  auto identifier_size = identifier.size() - 1;

  meta_info minfo = {};
  minfo.seq = (uint64_t)identifier[identifier_size] |
              (uint64_t)identifier[identifier_size - 1] << 8 |
              (uint64_t)identifier[identifier_size - 2] << 16 |
              (uint64_t)identifier[identifier_size - 3] << 24 |
              (uint64_t)identifier[identifier_size - 4] << 32 |
              (uint64_t)identifier[identifier_size - 5] << 40 |
              (uint64_t)identifier[identifier_size - 6] << 48 |
              (uint64_t)identifier[identifier_size - 7] << 56;
  identifier_size = identifier_size - 8;

  minfo.vbuuid = (uint64_t)identifier[identifier_size] |
                 (uint64_t)identifier[identifier_size - 1] << 8 |
                 (uint64_t)identifier[identifier_size - 2] << 16 |
                 (uint64_t)identifier[identifier_size - 3] << 24 |
                 (uint64_t)identifier[identifier_size - 4] << 32 |
                 (uint64_t)identifier[identifier_size - 5] << 40 |
                 (uint64_t)identifier[identifier_size - 6] << 48 |
                 (uint64_t)identifier[identifier_size - 7] << 56;
  identifier_size = identifier_size - 8;

  minfo.cid = (uint32_t)identifier[identifier_size] |
              (uint32_t)identifier[identifier_size - 1] << 8 |
              (uint32_t)identifier[identifier_size - 2] << 16 |
              (uint32_t)identifier[identifier_size - 3] << 24;
  identifier_size = identifier_size - 4;

  minfo.vb = (uint16_t)identifier[identifier_size] |
             (uint16_t)identifier[identifier_size - 1] << 8;
  identifier_size = identifier_size - 2;

  minfo.workerid = (uint8_t)identifier[identifier_size];
  identifier_size = identifier_size - 1;

  if (extras.size() >= 5) {
    int extras_size = 0;
    minfo.datatype = (uint8_t) static_cast<unsigned char>(extras[extras_size]);
    extras_size = extras_size + 1;

    minfo.expiry =
        (uint32_t) static_cast<unsigned char>(extras[extras_size]) << 24 |
        (uint32_t) static_cast<unsigned char>(extras[extras_size + 1]) << 16 |
        (uint32_t) static_cast<unsigned char>(extras[extras_size + 2]) << 8 |
        (uint32_t) static_cast<unsigned char>(extras[extras_size + 3]);
    extras_size = extras_size + 4;

    // Check for cursor related data
    if (extras_size > 5) {
      // get the root cas
      minfo.cursor_present = true;
      minfo.root_cas =
          (uint64_t) static_cast<unsigned char>(extras[extras_size]) |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 1]) << 8 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 2]) << 16 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 3]) << 24 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 4]) << 32 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 5]) << 40 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 6]) << 48 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 7]) << 56;
      extras_size = extras_size + 8;

      minfo.cas =
          (uint64_t) static_cast<unsigned char>(extras[extras_size]) |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 1]) << 8 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 2]) << 16 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 3]) << 24 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 4]) << 32 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 5]) << 40 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 6]) << 48 |
          (uint64_t) static_cast<unsigned char>(extras[extras_size + 7]) << 56;
      extras_size = extras_size + 8;

      uint16_t key_len =
          (uint16_t) static_cast<unsigned char>(extras[extras_size]) |
          (uint16_t) static_cast<unsigned char>(extras[extras_size + 1]) << 8;
      extras_size = extras_size + 2;
      minfo.key = extras.substr(extras_size, key_len);
      extras_size = extras_size + key_len;

      uint16_t stale_cursor_len =
          (uint32_t) static_cast<unsigned char>(extras[extras_size]) << 24 |
          (uint32_t) static_cast<unsigned char>(extras[extras_size + 1]) << 16 |
          (uint32_t) static_cast<unsigned char>(extras[extras_size + 2]) << 8 |
          (uint32_t) static_cast<unsigned char>(extras[extras_size + 3]);

      extras_size = extras_size + 4;
      if (stale_cursor_len > 0) {
        std::string stale_cursors =
            extras.substr(extras_size, stale_cursor_len);
        extras_size = extras_size + stale_cursor_len;
        splitString(stale_cursors, minfo.stale_cursors, ',');
      }

      uint16_t scope_len =
          (uint16_t) static_cast<unsigned char>(extras[extras_size]) |
          (uint16_t) static_cast<unsigned char>(extras[extras_size + 1]) << 8;
      extras_size = extras_size + 2;
      minfo.scope_name = extras.substr(extras_size, scope_len);
      extras_size = extras_size + scope_len;

      uint16_t collection_len =
          (uint16_t) static_cast<unsigned char>(extras[extras_size]) |
          (uint16_t) static_cast<unsigned char>(extras[extras_size + 1]) << 8;
      extras_size = extras_size + 2;
      minfo.collection_name = extras.substr(extras_size, collection_len);
      extras_size = extras_size + collection_len;
    }
  }
  return minfo;
}

inline std::string get_extras(const std::vector<uint8_t> &payload) {
  auto header = flatbuf::header_v2::GetHeaderV2(payload.data());
  return header->extras()->str();
}

inline std::string get_meta(const std::vector<uint8_t> &payload) {
  auto header = flatbuf::header_v2::GetHeaderV2(payload.data());
  return header->meta()->str();
}

inline std::string get_value(const std::vector<uint8_t> &payload) {
  auto header = flatbuf::header_v2::GetHeaderV2(payload.data());
  return header->value()->str();
}

struct payload {
  std::string extras;
  std::string meta;
  std::string xattr;
  std::string value;
};

inline payload get_payload_msg(const std::vector<uint8_t> &payload) {
  auto header = flatbuf::header_v2::GetHeaderV2(payload.data());
  auto extra = header->extras()->str();
  auto meta = header->meta()->str();
  auto xattr = header->xattr()->str();
  auto value = header->value()->str();
  return {extra, meta, xattr, value};
}

// TODO: Get vb lets see
inline std::tuple<uint32_t, uint16_t>
get_vb(const std::vector<uint8_t> &identifier) {
  return {0, 0};
}

} // namespace messages

class communicator {
public:
  communicator() = default;
  virtual ~communicator() {}

  virtual void send_message(uint64_t metadata_size,
                            const std::vector<uint8_t> &id, const char *payload,
                            uint32_t payload_size){};
  virtual void
  send_message(const std::unique_ptr<messages::worker_request> &wRequest,
               const std::string &extras, const std::string &meta,
               const std::string &value){};
};

#endif // CB_EVALUATOR_MESSAGES_H
