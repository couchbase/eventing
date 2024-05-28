#ifndef COUCHBASE_CHECKPOINT_WRITER_H
#define COUCHBASE_CHECKPOINT_WRITER_H

#include <libcouchbase/couchbase.h>
#include "bucket.h"

class CheckpointWriter {
  public:
    CheckpointWriter(v8::Isolate *isolate, const std::string& fiid, const std::string &bucket_name,
        const std::string &scope_name, const std::string &collection_name) : fiid_(fiid),
        bucket_(isolate, bucket_name, scope_name, collection_name, "", "") {}

    void Connect() {
        bucket_.Connect();
    }

    std::tuple<std::string, lcb_STATUS>
    Write(const MetaData &meta, const uint64_t& rootcas, const std::vector<std::string>& cleanup_cursors) {
      auto [err, err_code, result] = bucket_.WriteCheckpoint(meta, rootcas, cleanup_cursors);
      if (err)
        return {*err, LCB_ERR_GENERIC};
      if (err_code && (*err_code != LCB_SUCCESS))
        return {"", *err_code};
      if (result->rc != LCB_SUCCESS)
        return {"", result->rc};
      return {"", LCB_SUCCESS};
    }
  private:
    std::string fiid_;
    Bucket bucket_;
};

#endif