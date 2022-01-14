#ifndef LCBINSTANCE_H
#define LCBINSTANCE_H

#include "utils.h"
#include "lcb_utils.h"
#include <libcouchbase/couchbase.h>

class LcbInstance {
public:
    LcbInstance &operator=(const LcbInstance &) = delete;
    LcbInstance(LcbInstance &conn) = delete;
    LcbInstance() { skip_lcb_ops_.store(false); }

    LcbInstance &operator=(LcbInstance &&conn) {
        if (this != &conn) {
            std::swap(handle_, conn.handle_);
        }
        return *this;
    }

    LcbInstance(LcbInstance &&conn) { std::swap(handle_, conn.handle_); }

    ~LcbInstance() {
        if (handle_ != nullptr) {
            lcb_destroy(handle_);
        }
    }

    void SetSkipLcbOps() {
        skip_lcb_ops_.store(true);
    }

    void ResetSkipLcbOps() {
        skip_lcb_ops_.store(false);
    }

    template <typename CmdType, typename Callable>
    std::pair<lcb_error_t, Result> Execute(CmdType &cmd, Callable &&callable, int max_retry_count, uint32_t max_retry_secs) {
        if (skip_lcb_ops_.load() == true || handle_ == nullptr) {
            return {LCB_ERROR, Result()};
        }
        return RetryLcbCommand(handle_, cmd, max_retry_count, max_retry_secs, callable);
    }
    lcb_t handle_{nullptr};
private:
    std::atomic<bool> skip_lcb_ops_{false};
};

#endif
