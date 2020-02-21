// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

#include <boost/scoped_ptr.hpp>
#include <utility>
#include <vector>
#include <string>

// NOTE: try not to add more headers here: runtime-state.h is included in many many files.
#include "common/global-types.h"  // for PlanNodeId
#include "common/atomic.h"
#include "runtime/client-cache-types.h"
#include "runtime/dml-exec-state.h"
#include "util/error-util-internal.h"
#include "util/runtime-profile.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

class BufferPool;
class DataStreamRecvr;
class DescriptorTbl;
class Expr;
class KrpcDataStreamMgr;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class ReservationTracker;
class RuntimeFilterBank;
class ScalarExpr;
class Status;
class TimestampValue;
class ThreadResourcePool;
class TUniqueId;
class ExecEnv;
class HBaseTableFactory;
class TPlanFragmentCtx;
class TPlanFragmentInstanceCtx;
class QueryState;
class ConditionVariable;

namespace io {
  class DiskIoMgr;
}

/// Shared state for Impala's runtime query execution. Used in two contexts:
/// * Within execution of a fragment instance to hold shared state for the fragment
///   instance (i.e. there is a 1:1 relationship with FragmentInstanceState).
/// * A standalone mode for other cases where query execution infrastructure is used
///   outside the context of a fragment instance, e.g. tests, evaluation of constant
///   expressions, etc. In this case the RuntimeState sets up all the required
///   infrastructure.
///
/// RuntimeState is shared between multiple threads and so methods must generally be
/// thread-safe.
///
/// After initialisation, callers must call ReleaseResources() to ensure that all
/// resources are correctly freed before destruction.
class RuntimeState {
 public:
  /// query_state, fragment_ctx, and instance_ctx need to be alive at least as long as
  /// the constructed RuntimeState
  RuntimeState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& instance_ctx, ExecEnv* exec_env);

  /// RuntimeState for test execution and fe-support.cc. Creates its own QueryState and
  /// installs desc_tbl, if set. If query_ctx.request_pool isn't set, sets it to "test-pool".
  RuntimeState(
      const TQueryCtx& query_ctx, ExecEnv* exec_env, DescriptorTbl* desc_tbl = nullptr);

  /// Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  QueryState* query_state() const { return query_state_; }
  /// Return the query's ObjectPool
  ObjectPool* obj_pool() const;
  const DescriptorTbl& desc_tbl() const;
  const TQueryOptions& query_options() const;
  int batch_size() const { return query_options().batch_size; }
  bool abort_on_error() const { return query_options().abort_on_error; }
  bool strict_mode() const { return query_options().strict_mode; }
  bool decimal_v2() const { return query_options().decimal_v2; }
  const TQueryCtx& query_ctx() const;
  const TPlanFragmentInstanceCtx& instance_ctx() const { return *instance_ctx_; }
  const TUniqueId& session_id() const { return query_ctx().session.session_id; }
  const std::string& do_as_user() const { return query_ctx().session.delegated_user; }
  const std::string& connected_user() const {
    return query_ctx().session.connected_user;
  }
  const TimestampValue* now() const { return now_.get(); }
  const TimestampValue* utc_timestamp() const { return utc_timestamp_.get(); }
  void set_now(const TimestampValue* now);
  const Timezone& local_time_zone() const { return *local_time_zone_; }
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& fragment_instance_id() const {
    return instance_ctx_ != nullptr
        ? instance_ctx_->fragment_instance_id
        : no_instance_id_;
  }
  MemTracker* instance_mem_tracker() { return instance_mem_tracker_; }
  MemTracker* query_mem_tracker();  // reference to the query_state_'s memtracker
  ReservationTracker* instance_buffer_reservation() {
    return instance_buffer_reservation_;
  }
  ThreadResourcePool* resource_pool() { return resource_pool_.get(); }

  void set_fragment_root_id(PlanNodeId id) {
    DCHECK_EQ(root_node_id_, -1) << "Should not set this twice.";
    root_node_id_ = id;
  }

  /// The seed value to use when hashing tuples.
  /// See comment on root_node_id_. We add one to prevent having a hash seed of 0.
  uint32_t fragment_hash_seed() const { return root_node_id_ + 1; }

  RuntimeFilterBank* filter_bank() const;

  DmlExecState* dml_exec_state() { return &dml_exec_state_; }

  /// Returns runtime state profile
  RuntimeProfile* runtime_profile() { return profile_; }

  /// Returns the LlvmCodeGen object for this fragment instance.
  LlvmCodeGen* codegen() { return codegen_.get(); }

  const std::string& GetEffectiveUser() const;

  /// Add ScalarExpr expression 'expr' to be codegen'd later if it's not disabled by
  /// query option. If 'is_codegen_entry_point' is true, 'expr' will be an entry
  /// point into codegen'd evaluation (i.e. it will have a function pointer populated).
  /// Adding an expr here ensures that it will be codegen'd (i.e. fragment execution
  /// will fail with an error if the expr cannot be codegen'd).
  void AddScalarExprToCodegen(ScalarExpr* expr, bool is_codegen_entry_point) {
    scalar_exprs_to_codegen_.push_back({expr, is_codegen_entry_point});
  }

  /// Returns true if there are ScalarExpr expressions in the fragments that we want
  /// to codegen (because they can't be interpreted or based on options/hints).
  /// This should only be used after the Prepare() phase in which all expressions'
  /// Prepare() are invoked.
  bool ScalarExprNeedsCodegen() const { return !scalar_exprs_to_codegen_.empty(); }

  /// Check if codegen was disabled and if so, add a message to the runtime profile.
  void CheckAndAddCodegenDisabledMessage(RuntimeProfile* profile) {
    if (CodegenDisabledByQueryOption()) {
      profile->AddCodegenMsg(false, "disabled by query option DISABLE_CODEGEN");
    } else if (CodegenDisabledByHint()) {
      profile->AddCodegenMsg(false, "disabled due to optimization hints");
    }
  }

  /// Returns true if there is a hint to disable codegen. This can be true for single node
  /// optimization or expression evaluation request from FE to BE (see fe-support.cc).
  /// Note that this internal flag is advisory and it may be ignored if the fragment has
  /// any UDF which cannot be interpreted. See ScalarExpr::Prepare() for details.
  inline bool CodegenHasDisableHint() const {
    return query_ctx().disable_codegen_hint;
  }

  /// Returns true iff there is a hint to disable codegen and all expressions in the
  /// fragment can be interpreted. This should only be used after the Prepare() phase
  /// in which all expressions' Prepare() are invoked.
  inline bool CodegenDisabledByHint() const {
    return CodegenHasDisableHint() && !ScalarExprNeedsCodegen();
  }

  /// Returns true if codegen is disabled by query option.
  inline bool CodegenDisabledByQueryOption() const {
    return query_options().disable_codegen;
  }

  /// Returns true if codegen should be enabled for this fragment. Codegen is enabled
  /// if all the following conditions hold:
  /// 1. it's enabled by query option
  /// 2. it's not disabled by internal hints or there are expressions in the fragment
  ///    which cannot be interpreted.
  inline bool ShouldCodegen() const {
    return !CodegenDisabledByQueryOption() && !CodegenDisabledByHint();
  }

  inline Status GetQueryStatus() {
    // Do a racy check for query_status_ to avoid unnecessary spinlock acquisition.
    if (UNLIKELY(!query_status_.ok())) {
      std::lock_guard<SpinLock> l(query_status_lock_);
      return query_status_;
    }
    return Status::OK();
  }

  /// Log an error that will be sent back to the coordinator based on an instance of the
  /// ErrorMsg class. The runtime state aggregates log messages based on type with one
  /// exception: messages with the GENERAL type are not aggregated but are kept
  /// individually.
  bool LogError(const ErrorMsg& msg, int vlog_level = 1);

  /// Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    std::lock_guard<SpinLock> l(error_log_lock_);
    return error_log_.size() < query_options().max_errors;
  }

  /// Returns true if there are entries in the error log.
  bool HasErrors() {
    std::lock_guard<SpinLock> l(error_log_lock_);
    return !error_log_.empty();
  }

  /// Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog();

  /// Clear 'new_errors' and append all accumulated errors since the last call to this
  /// function to 'new_errors' to be sent back to the coordinator. This has the side
  /// effect of clearing out the internal error log map once this function returns.
  void GetUnreportedErrors(ErrorLogMapPB* new_errors);

  /// Given an error message, determine whether execution should be aborted and, if so,
  /// return the corresponding error status. Otherwise, log the error and return
  /// Status::OK(). Execution is aborted if the ABORT_ON_ERROR query option is set to
  /// true or the error is not recoverable and should be handled upstream.
  Status LogOrReturnError(const ErrorMsg& message);

  bool is_cancelled() const { return is_cancelled_.Load(); }
  void Cancel();
  /// Add a condition variable to be signalled when this RuntimeState is cancelled.
  /// Adding a condition variable multiple times is a no-op. Each distinct 'cv' will be
  /// signalled once with NotifyAll() when is_cancelled() becomes true.
  /// The condition variable must have query lifetime.
  void AddCancellationCV(ConditionVariable* cv);

  RuntimeProfile::Counter* total_storage_wait_timer() {
    return total_storage_wait_timer_;
  }

  RuntimeProfile::Counter* total_network_send_timer() {
    return total_network_send_timer_;
  }

  RuntimeProfile::Counter* total_network_receive_timer() {
    return total_network_receive_timer_;
  }

  RuntimeProfile::ThreadCounters* total_thread_statistics() const {
   return total_thread_statistics_;
  }

  void AddBytesReadCounter(RuntimeProfile::Counter* counter) {
    bytes_read_counters_.push_back(counter);
  }

  void AddBytesSentCounter(RuntimeProfile::Counter* counter) {
    bytes_sent_counters_.push_back(counter);
  }

  /// Computes the ratio between the bytes sent and the bytes read by this runtime state's
  /// fragment instance. For fragment instances that don't scan data, this returns 0.
  double ComputeExchangeScanRatio() const;

  /// Sets query_status_ with err_msg if no error has been set yet.
  void SetQueryStatus(const std::string& err_msg) {
    std::lock_guard<SpinLock> l(query_status_lock_);
    if (!query_status_.ok()) return;
    query_status_ = Status(err_msg);
  }

  /// Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
  /// Subsequent calls to this will be no-ops.
  /// If 'failed_allocation_size' is not 0, then it is the size of the allocation (in
  /// bytes) that would have exceeded the limit allocated for 'tracker'.
  /// This value and tracker are only used for error reporting.
  /// If 'msg' is non-NULL, it will be appended to query_status_ in addition to the
  /// generic "Memory limit exceeded" error.
  /// Note that this interface is deprecated and MemTracker::LimitExceeded() should be
  /// used and the error status should be returned.
  void SetMemLimitExceeded(MemTracker* tracker,
      int64_t failed_allocation_size = 0, const ErrorMsg* msg = NULL);

  /// Returns a non-OK status if query execution should stop (e.g., the query was
  /// cancelled or a mem limit was exceeded). Exec nodes should check this periodically so
  /// execution doesn't continue if the query terminates abnormally. This should not be
  /// called after ReleaseResources().
  Status CheckQueryState();

  /// Create a codegen object accessible via codegen() if it doesn't exist already.
  Status CreateCodegen();

  /// Codegen all ScalarExpr expressions in 'scalar_exprs_to_codegen_'. If codegen fails
  /// for any expressions, return immediately with the error status. Once IMPALA-4233 is
  /// fixed, it's not fatal to fail codegen if the expression can be interpreted.
  /// TODO: Fix IMPALA-4233
  Status CodegenScalarExprs();

  /// Helper to call QueryState::StartSpilling().
  Status StartSpilling(MemTracker* mem_tracker);

  /// Release resources and prepare this object for destruction. Can only be called once.
  void ReleaseResources();

  /// If the fragment instance associated with this RuntimeState failed due to a RPC
  /// failure, use this method to set the network address of the RPC's target node and
  /// the posix error code of the failed RPC. The target node address and posix error code
  /// will be included in the AuxErrorInfo returned by GetAuxErrorInfo. This method is
  /// idempotent.
  void SetRPCErrorInfo(TNetworkAddress dest_node, int16_t posix_error_code);

  /// Returns true if this RuntimeState has any auxiliary error information, false
  /// otherwise. Currently, only SetRPCErrorInfo() sets aux error info.
  bool HasAuxErrorInfo() {
    std::lock_guard<SpinLock> l(aux_error_info_lock_);
    return aux_error_info_ != nullptr;
  }

  /// Sets the given AuxErrorInfoPB with all relevant aux error info from the fragment
  /// instance associated with this RuntimeState. If no aux error info for this
  /// RuntimeState has been set, this method does nothing. Currently, only
  /// SetRPCErrorInfo() sets aux error info. This method clears aux_error_info_. Calls to
  /// HasAuxErrorInfo() after this method has been called will return false.
  void GetUnreportedAuxErrorInfo(AuxErrorInfoPB* aux_error_info);

  static const char* LLVM_CLASS_NAME;

 private:
  /// Allow TestEnv to use private methods for testing.
  friend class TestEnv;

  /// Set per-fragment state.
  void Init();

  /// Lock protecting error_log_
  SpinLock error_log_lock_;

  /// Logs error messages.
  ErrorLogMap error_log_;

  /// Global QueryState and original thrift descriptors for this fragment instance.
  QueryState* const query_state_;
  const TPlanFragmentCtx* const fragment_ctx_;
  const TPlanFragmentInstanceCtx* const instance_ctx_;

  /// only populated by the (const QueryCtx&, ExecEnv*, DescriptorTbl*) c'tor
  boost::scoped_ptr<QueryState> local_query_state_;

  /// Provides instance id if instance_ctx_ == nullptr
  TUniqueId no_instance_id_;

  /// Query-global timestamps for implementing now() and utc_timestamp(). Both represent
  /// the same point in time but now_ is in local time and utc_timestamp_ is in UTC.
  /// Set from query_globals_. Use pointer to avoid inclusion of timestampvalue.h and
  /// avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;
  boost::scoped_ptr<TimestampValue> utc_timestamp_;

  /// Query-global timezone used as local timezone when executing the query.
  /// Owned by a static storage member of TimezoneDatabase class. It cannot be nullptr.
  const Timezone* local_time_zone_;

  boost::scoped_ptr<LlvmCodeGen> codegen_;

  /// Contains all ScalarExpr expressions which need to be codegen'd. The second element
  /// is true if we want to generate a codegen entry point for this expr.
  std::vector<std::pair<ScalarExpr*, bool>> scalar_exprs_to_codegen_;

  /// Thread resource management object for this fragment's execution.  The runtime
  /// state is responsible for returning this pool to the thread mgr.
  std::unique_ptr<ThreadResourcePool> resource_pool_;

  /// Execution state for DML statements.
  DmlExecState dml_exec_state_;

  RuntimeProfile* const profile_;

  /// Total time waiting in storage (across all threads)
  RuntimeProfile::Counter* total_storage_wait_timer_;

  /// Total time spent waiting for RPCs to complete. This time is a combination of:
  /// - network time of sending the RPC payload to the destination
  /// - processing and queuing time in the destination
  /// - network time of sending the RPC response to the originating node
  /// TODO: rename this counter and account for the 3 components above. IMPALA-6705.
  RuntimeProfile::Counter* total_network_send_timer_;

  /// Total time spent receiving over the network (across all threads)
  RuntimeProfile::Counter* total_network_receive_timer_;

  /// Total CPU utilization for all threads in this plan fragment.
  RuntimeProfile::ThreadCounters* total_thread_statistics_;

  /// BytesRead counters in this instance's tree, not owned.
  std::vector<RuntimeProfile::Counter*> bytes_read_counters_;

  /// Counters for bytes sent over the network in this instance's tree, not owned.
  std::vector<RuntimeProfile::Counter*> bytes_sent_counters_;

  /// Memory usage of this fragment instance, a child of 'query_mem_tracker_'. Owned by
  /// 'query_state_' and destroyed with the rest of the query's MemTracker hierarchy.
  /// See IMPALA-8270 for a reason why having the QueryState own this is important.
  MemTracker* instance_mem_tracker_ = nullptr;

  /// Buffer reservation for this fragment instance - a child of the query buffer
  /// reservation. Non-NULL if this is a finstance's RuntimeState used for query
  /// execution. Owned by 'query_state_'.
  ReservationTracker* const instance_buffer_reservation_;

  /// If true, execution should stop, either because the query was cancelled by the
  /// client, or because execution of the fragment instance is finished. If the main
  /// fragment instance thread is still running, it should terminate with a CANCELLED
  /// status once it notices is_cancelled_ == true.
  AtomicBool is_cancelled_{false};

  /// Condition variables that will be signalled by Cancel(). Protected by
  /// 'cancellation_cvs_lock_'.
  std::vector<ConditionVariable*> cancellation_cvs_;
  SpinLock cancellation_cvs_lock_;

  /// if true, ReleaseResources() was called.
  bool released_resources_ = false;

  /// Non-OK if an error has occurred and query execution should abort. Used only for
  /// asynchronously reporting such errors (e.g., when a UDF reports an error), so this
  /// will not necessarily be set in all error cases.
  SpinLock query_status_lock_;
  Status query_status_;

  /// This is the node id of the root node for this plan fragment.
  ///
  /// This is used as the hash seed within the fragment so we do not run into hash
  /// collisions after data partitioning (across fragments). See IMPALA-219 for more
  /// details.
  PlanNodeId root_node_id_ = -1;

  /// Lock protecting aux_error_info_.
  SpinLock aux_error_info_lock_;

  /// Auxiliary error information, only set if the fragment instance failed (e.g.
  /// query_status_ != Status::OK()). Owned by this RuntimeState.
  std::unique_ptr<AuxErrorInfoPB> aux_error_info_;

  /// True if aux_error_info_ has been sent in a status report, false otherwise.
  bool reported_aux_error_info_ = false;

  /// prohibit copies
  RuntimeState(const RuntimeState&);
};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status::CANCELLED; \
  } while (false)

}

#endif
