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

#include "runtime/io/data-cache.h"

#include <errno.h>
#include <fcntl.h>
#include <mutex>
#include <string.h>
#include <unistd.h>
#include <sstream>

#include <glog/logging.h>

#include "exec/kudu-util.h"
#include "kudu/util/async_logger.h"
#include "kudu/util/env.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/locks.h"
#include "kudu/util/path_util.h"
#include "gutil/hash/city.h"
#include "gutil/port.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/split.h"
#include "gutil/walltime.h"
#include "util/bit-util.h"
#include "util/cache/cache.h"
#include "util/error-util.h"
#include "util/filesystem-util.h"
#include "util/hash-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/scope-exit-trigger.h"
#include "util/test-info.h"
#include "util/uid-util.h"

#ifndef FALLOC_FL_PUNCH_HOLE
#include <linux/falloc.h>
#endif

#include "common/names.h"

using kudu::Env;
using kudu::faststring;
using kudu::JoinPathSegments;
using kudu::percpu_rwlock;
using kudu::RWFile;
using kudu::rw_spinlock;
using kudu::Slice;
using kudu::WritableFile;
using strings::SkipEmpty;
using strings::Split;

#ifdef NDEBUG
#define ENABLE_CHECKSUMMING (false)
#else
#define ENABLE_CHECKSUMMING (true)
#endif

DEFINE_int64(data_cache_file_max_size_bytes, 1L << 40 /* 1TB */,
    "(Advanced) The maximum size which a cache file can grow to before data stops being "
    "appended to it.");
DEFINE_int32(data_cache_max_opened_files, 1000,
    "(Advanced) The maximum number of allowed opened files. This must be at least the "
    "number of specified partitions.");
DEFINE_int32(data_cache_write_concurrency, 1,
    "(Advanced) Number of concurrent threads allowed to insert into the cache per "
    "partition.");
DEFINE_bool(data_cache_checksum, ENABLE_CHECKSUMMING,
    "(Advanced) Enable checksumming for the cached buffer.");

DEFINE_bool(data_cache_enable_tracing, false,
    "(Advanced) Collect a trace of all lookups in the data cache.");
DEFINE_bool(data_cache_anonymize_trace, false,
    "(Advanced) Use hashes of filenames rather than file paths in the data "
    "cache access trace.");

DEFINE_string(data_cache_eviction_policy, "LRU",
    "(Advanced) The cache eviction policy to use for the data cache. "
    "Either 'LRU' (default) or 'LIRS' (experimental)");

namespace impala {
namespace io {

static const int64_t PAGE_SIZE = 1L << 12;
const char* DataCache::Partition::CACHE_FILE_PREFIX = "impala-cache-file-";
const char* DataCache::Partition::TRACE_FILE_NAME = "impala-cache-trace.txt";
const int MAX_FILE_DELETER_QUEUE_SIZE = 500;
static const char* PARTITION_PATH_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.remote-data-cache-partition-$0.path";
static const char* PARTITION_READ_LATENCY_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.remote-data-cache-partition-$0.read-latency";
static const char* PARTITION_WRITE_LATENCY_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.remote-data-cache-partition-$0.write-latency";
static const char* PARTITION_EVICTION_LATENCY_METRIC_KEY_TEMPLATE =
    "impala-server.io-mgr.remote-data-cache-partition-$0.eviction-latency";


namespace {

class FileLogger;

// Simple implementation of a glog Logger that writes to a file, used for
// cache access tracing.
//
// This doesn't fully implement the Logger interface -- only the bare minimum
// to be usable with kudu::AsyncLogger.
class FileLogger : public google::base::Logger {
 public:
  explicit FileLogger(string path) : path_(std::move(path)) {}

  virtual ~FileLogger() {
    if (file_) Flush();
  }

  Status Open() {
    KUDU_RETURN_IF_ERROR(Env::Default()->NewWritableFile({}, path_, &file_),
                         "Failed to create trace log file");
    return Status::OK();
  }

  void Write(bool force_flush,
             time_t timestamp,
             const char* message,
             int message_len) override {
    buf_.append(message, message_len);
    if (force_flush || buf_.size() > kBufSize) {
      Flush();
    }
  }

  // Flush any buffered messages.
  // NOTE: declared 'final' to allow safe calls from the destructor.
  void Flush() override final {
    if (buf_.empty()) return;

    KUDU_WARN_NOT_OK(file_->Append(buf_), "Could not append to trace log");
    buf_.clear();
  }

  uint32 LogSize() override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

 private:
  const string path_;
  string buf_;
  unique_ptr<WritableFile> file_;

  static constexpr int kBufSize = 64*1024;
};

} // anonymous namespace

class DataCache::Partition::Tracer {
 public:
  explicit Tracer(string path) : underlying_logger_(new FileLogger(std::move(path))) {}

  ~Tracer() {
    if (logger_) logger_->Stop();
  }

  Status Start() {
    RETURN_IF_ERROR(underlying_logger_->Open());
    logger_.reset(new kudu::AsyncLogger(underlying_logger_.get(), 8 * 1024 * 1024));
    logger_->Start();
    return Status::OK();
  }

  enum CacheStatus {
    HIT,
    MISS,
    STORE,
    STORE_FAILED_BUSY
  };

  void Trace(CacheStatus status, const DataCache::CacheKey& key,
             int64_t lookup_len, int64_t entry_len);

 private:
  // The underlying logger that we wrap with the AsyncLogger wrapper
  // 'logger_'. NOTE: AsyncLogger consumes a raw pointer which must
  // outlive the AsyncLogger instance, so it's important that these
  // are declared in this order (logger_ must destruct before
  // underlying_logger_).
  unique_ptr<FileLogger> underlying_logger_;
  // The async wrapper around underlying_logger_ (see above).
  unique_ptr<kudu::AsyncLogger> logger_;
};


/// This class is an implementation of backing files in a cache partition.
///
/// A partition uses the interface Create() to create a backing file. A reader can read
/// from the backing file using the interface Read().
///
/// The backing file is append-only. To insert new data into the file, Allocate() is
/// called to reserve a contiguous area in the backing file. If the reservation succeeds,
/// the insertion offset is returned. Write() is called to add the data at the insertion
/// offset in the backing file. Allocations in the file may be evicted by punching hole
/// (via PunchHole()) in the backing file. The data in the hole area is reclaimed by the
/// underlying filesystem.
///
/// To avoid having too many backing files opened, old files are deleted to keep the
/// number of opened files within --data_cache_max_opened_files. Files are deleted
/// asynchronously by the file deleter thread pool. To synchronize between file deletion
/// and concurrent accesses of the file via Read()/Write()/PunchHole(), reader lock
/// is held in those functions. Before a file is deleted, Close() must be called by
/// the deleter thread, which holds the writer lock to block off all readers and sets
/// 'file_' to NULL. Read()/Write()/PunchHole() will check whether 'file_' is NULL.
/// If the file is already closed, the function will fail. On a failure of Read(), the
/// caller is expected to delete the stale cache entry. On a failure of Write(), the
/// caller is not expected to insert the cache entry. In other words, any stale cache
/// entry which references a deleted file will either be lazily erased on Read() or
/// evicted due to inactivity.
///
class DataCache::CacheFile {
 public:
  ~CacheFile() {
    // Close file if it's not closed already.
    DeleteFile();
  }

  static Status Create(std::string path, std::unique_ptr<CacheFile>* cache_file_ptr) {
    unique_ptr<CacheFile> cache_file(new CacheFile(path));
    KUDU_RETURN_IF_ERROR(kudu::Env::Default()->NewRWFile(path, &cache_file->file_),
        "Failed to create cache file");
    *cache_file_ptr = std::move(cache_file);
    return Status::OK();
  }

  // Close the underlying file so it cannot be read or written to anymore.
  void Close() {
    // Explicitly hold the lock in write mode to block all readers. This ensures that
    // setting 'file_' to NULL and 'allow_append_' to false below is atomic.
    std::unique_lock<percpu_rwlock> lock(lock_);
    // If the file is already closed, nothing to do.
    if (!file_) return;
    kudu::Status status = file_->Close();
    if (!status.ok()) {
      LOG(WARNING) << Substitute("Failed to close cache file $0: $1", path_,
          status.ToString());
    }
    file_.reset();
    allow_append_ = false;
  }

  // Close the underlying file and delete it from the filesystem.
  void DeleteFile() {
    Close();
    DCHECK(!file_);
    kudu::Status status = kudu::Env::Default()->DeleteFile(path_);
    if (!status.ok()) {
      LOG(WARNING) << Substitute("Failed to unlink $0: $1", path_, status.ToString());
    }
  }

  // Allocates a chunk of 'len' bytes in this file. The cache partition's lock
  // 'partition_lock' must be held when calling this function. Returns the byte offset
  // into the file for insertion. 'len' is expected to be multiples of PAGE_SIZE.
  // Returns -1 if the file doesn't have enough space for insertion.
  int64_t Allocate(int64_t len, const std::unique_lock<SpinLock>& partition_lock) {
    DCHECK(partition_lock.owns_lock());
    DCHECK_EQ(len % PAGE_SIZE, 0);
    int64_t current_offset = current_offset_.Load();
    DCHECK_EQ(current_offset % PAGE_SIZE, 0);
    // Hold the lock in shared mode to check if 'file_' is not closed already.
    kudu::shared_lock<rw_spinlock> lock(lock_.get_lock());
    if (!allow_append_ || (current_offset + len > FLAGS_data_cache_file_max_size_bytes &&
            current_offset > 0)) {
      allow_append_ = false;
      return -1;
    }
    DCHECK(file_);
    int64_t insertion_offset = current_offset;
    current_offset_.Add(len);
    return insertion_offset;
  }

  // Reads from byte offset 'offset' for 'bytes_to_read' bytes into 'buffer'.
  // Returns true iff read succeeded. Returns false on error or if the file
  // is already closed.
  bool Read(int64_t offset, uint8_t* buffer, int64_t bytes_to_read) {
    DCHECK_EQ(offset % PAGE_SIZE, 0);
    // Hold the lock in shared mode to check if 'file_' is not closed already.
    kudu::shared_lock<rw_spinlock> lock(lock_.get_lock());
    if (UNLIKELY(!file_)) return false;
    DCHECK_LE(offset + bytes_to_read, current_offset_.Load());
    kudu::Status status = file_->Read(offset, Slice(buffer, bytes_to_read));
    if (UNLIKELY(!status.ok())) {
      LOG(ERROR) << Substitute("Failed to read from $0 at offset $1 for $2 bytes: $3",
          path_, offset, PrettyPrinter::PrintBytes(bytes_to_read), status.ToString());
      return false;
    }
    return true;
  }

  // Writes 'buffer' of length 'buffer_len' into  byte offset 'offset' in the file.
  // Returns true iff write succeeded. Returns false on errors or if the file is
  // already closed.
  bool Write(int64_t offset, const uint8_t* buffer, int64_t buffer_len) {
    DCHECK_EQ(offset % PAGE_SIZE, 0);
    DCHECK_LE(offset, current_offset_.Load());
    // Hold the lock in shared mode to check if 'file_' is not closed already.
    kudu::shared_lock<rw_spinlock> lock(lock_.get_lock());
    if (UNLIKELY(!file_)) return false;
    DCHECK_LE(offset + buffer_len, current_offset_.Load());
    kudu::Status status = file_->Write(offset, Slice(buffer, buffer_len));
    if (UNLIKELY(!status.ok())) {
      LOG(ERROR) << Substitute("Failed to write to $0 at offset $1 for $2 bytes: $3",
          path_, offset, PrettyPrinter::PrintBytes(buffer_len), status.ToString());
      return false;
    }
    return true;
  }

  void PunchHole(int64_t offset, int64_t hole_size) {
    DCHECK_EQ(offset % PAGE_SIZE, 0);
    DCHECK_EQ(hole_size % PAGE_SIZE, 0);
    // Hold the lock in shared mode to check if 'file_' is not closed already.
    kudu::shared_lock<rw_spinlock> lock(lock_.get_lock());
    if (UNLIKELY(!file_)) return;
    DCHECK_LE(offset + hole_size, current_offset_.Load());
    kudu::Status status = file_->PunchHole(offset, hole_size);
    if (UNLIKELY(!status.ok())) {
      LOG(DFATAL) << Substitute("Failed to punch hole in $0 at offset $1 for $2 $3",
          path_, offset, PrettyPrinter::PrintBytes(hole_size), status.ToString());
    }
  }

  const string& path() const { return path_; }

 private:
  /// Full path of the backing file in the local storage.
  const string path_;

  /// The underlying backing file. NULL if the file has been closed.
  unique_ptr<RWFile> file_;

  /// True iff it's okay to append to this backing file.
  bool allow_append_ = true;

  /// The current offset in the file to append to on next insert.
  AtomicInt64 current_offset_;

  /// This is a reader-writer lock used for synchronization with the deleter thread.
  /// It is taken in write mode in Close() and shared mode everywhere else. It's expected
  /// that all places except for Close() check that 'file_' is not NULL with the lock held
  /// in shared mode while Close() ensures that no thread is holding the lock in shared
  /// mode so it's safe to close the file. The file can no longer be read, written or hole
  /// punched after it has been closed. The only operation allowed is to deletion.
  percpu_rwlock lock_;

  /// C'tor of CacheFile to be called by Create() only.
  explicit CacheFile(std::string path) : path_(move(path)) { }

  DISALLOW_COPY_AND_ASSIGN(CacheFile);
};

/// An entry in the metadata cache in a partition.
/// Contains the whereabouts of the cached content.
class DataCache::CacheEntry {
 public:
  explicit CacheEntry(CacheFile* file, int64_t offset, int64_t len, uint64_t checksum)
    : file_(file), offset_(offset), len_(len), checksum_(checksum) {
  }

  // Unpack a cache's entry represented by 'slice'. This is done in place of casting
  // to avoid any potential alignment issue.
  explicit CacheEntry(const Slice& value) {
    DCHECK_EQ(value.size(), sizeof(CacheEntry));
    memcpy(this, value.data(), value.size());
  }

  CacheFile* file() const { return file_; }
  int64_t offset() const { return offset_; }
  int64_t len() const { return len_; }
  uint64_t checksum() const { return checksum_; }

 private:
  /// The backing file holding the cached content.
  CacheFile* const file_ = nullptr;

  /// The starting byte offset in the backing file at which the content is stored.
  const int64_t offset_ = 0;

  /// The length in bytes of the cached content.
  const int64_t len_ = 0;

  /// Optional checksum of the content computed when inserting the cache entry.
  const uint64_t checksum_ = 0;
};

/// The key used for look up in the cache.
struct DataCache::CacheKey {
 public:
  explicit CacheKey(const string& filename, int64_t mtime, int64_t offset)
    : key_(filename.size() + sizeof(mtime) + sizeof(offset)) {
    DCHECK_GE(mtime, 0);
    DCHECK_GE(offset, 0);
    key_.append(&mtime, sizeof(mtime));
    key_.append(&offset, sizeof(offset));
    key_.append(filename);
  }

  int64_t Hash() const {
    return HashUtil::FastHash64(key_.data(), key_.size(), 0);
  }

  Slice filename() const {
    return Slice(key_.data() + OFFSETOF_FILENAME, key_.size() - OFFSETOF_FILENAME);
  }

  int64_t mtime() const {
    return UNALIGNED_LOAD64(key_.data() + OFFSETOF_MTIME);
  }

  int64_t offset() const {
    return UNALIGNED_LOAD64(key_.data() + OFFSETOF_OFFSET);
  }

  Slice ToSlice() const {
    return key_;
  }

 private:
  // Key encoding stored in key_:
  //
  //  int64_t mtime;
  //  int64_t offset;
  //  <variable length bytes> filename;
  static constexpr int OFFSETOF_MTIME = 0;
  static constexpr int OFFSETOF_OFFSET = OFFSETOF_MTIME + sizeof(int64_t);
  static constexpr int OFFSETOF_FILENAME = OFFSETOF_OFFSET + sizeof(int64_t);
  faststring key_;
};

static Cache::EvictionPolicy GetCacheEvictionPolicy(const std::string& policy_string) {
  Cache::EvictionPolicy policy = Cache::ParseEvictionPolicy(policy_string);
  if (policy != Cache::EvictionPolicy::LRU && policy != Cache::EvictionPolicy::LIRS) {
    LOG(FATAL) << "Unsupported eviction policy: " << policy_string;
  }
  return policy;
}

DataCache::Partition::Partition(
    int32_t index, const string& path, int64_t capacity, int max_opened_files)
  : index_(index),
    path_(path),
    capacity_(max<int64_t>(capacity, PAGE_SIZE)),
    max_opened_files_(max_opened_files),
    meta_cache_(NewCache(GetCacheEvictionPolicy(FLAGS_data_cache_eviction_policy),
        capacity_, path_)) {}

DataCache::Partition::~Partition() {
  if (!closed_) ReleaseResources();
}

Status DataCache::Partition::CreateCacheFile() {
  lock_.DCheckLocked();
  const string& path =
      JoinPathSegments(path_, CACHE_FILE_PREFIX + PrintId(GenerateUUID()));
  unique_ptr<CacheFile> cache_file;
  RETURN_IF_ERROR(CacheFile::Create(path, &cache_file));
  cache_files_.emplace_back(std::move(cache_file));
  LOG(INFO) << "Created cache file " << path;
  return Status::OK();
}

Status DataCache::Partition::DeleteExistingFiles() const {
  vector<string> entries;
  RETURN_IF_ERROR(FileSystemUtil::Directory::GetEntryNames(path_, &entries, 0,
      FileSystemUtil::Directory::EntryType::DIR_ENTRY_REG));
  for (const string& entry : entries) {
    if (entry.find(CACHE_FILE_PREFIX) == 0) {
      const string file_path = JoinPathSegments(path_, entry);
      KUDU_RETURN_IF_ERROR(kudu::Env::Default()->DeleteFile(file_path),
          Substitute("Failed to delete old cache file $0", file_path));
      LOG(INFO) << Substitute("Deleted old cache file $0", file_path);
    }
  }
  return Status::OK();
}

Status DataCache::Partition::Init() {
  std::unique_lock<SpinLock> partition_lock(lock_);

  RETURN_IF_ERROR(meta_cache_->Init());

  // Verify the validity of the path specified.
  if (!FileSystemUtil::IsCanonicalPath(path_)) {
    return Status(Substitute("$0 is not a canonical path", path_));
  }
  RETURN_IF_ERROR(FileSystemUtil::VerifyIsDirectory(path_));

  // Delete all existing backing files left over from previous runs.
  RETURN_IF_ERROR(DeleteExistingFiles());

  // Check if there is enough space available at this point in time.
  uint64_t available_bytes;
  RETURN_IF_ERROR(FileSystemUtil::GetSpaceAvailable(path_, &available_bytes));
  if (available_bytes < capacity_) {
    const string& err = Substitute("Insufficient space for $0. Required $1. Only $2 is "
        "available", path_, PrettyPrinter::PrintBytes(capacity_),
        PrettyPrinter::PrintBytes(available_bytes));
    LOG(ERROR) << err;
    return Status(err);
  }

  // Make sure hole punching is supported for the caching directory.
  RETURN_IF_ERROR(FileSystemUtil::CheckHolePunch(path_));

  if (FLAGS_data_cache_enable_tracing) {
    tracer_.reset(new Tracer(path_ + "/" + TRACE_FILE_NAME));
    RETURN_IF_ERROR(tracer_->Start());
  }

  // Create metrics for this partition
  InitMetrics();

  // Create a backing file for the partition.
  RETURN_IF_ERROR(CreateCacheFile());
  oldest_opened_file_ = 0;
  return Status::OK();
}

void DataCache::Partition::InitMetrics() {
  const string& i_string = Substitute("$0", index_);
  // Backend tests may instantiate the data cache (and its associated partitions)
  // more than once. If the metrics already exist, then we just need to look up the
  // metrics to populate the partition's fields.
  if (TestInfo::is_test() &&
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<StringProperty>(
          Substitute(PARTITION_PATH_METRIC_KEY_TEMPLATE, i_string)) != nullptr) {
    // If the partition path metric already initialized, then all the other metrics
    // must be initialized.
    read_latency_ =
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          Substitute(PARTITION_READ_LATENCY_METRIC_KEY_TEMPLATE, i_string));
    write_latency_ =
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          Substitute(PARTITION_WRITE_LATENCY_METRIC_KEY_TEMPLATE, i_string));
    eviction_latency_ =
      ImpaladMetrics::IO_MGR_METRICS->FindMetricForTesting<HistogramMetric>(
          Substitute(PARTITION_EVICTION_LATENCY_METRIC_KEY_TEMPLATE, i_string));
    DCHECK(read_latency_ != nullptr);
    DCHECK(write_latency_ != nullptr);
    DCHECK(eviction_latency_ != nullptr);
    return;
  }
  // Two cases:
  // - This is not a backend test, so metrics should only be initialized once.
  // - This is a backend test, but none of the metrics have been initialized before.
  DCHECK(read_latency_ == nullptr);
  DCHECK(write_latency_ == nullptr);
  DCHECK(eviction_latency_ == nullptr);
  int64_t ONE_HOUR_IN_NS = 60L * 60L * NANOS_PER_SEC;
  ImpaladMetrics::IO_MGR_METRICS->AddProperty<string>(
      PARTITION_PATH_METRIC_KEY_TEMPLATE, path_, i_string);
  read_latency_ = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
      MetricDefs::Get(PARTITION_READ_LATENCY_METRIC_KEY_TEMPLATE, i_string),
      ONE_HOUR_IN_NS, 3));
  write_latency_ = ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
      MetricDefs::Get(PARTITION_WRITE_LATENCY_METRIC_KEY_TEMPLATE, i_string),
      ONE_HOUR_IN_NS, 3));
  eviction_latency_ =
      ImpaladMetrics::IO_MGR_METRICS->RegisterMetric(new HistogramMetric(
          MetricDefs::Get(PARTITION_EVICTION_LATENCY_METRIC_KEY_TEMPLATE, i_string),
          ONE_HOUR_IN_NS, 3));
}

Status DataCache::Partition::CloseFilesAndVerifySizes() {
  int64_t total_size = 0;
  for (auto& file : cache_files_) {
    uint64_t sz_on_disk;
    // Close the backing files before checking sizes as some filesystems (e.g. XFS)
    // preallocate the file beyond EOF. Closing the file removes any preallocation.
    file->Close();
    kudu::Env* env = kudu::Env::Default();
    KUDU_RETURN_IF_ERROR(env->GetFileSizeOnDisk(file->path(), &sz_on_disk),
        "CloseFilesAndVerifySizes()");
    total_size += sz_on_disk;
    uint64_t logical_sz;
    KUDU_RETURN_IF_ERROR(env->GetFileSize(file->path(), &logical_sz),
        "CloseFilesAndVerifySizes()");
    DCHECK_LE(logical_sz, FLAGS_data_cache_file_max_size_bytes);
  }
  if (total_size > capacity_) {
    return Status(Substitute("Partition $0 consumed $1 bytes, exceeding capacity of $2 "
        "bytes", path_, total_size, capacity_));
  }
  return Status::OK();
}

void DataCache::Partition::ReleaseResources() {
  std::unique_lock<SpinLock> partition_lock(lock_);
  if (closed_) return;
  closed_ = true;
  // Close and delete all backing files in this partition.
  cache_files_.clear();
  // Free all memory consumed by the metadata cache.
  meta_cache_.reset();
}

int64_t DataCache::Partition::Lookup(const CacheKey& cache_key, int64_t bytes_to_read,
    uint8_t* buffer) {
  DCHECK(!closed_);
  Slice key = cache_key.ToSlice();
  Cache::UniqueHandle handle(meta_cache_->Lookup(key));

  if (handle.get() == nullptr) {
    if (tracer_ != nullptr) {
      tracer_->Trace(Tracer::MISS, cache_key, bytes_to_read, /*entry_len=*/-1);
    }
    return 0;
  }

  // Read from the backing file.
  CacheEntry entry(meta_cache_->Value(handle));

  if (tracer_ != nullptr) {
    tracer_->Trace(Tracer::HIT, cache_key, bytes_to_read, entry.len());
  }

  CacheFile* cache_file = entry.file();
  bytes_to_read = min(entry.len(), bytes_to_read);
  VLOG(3) << Substitute("Reading file $0 offset $1 len $2 checksum $3 bytes_to_read $4",
      cache_file->path(), entry.offset(), entry.len(), entry.checksum(), bytes_to_read);
  bool read_success;
  {
    ScopedHistogramTimer read_timer(read_latency_);
    read_success = cache_file->Read(entry.offset(), buffer, bytes_to_read);
  }
  if (UNLIKELY(!read_success)) {
    meta_cache_->Erase(key);
    return 0;
  }

  // Verify checksum if enabled. Delete entry on checksum mismatch.
  if (FLAGS_data_cache_checksum && bytes_to_read == entry.len() &&
      !VerifyChecksum("read", entry, buffer, bytes_to_read)) {
    meta_cache_->Erase(key);
    return 0;
  }
  return bytes_to_read;
}

bool DataCache::Partition::HandleExistingEntry(const Slice& key,
    const Cache::UniqueHandle& handle, const uint8_t* buffer, int64_t buffer_len) {
  // Unpack the cache entry.
  CacheEntry entry(meta_cache_->Value(handle));

  // Try verifying the checksum of the new buffer matches that of the existing entry.
  // On checksum mismatch, delete the existing entry and don't install the new entry
  // as it's unclear which one is right.
  if (FLAGS_data_cache_checksum && buffer_len >= entry.len()) {
    if (!VerifyChecksum("write", entry, buffer, buffer_len)) {
      meta_cache_->Erase(key);
      return true;
    }
  }
  // If the new entry is not any longer than the existing entry, no work to do.
  return entry.len() >= buffer_len;
}

bool DataCache::Partition::InsertIntoCache(const Slice& key, CacheFile* cache_file,
    int64_t insertion_offset, const uint8_t* buffer, int64_t buffer_len) {
  DCHECK_EQ(insertion_offset % PAGE_SIZE, 0);
  const int64_t charge_len = BitUtil::RoundUp(buffer_len, PAGE_SIZE);

  // Allocate a cache handle
  Cache::UniquePendingHandle pending_handle(
      meta_cache_->Allocate(key, sizeof(CacheEntry), charge_len));
  if (UNLIKELY(pending_handle.get() == nullptr)) return false;

  // Compute checksum if necessary.
  int64_t checksum = FLAGS_data_cache_checksum ? Checksum(buffer, buffer_len) : 0;

  // Write to backing file.
  VLOG(3) << Substitute("Storing file $0 offset $1 len $2 checksum $3 ",
      cache_file->path(), insertion_offset, buffer_len, checksum);
  bool write_success;
  {
    ScopedHistogramTimer write_timer(write_latency_);
    write_success = cache_file->Write(insertion_offset, buffer, buffer_len);
  }
  if (UNLIKELY(!write_success)) {
    return false;
  }

  // Insert the new entry into the cache.
  CacheEntry entry(cache_file, insertion_offset, buffer_len, checksum);
  memcpy(meta_cache_->MutableValue(&pending_handle), &entry, sizeof(CacheEntry));
  Cache::UniqueHandle handle(meta_cache_->Insert(std::move(pending_handle), this));
  // Check for failure of Insert(), which means the entry was evicted during Insert()
  if (UNLIKELY(handle.get() == nullptr)){
    ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_INSTANT_EVICTIONS->Increment(1);
    return false;
  }
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES->Increment(charge_len);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_NUM_ENTRIES->Increment(1);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_NUM_WRITES->Increment(1);
  return true;
}

bool DataCache::Partition::Store(const CacheKey& cache_key, const uint8_t* buffer,
    int64_t buffer_len, bool* start_reclaim) {
  DCHECK(!closed_);
  *start_reclaim = false;
  Slice key = cache_key.ToSlice();
  const int64_t charge_len = BitUtil::RoundUp(buffer_len, PAGE_SIZE);
  if (charge_len > capacity_) return false;

  // Check for existing entry.
  {
    Cache::UniqueHandle handle(meta_cache_->Lookup(key, Cache::NO_UPDATE));
    if (handle.get() != nullptr) {
      if (HandleExistingEntry(key, handle, buffer, buffer_len)) return false;
    }
  }

  CacheFile* cache_file;
  int64_t insertion_offset;
  {
    std::unique_lock<SpinLock> partition_lock(lock_);

    // Limit the write concurrency to avoid blocking the caller (which could be calling
    // from the critical path of an IO read) when the cache becomes IO bound due to either
    // limited memory for page cache or the cache is undersized which leads to eviction.
    //
    // TODO: defer the writes to another thread which writes asynchronously. Need to bound
    // the extra memory consumption for holding the temporary buffer though.
    const bool exceed_concurrency =
        pending_insert_set_.size() >= FLAGS_data_cache_write_concurrency;
    if (exceed_concurrency ||
        pending_insert_set_.find(key.ToString()) != pending_insert_set_.end()) {
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_DROPPED_BYTES->Increment(buffer_len);
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_DROPPED_ENTRIES->Increment(1);
      if (tracer_ != nullptr) {
        tracer_->Trace(Tracer::STORE_FAILED_BUSY, cache_key, /*lookup_len=*/-1,
            buffer_len);
      }
      return false;
    }

    // Allocate from the backing file.
    CHECK(!cache_files_.empty());
    cache_file = cache_files_.back().get();
    insertion_offset = cache_file->Allocate(charge_len, partition_lock);
    // Create and append to a new file if necessary.
    if (UNLIKELY(insertion_offset < 0)) {
      if (!CreateCacheFile().ok()) return false;
      cache_file = cache_files_.back().get();
      insertion_offset = cache_file->Allocate(charge_len, partition_lock);
      if (UNLIKELY(insertion_offset < 0)) return false;
    }

    // Start deleting old files if there are too many opened.
    *start_reclaim = cache_files_.size() > max_opened_files_;

    // Do this last. At this point, we are committed to inserting 'key' into the cache.
    pending_insert_set_.emplace(key.ToString());
  }

  if (tracer_ != nullptr) {
    tracer_->Trace(Tracer::STORE, cache_key, /* lookup_len=*/-1, buffer_len);
  }

  // Set up a scoped exit to always remove entry from the pending insertion set.
  auto remove_from_pending_set = MakeScopeExitTrigger([this, &key]() {
    std::unique_lock<SpinLock> partition_lock(lock_);
    pending_insert_set_.erase(key.ToString());
  });

  // Try inserting into the cache.
  return InsertIntoCache(key, cache_file, insertion_offset, buffer, buffer_len);
}

void DataCache::Partition::DeleteOldFiles() {
  std::unique_lock<SpinLock> partition_lock(lock_);
  DCHECK_GE(oldest_opened_file_, 0);
  int target = cache_files_.size() - FLAGS_data_cache_max_opened_files;
  while (oldest_opened_file_ < target) {
    cache_files_[oldest_opened_file_++]->DeleteFile();
  }
}

void DataCache::Partition::EvictedEntry(Slice key, Slice value) {
  if (closed_) return;
  ScopedHistogramTimer eviction_timer(eviction_latency_);
  // Unpack the cache entry.
  CacheEntry entry(value);
  int64_t eviction_len = BitUtil::RoundUp(entry.len(), PAGE_SIZE);
  DCHECK_EQ(entry.offset() % PAGE_SIZE, 0);
  entry.file()->PunchHole(entry.offset(), eviction_len);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES->Increment(-eviction_len);
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_NUM_ENTRIES->Increment(-1);
}

// TODO: Switch to using CRC32 once we fix the TODO in hash-util.h
uint64_t DataCache::Partition::Checksum(const uint8_t* buffer, int64_t buffer_len) {
  return HashUtil::FastHash64(buffer, buffer_len, 0xcafebeef);
}

bool DataCache::Partition::VerifyChecksum(const string& ops_name, const CacheEntry& entry,
    const uint8_t* buffer, int64_t buffer_len) {
  DCHECK(FLAGS_data_cache_checksum);
  DCHECK_GE(buffer_len, entry.len());
  int64_t checksum = Checksum(buffer, entry.len());
  if (UNLIKELY(checksum != entry.checksum())) {
    LOG(DFATAL) << Substitute("Checksum mismatch during $0 for file $1 "
        "offset: $2 len: $3 buffer len: $4. Expected $5, Got $6.", ops_name,
        entry.file()->path(), entry.offset(), entry.len(), buffer_len, entry.checksum(),
        checksum);
    return false;
  }
  return true;
}

Status DataCache::Init() {
  // Verifies all the configured flags are sane.
  if (FLAGS_data_cache_file_max_size_bytes < PAGE_SIZE) {
    return Status(Substitute("Misconfigured --data_cache_file_max_size_bytes: $0 bytes. "
        "Must be at least $1 bytes", FLAGS_data_cache_file_max_size_bytes, PAGE_SIZE));
  }
  if (FLAGS_data_cache_write_concurrency < 1) {
    return Status(Substitute("Misconfigured --data_cache_write_concurrency: $0. "
        "Must be at least 1.", FLAGS_data_cache_write_concurrency));
  }

  // The expected form of the configuration string is: dir1,dir2,..,dirN:capacity
  // Example: /tmp/data1,/tmp/data2:1TB
  vector<string> all_cache_configs = Split(config_, ":", SkipEmpty());
  if (all_cache_configs.size() != 2) {
    return Status(Substitute("Malformed data cache configuration $0", config_));
  }

  // Parse the capacity string to make sure it's well-formed.
  bool is_percent;
  int64_t capacity = ParseUtil::ParseMemSpec(all_cache_configs[1], &is_percent, 0);
  if (is_percent) {
    return Status(Substitute("Malformed data cache capacity configuration $0",
        all_cache_configs[1]));
  }
  if (capacity < PAGE_SIZE) {
    return Status(Substitute("Configured data cache capacity $0 is too small",
        all_cache_configs[1]));
  }

  set<string> cache_dirs;
  SplitStringToSetUsing(all_cache_configs[0], ",", &cache_dirs);
  int max_opened_files_per_partition =
      FLAGS_data_cache_max_opened_files / cache_dirs.size();
  if (max_opened_files_per_partition < 1) {
    return Status(Substitute("Misconfigured --data_cache_max_opened_files: $0. Must be "
        "at least $1.", FLAGS_data_cache_max_opened_files, cache_dirs.size()));
  }
  int32_t partition_idx = 0;
  for (const string& dir_path : cache_dirs) {
    LOG(INFO) << "Adding partition " << dir_path << " with capacity "
              << PrettyPrinter::PrintBytes(capacity);
    std::unique_ptr<Partition> partition =
        make_unique<Partition>(partition_idx, dir_path, capacity,
            max_opened_files_per_partition);
    RETURN_IF_ERROR(partition->Init());
    partitions_.emplace_back(move(partition));
    ++partition_idx;
  }
  CHECK_GT(partitions_.size(), 0);

  // Starts a thread pool which deletes old files from partitions. DataCache::Store()
  // will enqueue a request (i.e. a partition index) when it notices the number of files
  // in a partition exceeds the per-partition limit. The files in a partition will be
  // closed in the order they are created until it's within the per-partition limit.
  file_deleter_pool_.reset(new ThreadPool<int>("impala-server",
      "data-cache-file-deleter", 1, MAX_FILE_DELETER_QUEUE_SIZE,
      bind<void>(&DataCache::DeleteOldFiles, this, _1, _2)));
  RETURN_IF_ERROR(file_deleter_pool_->Init());

  return Status::OK();
}

void DataCache::ReleaseResources() {
  if (file_deleter_pool_) file_deleter_pool_->Shutdown();
  for (auto& partition : partitions_) partition->ReleaseResources();
}

int64_t DataCache::Lookup(const string& filename, int64_t mtime, int64_t offset,
    int64_t bytes_to_read, uint8_t* buffer) {
  DCHECK(!partitions_.empty());
  // Bail out early for uncacheable ranges or invalid requests.
  if (mtime < 0 || offset < 0 || bytes_to_read < 0) {
    VLOG(3) << Substitute("Skipping lookup of invalid entry $0 mtime: $1 offset: $2 "
         "bytes_to_read: $3", filename, mtime, offset, bytes_to_read);
    return 0;
  }

  // Construct a cache key. The cache key is also hashed to compute the partition index.
  const CacheKey key(filename, mtime, offset);
  int idx = key.Hash() % partitions_.size();
  int64_t bytes_read = partitions_[idx]->Lookup(key, bytes_to_read, buffer);
  if (VLOG_IS_ON(3)) {
    stringstream ss;
    ss << std::hex << reinterpret_cast<int64_t>(buffer);
    LOG(INFO) << Substitute("Looking up $0 mtime: $1 offset: $2 bytes_to_read: $3 "
        "buffer: 0x$4 bytes_read: $5", filename, mtime, offset, bytes_to_read,
        ss.str(), bytes_read);
  }
  return bytes_read;
}

bool DataCache::Store(const string& filename, int64_t mtime, int64_t offset,
    const uint8_t* buffer, int64_t buffer_len) {
  DCHECK(!partitions_.empty());
  // Bail out early for uncacheable ranges or invalid requests.
  if (mtime < 0 || offset < 0 || buffer_len < 0) {
    VLOG(3) << Substitute("Skipping insertion of invalid entry $0 mtime: $1 offset: $2 "
         "buffer_len: $3", filename, mtime, offset, buffer_len);
    return false;
  }

  // Construct a cache key. The cache key is also hashed to compute the partition index.
  const CacheKey key(filename, mtime, offset);
  int idx = key.Hash() % partitions_.size();
  bool start_reclaim;
  bool stored = partitions_[idx]->Store(key, buffer, buffer_len, &start_reclaim);
  if (VLOG_IS_ON(3)) {
    stringstream ss;
    ss << std::hex << reinterpret_cast<int64_t>(buffer);
    LOG(INFO) << Substitute("Storing $0 mtime: $1 offset: $2 bytes_to_read: $3 "
        "buffer: 0x$4 stored: $5", filename, mtime, offset, buffer_len, ss.str(), stored);
  }
  if (start_reclaim) file_deleter_pool_->Offer(idx);
  return stored;
}

Status DataCache::CloseFilesAndVerifySizes() {
  for (auto& partition : partitions_) {
    RETURN_IF_ERROR(partition->CloseFilesAndVerifySizes());
  }
  return Status::OK();
}

void DataCache::DeleteOldFiles(uint32_t thread_id, int partition_idx) {
  DCHECK_LT(partition_idx, partitions_.size());
  partitions_[partition_idx]->DeleteOldFiles();
}

void DataCache::Partition::Tracer::Trace(
    CacheStatus status, const DataCache::CacheKey& key,
    int64_t lookup_len, int64_t entry_len) {

  ostringstream buf;
  kudu::JsonWriter jw(&buf, kudu::JsonWriter::COMPACT);

  jw.StartObject();
  jw.String("ts");
  jw.Double(WallTime_Now());
  jw.String("s");
  switch (status) {
    case HIT: jw.String("H"); break;
    case MISS: jw.String("M"); break;
    case STORE: jw.String("S"); break;
    case STORE_FAILED_BUSY: jw.String("F"); break;
  }

  jw.String("f");
  if (FLAGS_data_cache_anonymize_trace) {
    uint128 hash = util_hash::CityHash128(
        reinterpret_cast<const char*>(key.filename().data()),
        key.filename().size());
    // A 128-bit (16-byte) hash results in a 24-byte base64-encoded string, including
    // two characters of padding.
    const int BUF_LEN = 24;
    DCHECK_EQ(BUF_LEN, CalculateBase64EscapedLen(sizeof(hash)));
    char b64_buf[BUF_LEN];
    int out_len = Base64Escape(reinterpret_cast<const unsigned char*>(&hash),
        sizeof(hash), b64_buf, BUF_LEN);
    DCHECK_EQ(out_len, BUF_LEN);
    // Chop off the two padding bytes.
    DCHECK(b64_buf[23] == '=' && b64_buf[22] == '=');
    out_len = 22;
    jw.String(b64_buf, out_len);
  } else {
    jw.String(reinterpret_cast<const char*>(key.filename().data()),
              key.filename().size());
  }
  jw.String("m");
  jw.Int64(key.mtime());
  jw.String("o");
  jw.Int64(key.offset());

  if (lookup_len != -1) {
    jw.String("lLen");
    jw.Int64(lookup_len);
  }
  if (entry_len != -1) {
    jw.String("eLen");
    jw.Int64(entry_len);
  }
  jw.EndObject();
  buf << "\n";

  string s = buf.str();
  logger_->Write(/*force_flush=*/false, /*timestamp=*/0, s.data(), s.size());
}

} // namespace io
} // namespace impala
