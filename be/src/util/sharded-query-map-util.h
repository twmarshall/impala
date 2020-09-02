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

#pragma once

#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "util/aligned-new.h"
#include "util/spinlock.h"
#include "util/unique-id-hash.h"

namespace impala {

/// This is a template that can be used for any map that maps from a query ID (TUniqueId
/// or UniqueIdPB) to some object, and that needs to be sharded. It provides a SpinLock
/// per shard to synchronize access to each shard of the map. The underlying shard is
/// locked and accessed by instantiating a GenericScopedShardedMapRef.
//
/// Usage pattern:
//
///   typedef ShardedQueryMap<QueryState*> QueryStateMap;
///   QueryStateMap qs_map_;
//
template <typename K, typename V>
class GenericShardedQueryMap {
 public:

  // This function takes a lambda which should take a parameter of object 'T' and
  // runs the lambda for all the entries in the map. The lambda should have a return
  // type of 'void'..
  // TODO: If necessary, refactor the lambda signature to allow returning Status objects.
  void DoFuncForAllEntries(const std::function<void(const V&)>& call) {
    for (int i = 0; i < NUM_QUERY_BUCKETS; ++i) {
      std::lock_guard<SpinLock> l(shards_[i].map_lock_);
      for (const auto& map_value_ref: shards_[i].map_) {
        call(map_value_ref.second);
      }
    }
  }

  // Adds ('key', 'value') to the map, returning an error if 'key' already exists.
  Status Add(const K& key, const V& value);

  // Removes 'key' from the map, returning an error if 'key' doesn't exist.
  Status Delete(const K& key);

 private:
  template <typename K2, typename V2>
  friend class GenericScopedShardedMapRef;

  // Number of buckets to split the containers of query IDs into.
  static constexpr uint32_t NUM_QUERY_BUCKETS = 4;

  // We group the map and its corresponding lock together to avoid false sharing. Since
  // we will always access a map and its corresponding lock together, it's better if
  // they can be allocated on the same cache line.
  struct MapShard : public CacheLineAligned {
    std::unordered_map<K, V> map_;
    SpinLock map_lock_;
  };
  struct MapShard shards_[NUM_QUERY_BUCKETS];
};

template <typename T>
class ShardedQueryMap : public GenericShardedQueryMap<TUniqueId, T> {};

template <typename T>
class ShardedQueryPBMap : public GenericShardedQueryMap<UniqueIdPB, T> {};

/// Use this class to obtain a locked reference to the underlying map shard
/// of a GenericShardedQueryMap, corresponding to the 'query_id'.
//
/// Pattern:
/// {
///   ScopedShardedMapRef map_ref(qid, sharded_map);
///   DCHECK(map_ref != nullptr);  <nullptr should never be returned>
///   ...
/// }
//
/// The caller should ensure that the lifetime of the ShardedQueryMap should be longer
/// than the lifetime of this scoped class.
template <typename K, typename V>
class GenericScopedShardedMapRef {
 public:

  // Finds the appropriate map that could/should contain 'query_id' and locks it.
  GenericScopedShardedMapRef(
      const K& query_id, class GenericShardedQueryMap<K, V>* sharded_map) {
    DCHECK(sharded_map != nullptr);
    int qs_map_bucket = QueryIdToBucket(query_id);
    shard_ = &sharded_map->shards_[qs_map_bucket];

    // Lock the corresponding shard.
    shard_->map_lock_.lock();
  }

  ~GenericScopedShardedMapRef() {
    shard_->map_lock_.DCheckLocked();
    shard_->map_lock_.unlock();
  }

  // Returns the shard (map) for the 'query_id' passed to the constructor.
  // Should never return nullptr.
  std::unordered_map<K, V>* get() {
    shard_->map_lock_.DCheckLocked();
    return &shard_->map_;
  }

  std::unordered_map<K, V>* operator->() {
    shard_->map_lock_.DCheckLocked();
    return get();
  }

 private:

  // Return the correct bucket that a query ID would belong to.
  inline int QueryIdToBucket(const TUniqueId& query_id) {
    int bucket = static_cast<int>(query_id.hi) % ShardedQueryMap<V>::NUM_QUERY_BUCKETS;
    DCHECK(bucket < ShardedQueryMap<V>::NUM_QUERY_BUCKETS && bucket >= 0);
    return bucket;
  }

  inline int QueryIdToBucket(const UniqueIdPB& query_id) {
    int bucket = static_cast<int>(query_id.hi()) % ShardedQueryMap<V>::NUM_QUERY_BUCKETS;
    DCHECK(bucket < ShardedQueryMap<V>::NUM_QUERY_BUCKETS && bucket >= 0);
    return bucket;
  }

  typename GenericShardedQueryMap<K, V>::MapShard* shard_;
  DISALLOW_COPY_AND_ASSIGN(GenericScopedShardedMapRef);
};

template <typename T>
class ScopedShardedMapRef : public GenericScopedShardedMapRef<TUniqueId, T> {
 public:
  ScopedShardedMapRef(const TUniqueId& query_id, class ShardedQueryMap<T>* sharded_map)
    : GenericScopedShardedMapRef<TUniqueId, T>(query_id, sharded_map) {}
};

template <typename T>
class ScopedShardedMapPBRef : public GenericScopedShardedMapRef<UniqueIdPB, T> {
 public:
  ScopedShardedMapPBRef(const TUniqueId& query_id, class ShardedQueryMap<T>* sharded_map)
    : GenericScopedShardedMapRef<TUniqueId, T>(query_id, sharded_map) {}
};

template <typename K, typename V>
Status GenericShardedQueryMap<K, V>::Add(const K& query_id, const V& obj) {
  GenericScopedShardedMapRef<K, V> map_ref(query_id, this);
  DCHECK(map_ref.get() != nullptr);

  auto entry = map_ref->find(query_id);
  if (entry != map_ref->end()) {
    // There shouldn't be an active query with that same id.
    // (query_id is globally unique)
    return Status(ErrorMsg(TErrorCode::INTERNAL_ERROR,
        strings::Substitute("query id $0 already exists", PrintId(query_id))));
  }
  map_ref->insert(make_pair(query_id, obj));
  return Status::OK();
}

template <typename K, typename V>
Status GenericShardedQueryMap<K, V>::Delete(const K& query_id) {
  GenericScopedShardedMapRef<K, V> map_ref(query_id, this);
  DCHECK(map_ref.get() != nullptr);
  auto entry = map_ref->find(query_id);
  if (entry == map_ref->end()) {
    Status err = Status::Expected(TErrorCode::INVALID_QUERY_HANDLE, PrintId(query_id));
    VLOG(1) << err.GetDetail();
    return err;
  }
  map_ref->erase(entry);
  return Status::OK();
}

} // namespace impala
