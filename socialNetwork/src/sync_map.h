#pragma once

#ifndef SOCIAL_NETWORK_MICROSERVICES_SYNC_MAP_H
#define SOCIAL_NETWORK_MICROSERVICES_SYNC_MAP_H

#include <unordered_map>
#include <shared_mutex>

constexpr int KPowOfShards = 24;

template <typename Key, typename Tp, typename Lock = std::shared_timed_mutex>
class sync_map {
  typedef std::unique_lock<Lock> WriteLock;
  typedef std::shared_lock<Lock> ReadLock;

public:
  void init() {
    _map.rehash(1 << KPowOfShards);
  }

  Tp &at(const Key &k) {
    auto hash_fn = _map.hash_function();
    auto &_lock = _locks[hash_fn(k) % (1 << KPowOfShards)];
    ReadLock r_lock(_lock);
    return _map.at(k);
  }

  Tp &operator[](const Key &k) {
    auto hash_fn = _map.hash_function();
    auto &_lock = _locks[hash_fn(k) % (1 << KPowOfShards)];
    ReadLock r_lock(_lock);
    return _map[k];
  }

  void emplace(const Key &k, const Tp &v) {
    auto hash_fn = _map.hash_function();
    auto &_lock = _locks[hash_fn(k) % (1 << KPowOfShards)];
    WriteLock w_lock(_lock);
    _map.emplace(k, v);
  }

  bool find(const Key &k) {
    return _map.find(k) != _map.end();
  }

private:
  Lock _locks[1 << KPowOfShards];
  std::unordered_map<Key, Tp> _map;
};

#endif // SOCIAL_NETWORK_MICROSERVICES_SYNC_MAP_H