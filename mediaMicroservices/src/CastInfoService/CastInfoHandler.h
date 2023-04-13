#ifndef MEDIA_MICROSERVICES_CASTINFOHANDLER_H
#define MEDIA_MICROSERVICES_CASTINFOHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>

#include "../../gen-cpp/CastInfoService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

// [Midas]
#include "cache_manager.hpp"
#include "sync_kv.hpp"
#include "time.hpp"
constexpr static uint64_t kNumBuckets = 1 << 20;


namespace media_service {

class CastInfoHandler : public CastInfoServiceIf {
 public:
  CastInfoHandler(
      // memcached_pool_st *,
      mongoc_client_pool_t *);
  ~CastInfoHandler() override = default;

  void WriteCastInfo(int64_t req_id, int64_t cast_info_id,
      const std::string &name, bool gender, const std::string &intro,
      const std::map<std::string, std::string>& carrier) override;

  void ReadCastInfo(std::vector<CastInfo>& _return, int64_t req_id,
      const std::vector<int64_t> & cast_info_ids,
      const std::map<std::string, std::string>& carrier) override;

 private:
  midas::CachePool *_pool;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> _cast_info_cache;
  mongoc_client_pool_t *_mongodb_client_pool;
};

CastInfoHandler::CastInfoHandler(
    mongoc_client_pool_t *mongodb_client_pool) {
  
  auto cmanager = midas::CacheManager::global_cache_manager();
  if (!cmanager->create_pool("castinfo") ||
      (_pool = cmanager->get_pool("castinfo")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool";
    throw se;
  }
  // _pool->update_limit(5ull * 1024 * 1024 * 1024); // ~1GB
  // _pool->update_limit(342ull * 1024 * 1024); // ~1GB
  _pool->update_limit(1217ull * 1024 * 1024); // ~1GB
  _cast_info_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);

  _mongodb_client_pool = mongodb_client_pool;
}
void CastInfoHandler::WriteCastInfo(
    int64_t req_id,
    int64_t cast_info_id,
    const std::string &name,
    bool gender,
    const std::string &intro,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "WriteCastInfo",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  bson_t *new_doc = bson_new();
  BSON_APPEND_INT64(new_doc, "cast_info_id", cast_info_id);
  BSON_APPEND_UTF8(new_doc, "name", name.c_str());
  BSON_APPEND_BOOL(new_doc, "gender", gender);
  BSON_APPEND_UTF8(new_doc, "intro", intro.c_str());

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
      _mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "cast-info", "cast-info");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection cast-info from DB cast-info";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_error_t error;
  auto insert_span = opentracing::Tracer::Global()->StartSpan(
      "MongoInsertCastInfo", { opentracing::ChildOf(&span->context()) });
  bool plotinsert = mongoc_collection_insert_one (
      collection, new_doc, nullptr, nullptr, &error);
  insert_span->Finish();
  if (!plotinsert) {
    LOG(error) << "Error: Failed to insert cast-info to MongoDB: "
               << error.message;
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = error.message;
    bson_destroy(new_doc);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_destroy(new_doc);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  span->Finish();
}

void CastInfoHandler::ReadCastInfo(
    std::vector<CastInfo> &_return,
    int64_t req_id,
    const std::vector<int64_t> &cast_info_ids,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadCastInfo",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (cast_info_ids.empty()) {
    return;
  }

  std::set<int64_t> cast_info_ids_not_cached(cast_info_ids.begin(), cast_info_ids.end());
  if (cast_info_ids_not_cached.size() != cast_info_ids.size()) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "cast_info_ids are duplicated";
    throw se;
  }

  std::map<int64_t, CastInfo> return_map;
  for (auto &cast_info_id : cast_info_ids) {
    size_t return_value_length = 0;
    char *return_value = reinterpret_cast<char *>(
        _cast_info_cache->get(&cast_info_id, sizeof(cast_info_id), &return_value_length));
    if (return_value) {
      CastInfo new_cast_info;
      json cast_info_json = json::parse(std::string(
          return_value, return_value + return_value_length));
      new_cast_info.cast_info_id = cast_info_json["cast_info_id"];
      new_cast_info.gender = cast_info_json["gender"];
      new_cast_info.name = cast_info_json["name"];
      new_cast_info.intro = cast_info_json["intro"];
      return_map.insert(std::make_pair(new_cast_info.cast_info_id, new_cast_info));
      cast_info_ids_not_cached.erase(new_cast_info.cast_info_id);
      free(return_value);
    }
  }

  std::map<int64_t, std::string> cast_info_json_map;

  // Find the rest in MongoDB
  if (!cast_info_ids_not_cached.empty()) {
    auto missed_cycles_stt = midas::Time::get_cycles_stt();


    bson_t *query = bson_new();
    bson_t query_child;
    bson_t query_cast_info_id_list;
    const char *key;
    int idx = 0;
    char buf[16];
    BSON_APPEND_DOCUMENT_BEGIN(query, "cast_info_id", &query_child);
    BSON_APPEND_ARRAY_BEGIN(&query_child, "$in", &query_cast_info_id_list);
    for (auto &item : cast_info_ids_not_cached) {
      bson_uint32_to_string(idx, &key, buf, sizeof buf);
      BSON_APPEND_INT64(&query_cast_info_id_list, key, item);
      idx++;
    }
    bson_append_array_end(&query_child, &query_cast_info_id_list);
    bson_append_document_end(query, &query_child);

    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "cast-info", "cast-info");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB user";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindCastInfo", {opentracing::ChildOf(&span->context())});

    while (true) {
      bool found = mongoc_cursor_next(cursor, &doc);
      if (!found) {
        break;
      }
      bson_iter_t iter;
      CastInfo new_cast_info;
      char *cast_info_json_char = bson_as_json(doc, nullptr);
      json cast_info_json = json::parse(cast_info_json_char);
      new_cast_info.cast_info_id = cast_info_json["cast_info_id"];
      new_cast_info.gender = cast_info_json["gender"];
      new_cast_info.name = cast_info_json["name"];
      new_cast_info.intro = cast_info_json["intro"];
      cast_info_json_map.insert({
        new_cast_info.cast_info_id, std::string(cast_info_json_char)});
      return_map.insert({new_cast_info.cast_info_id, new_cast_info});
      bson_free(cast_info_json_char);
    }
    find_span->Finish();
    bson_error_t error;
    if (mongoc_cursor_error(cursor, &error)) {
      LOG(warning) << error.message;
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      throw se;
    }
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);


    // upload posts to midas cache
    size_t missed_bytes = 0;
    for (auto &it : cast_info_json_map) {
      if (!_cast_info_cache->set(&it.first, sizeof(it.first), it.second.c_str(),
                           it.second.length()))
        LOG(debug) << "Failed to set post " << it.first << " to midas";
      missed_bytes += it.second.length();
    }
    auto missed_cycles_end = midas::Time::get_cycles_end();
    _pool->record_miss_penalty(missed_cycles_end - missed_cycles_stt,
                               missed_bytes);
  }

  if (return_map.size() != cast_info_ids.size()) {
    // try {
    //   for (auto &it : set_futures) { it.get(); }
    // } catch (...) {
    //   LOG(warning) << "Failed to set cast-info to memcached";
    // }
    LOG(error) << "cast-info-service return set incomplete" << return_map.size() << "!=" << cast_info_ids.size();
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "cast-info-service return set incomplete";
    throw se;
  }

  for (auto &cast_info_id : cast_info_ids) {
    _return.emplace_back(return_map[cast_info_id]);
  }

  // try {
  //   for (auto &it : set_futures) { it.get(); }
  // } catch (...) {
  //   LOG(warning) << "Failed to set cast-info to memcached";
  // }
}

} // namespace media_service

#endif //MEDIA_MICROSERVICES_CASTINFOHANDLER_H
