#ifndef MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H
#define MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>

#include "../../gen-cpp/ReviewStorageService.h"
#include "../logger.h"
#include "../tracing.h"

// [Midas]
#include "cache_manager.hpp"
#include "sync_kv.hpp"
#include "time.hpp"
constexpr static uint64_t kNumBuckets = 1 << 20;


namespace media_service {

class ReviewStorageHandler : public ReviewStorageServiceIf{
 public:
  ReviewStorageHandler(mongoc_client_pool_t *);
  ~ReviewStorageHandler() override = default;
  void StoreReview(int64_t, const Review &, 
      const std::map<std::string, std::string> &) override;
  void ReadReviews(std::vector<Review> &, int64_t, const std::vector<int64_t> &,
                   const std::map<std::string, std::string> &) override;
  
 private:
  midas::CachePool *_pool;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> _rstorage_cache;
  mongoc_client_pool_t *_mongodb_client_pool;
};

ReviewStorageHandler::ReviewStorageHandler(
    mongoc_client_pool_t *mongodb_pool) {
  auto cmanager = midas::CacheManager::global_cache_manager();
  if (!cmanager->create_pool("rstorages") ||
      (_pool = cmanager->get_pool("rstorages")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool";
    throw se;
  }
  // _pool->update_limit(10ull * 1024 * 1024 * 1024); // ~1GB
  // _pool->update_limit(4319ull * 1024 * 1024); // ~1GB
  _pool->update_limit(1600ull * 1024 * 1024); // ~1GB
  _rstorage_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);
  _mongodb_client_pool = mongodb_pool;
}

void ReviewStorageHandler::StoreReview(
    int64_t req_id, 
    const Review &review,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "StoreReview",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
      _mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }

  auto collection = mongoc_client_get_collection(
      mongodb_client, "review-storage", "review-storage");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user from DB user";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  bson_t *new_doc = bson_new();
  BSON_APPEND_INT64(new_doc, "review_id", review.review_id);
  BSON_APPEND_INT64(new_doc, "timestamp", review.timestamp);
  BSON_APPEND_INT64(new_doc, "user_id", review.user_id);
  BSON_APPEND_UTF8(new_doc, "movie_id", review.movie_id.c_str());
  BSON_APPEND_UTF8(new_doc, "text", review.text.c_str());
  BSON_APPEND_INT32(new_doc, "rating", review.rating);
  BSON_APPEND_INT64(new_doc, "req_id", review.req_id);
  bson_error_t error;

  auto insert_span = opentracing::Tracer::Global()->StartSpan(
      "MongoInsertReview", { opentracing::ChildOf(&span->context()) });
  bool plotinsert = mongoc_collection_insert_one (
      collection, new_doc, nullptr, nullptr, &error);
  insert_span->Finish();

  if (!plotinsert) {
    LOG(error) << "Error: Failed to insert review to MongoDB: "
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
void ReviewStorageHandler::ReadReviews(
    std::vector<Review> & _return,
    int64_t req_id,
    const std::vector<int64_t> &review_ids,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadReviews",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (review_ids.empty()) {
    return;
  }

  std::set<int64_t> review_ids_not_cached(review_ids.begin(), review_ids.end());
  if (review_ids_not_cached.size() != review_ids.size()) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Post_ids are duplicated";
    throw se;
  }
  std::map<int64_t, Review> return_map;
  // midas::kv_types::BatchPlug plug;
  // _rstorage_cache->batch_stt(plug);
  for (auto &review_id : review_ids) {
    size_t return_value_length = 0;
    // char *return_value = reinterpret_cast<char *>(
    //     _rstorage_cache->bget_single(&review_id, sizeof(review_id), &return_value_length, plug));
    char *return_value = reinterpret_cast<char *>(
        _rstorage_cache->get(&review_id, sizeof(review_id), &return_value_length));
    if (return_value) {
      Review new_review;
      json review_json = json::parse(std::string(
          return_value, return_value + return_value_length));
      new_review.req_id = review_json["req_id"];
      new_review.user_id = review_json["user_id"];
      new_review.movie_id = review_json["movie_id"];
      new_review.text = review_json["text"];
      new_review.rating = review_json["rating"];
      new_review.timestamp = review_json["timestamp"];
      new_review.review_id = review_json["review_id"];
      // _return_json->emplace_back(review_json.dump());
      return_map.insert(std::make_pair(new_review.review_id, new_review));
      review_ids_not_cached.erase(new_review.review_id);
      free(return_value);
      LOG(debug) << "Review: " << new_review.review_id << " found in memcached";
    }
  }
  // _rstorage_cache->batch_end(plug);

  std::map<int64_t, std::string> review_json_map;
  
  // Find the rest in MongoDB
  if (!review_ids_not_cached.empty()) {
    auto missed_cycles_stt = midas::Time::get_cycles_stt();

    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "review-storage", "review-storage");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB user";
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_t *query = bson_new();
    bson_t query_child;
    bson_t query_review_id_list;
    const char *key;
    int idx = 0;
    char buf[16];
    BSON_APPEND_DOCUMENT_BEGIN(query, "review_id", &query_child);
    BSON_APPEND_ARRAY_BEGIN(&query_child, "$in", &query_review_id_list);
    for (auto &item : review_ids_not_cached) {
      bson_uint32_to_string(idx, &key, buf, sizeof buf);
      BSON_APPEND_INT64(&query_review_id_list, key, item);
      idx++;
    }
    bson_append_array_end(&query_child, &query_review_id_list);
    bson_append_document_end(query, &query_child);
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindPosts", {opentracing::ChildOf(&span->context())});
    while (true) {
      bool found = mongoc_cursor_next(cursor, &doc);
      if (!found) {
        break;
      }
      Review new_review;
      char *review_json_char = bson_as_json(doc, nullptr);
      json review_json = json::parse(review_json_char);
      new_review.req_id = review_json["req_id"];
      new_review.user_id = review_json["user_id"];
      new_review.movie_id = review_json["movie_id"];
      new_review.text = review_json["text"];
      new_review.rating = review_json["rating"];
      new_review.timestamp = review_json["timestamp"];
      new_review.review_id = review_json["review_id"];
      // _return_json->emplace_back(std::string(review_json_char));
      review_json_map.insert({new_review.review_id, std::string(review_json_char)});
      return_map.insert({new_review.review_id, new_review});
      bson_free(review_json_char);
      // std::cout<<"A new review: "<<new_review.review_id<<std::endl<<std::flush;
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
    for (auto &it : review_json_map) {
      if (!_rstorage_cache->set(&it.first, sizeof(it.first), it.second.c_str(),
                           it.second.length()))
        LOG(debug) << "Failed to set review " << it.first << " to midas";
      missed_bytes += it.second.length();
    }
    auto missed_cycles_end = midas::Time::get_cycles_end();
    _pool->record_miss_penalty(missed_cycles_end - missed_cycles_stt,
                               missed_bytes);
  }

  if (return_map.size() != review_ids.size()) {
    LOG(error) << "review storage service: return set incomplete";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "review storage service: return set incomplete";
    throw se;
  }

  for (auto &review_id : review_ids) {
    _return.emplace_back(return_map[review_id]);
  }
  
}

} // namespace media_service


#endif //MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H
