#ifndef MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
#define MEDIA_MICROSERVICES_MOVIEIDHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>

#include "../../gen-cpp/MovieIdService.h"
#include "../../gen-cpp/ComposeReviewService.h"
#include "../../gen-cpp/RatingService.h"
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

class MovieIdHandler : public MovieIdServiceIf {
 public:
  MovieIdHandler(
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<ComposeReviewServiceClient>> *,
      ClientPool<ThriftClient<RatingServiceClient>> *);
  ~MovieIdHandler() override = default;
  void UploadMovieId(int64_t, const std::string &, int32_t,
                     const std::map<std::string, std::string> &) override;
  void RegisterMovieId(int64_t, const std::string &, const std::string &,
                       const std::map<std::string, std::string> &) override;

 private:
  midas::CachePool *_pool;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> _movie_cache;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ComposeReviewServiceClient>> *_compose_client_pool;
  ClientPool<ThriftClient<RatingServiceClient>> *_rating_client_pool;
};

MovieIdHandler::MovieIdHandler(
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<ThriftClient<ComposeReviewServiceClient>> *compose_client_pool,
    ClientPool<ThriftClient<RatingServiceClient>> *rating_client_pool) {
  auto cmanager = midas::CacheManager::global_cache_manager();
  if (!cmanager->create_pool("movies") ||
      (_pool = cmanager->get_pool("movies")) == nullptr) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MIDAS_ERROR;
    se.message = "Failed to create midas cache pool";
    throw se;
  }
  _pool->update_limit(5ull * 1024 * 1024 * 1024); // ~1GB
  _movie_cache = std::make_shared<midas::SyncKV<kNumBuckets>>(_pool);
  _mongodb_client_pool = mongodb_client_pool;
  _compose_client_pool = compose_client_pool;
  _rating_client_pool = rating_client_pool;
}

void MovieIdHandler::UploadMovieId(
    int64_t req_id,
    const std::string &title,
    int32_t rating,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadMovieId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  size_t movie_id_size;
  uint32_t memcached_flags;
  // Look for the movie id from memcached

  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetMovieId", { opentracing::ChildOf(&span->context()) });

  char *movie_id_mmc = reinterpret_cast<char *>( _movie_cache->get(
      title.c_str(),
      title.length(),
      &movie_id_size));
  
  get_span->Finish();
  std::string movie_id_str;

  uint64_t missed_cycles_stt = 0;
  uint64_t missed_cycles_end = 0;

  // If cached in memcached
  if (movie_id_mmc) {
    LOG(debug) << "Get movie_id " << movie_id_mmc
        << " cache hit from Midas";
    movie_id_str = std::string(movie_id_mmc);
    free(movie_id_mmc);
  }

    // If not cached in memcached
  else {
    missed_cycles_stt = midas::Time::get_cycles_stt();
    mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
        _mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      free(movie_id_mmc);
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "movie-id", "movie-id");

    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user from DB movie-id";
      free(movie_id_mmc);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }

    bson_t *query = bson_new();
    BSON_APPEND_UTF8(query, "title", title.c_str());

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "MongoFindMovieId", { opentracing::ChildOf(&span->context()) });
    mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
        collection, query, nullptr, nullptr);
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    find_span->Finish();

    if (found) {
      bson_iter_t iter;
      if (bson_iter_init_find(&iter, doc, "movie_id")) {
        movie_id_str = std::string(bson_iter_value(&iter)->value.v_utf8.str);
        LOG(debug) << "Find movie " << movie_id_str << " cache miss";
      } else {
        LOG(error) << "Attribute movie_id is not find in MongoDB";
        bson_destroy(query);
        mongoc_cursor_destroy(cursor);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Attribute movie_id is not find in MongoDB";
        free(movie_id_mmc);
        throw se;
      }
    } else {
      LOG(error) << "Movie " << title << " is not found in MongoDB";
      bson_destroy(query);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "Movie " + title + " is not found in MongoDB";
      free(movie_id_mmc);
      throw se;
    }
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

    missed_cycles_end += midas::Time::get_cycles_end() - missed_cycles_stt;
    _pool->record_miss_penalty(missed_cycles_end, movie_id_str.length());

  }
  
  std::future<void> set_future;
  std::future<void> movie_id_future;
  std::future<void> rating_future;
  set_future = std::async(std::launch::async, [&]() {
    auto set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetMovieId", { opentracing::ChildOf(&span->context()) });
    // Upload the movie id to memcached
    bool set_success = _movie_cache->set(title.c_str(), title.length(), movie_id_str.c_str(), movie_id_str.length());
    set_span->Finish();
    if (!set_success) {
      LOG(warning) << "Failed to set movie_id to Midas";
    }
  });

  movie_id_future = std::async(std::launch::async, [&]() {
    auto compose_client_wrapper = _compose_client_pool->Pop();
    if (!compose_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to compose-review-service";
      throw se;
    }
    auto compose_client = compose_client_wrapper->GetClient();
    try {
      compose_client->UploadMovieId(req_id, movie_id_str, writer_text_map);
    } catch (...) {
      _compose_client_pool->Push(compose_client_wrapper);
      LOG(error) << "Failed to upload movie_id to compose-review-service";
      throw;
    }
    _compose_client_pool->Push(compose_client_wrapper);
  });

  rating_future = std::async(std::launch::async, [&]() {
    auto rating_client_wrapper = _rating_client_pool->Pop();
    if (!rating_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to rating-service";
      throw se;
    }
    auto rating_client = rating_client_wrapper->GetClient();
    try {
      rating_client->UploadRating(req_id, movie_id_str, rating, writer_text_map);
    } catch (...) {
      _rating_client_pool->Push(rating_client_wrapper);
      LOG(error) << "Failed to upload rating to rating-service";
      throw;
    }
    _rating_client_pool->Push(rating_client_wrapper);
  });

  try {
    movie_id_future.get();
    rating_future.get();
    set_future.get();
  } catch (...) {
    throw;
  }

  span->Finish();
}

void MovieIdHandler::RegisterMovieId (
    const int64_t req_id,
    const std::string &title,
    const std::string &movie_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "RegisterMovieId",
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
      mongodb_client, "movie-id", "movie-id");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection movie_id from DB movie-id";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }

  // Check if the username has existed in the database
  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "title", title.c_str());

  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "MongoFindMovie", { opentracing::ChildOf(&span->context()) });
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  bool found = mongoc_cursor_next(cursor, &doc);
  find_span->Finish();

  if (found) {
    LOG(warning) << "Movie "<< title << " already existed in MongoDB";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Movie " + title + " already existed in MongoDB";
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  } else {
    bson_t *new_doc = bson_new();
    BSON_APPEND_UTF8(new_doc, "title", title.c_str());
    BSON_APPEND_UTF8(new_doc, "movie_id", movie_id.c_str());
    bson_error_t error;

    auto insert_span = opentracing::Tracer::Global()->StartSpan(
        "MongoInsertMovie", { opentracing::ChildOf(&span->context()) });
    bool plotinsert = mongoc_collection_insert_one (
        collection, new_doc, nullptr, nullptr, &error);
    insert_span->Finish();

    if (!plotinsert) {
      LOG(error) << "Failed to insert movie_id of " << title
          << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(new_doc);
      mongoc_cursor_destroy(cursor);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
    bson_destroy(new_doc);
  }
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  span->Finish();
}
} // namespace media_service

#endif //MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
