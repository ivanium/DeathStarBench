#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_

#include <bson/bson.h>
#include <mongoc.h>

#include <future>
#include <iostream>
#include <string>

#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/UserTimelineService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"
#include "../PostUtils.h"

// [Midas]
#include "resource_manager.hpp"
#include "sync_kv.hpp"
constexpr static int kNumBuckets = 1 << 20;

namespace social_network {

class UserTimelineHandler : public UserTimelineServiceIf {
public:
  UserTimelineHandler(mongoc_client_pool_t *,
                      ClientPool<ThriftClient<PostStorageServiceClient>> *);
  ~UserTimelineHandler() override = default;

  void WriteUserTimeline(
      int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
      const std::map<std::string, std::string> &carrier) override;

  void ReadUserTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

 private:
  std::shared_ptr<midas::SyncKV<kNumBuckets>> post_cache;
  std::shared_ptr<midas::SyncKV<kNumBuckets>> postid_cache;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<PostStorageServiceClient>> *_post_client_pool;
};

UserTimelineHandler::UserTimelineHandler(
    mongoc_client_pool_t *mongodb_pool,
    ClientPool<ThriftClient<PostStorageServiceClient>> *post_client_pool) {
  auto rmanager = midas::ResourceManager::global_manager();
  LOG(error) << "Get midas rmanager @ " << rmanager;
  post_cache = std::make_shared<midas::SyncKV<kNumBuckets>>();
  postid_cache = std::make_shared<midas::SyncKV<kNumBuckets>>();
  _mongodb_client_pool = mongodb_pool;
  _post_client_pool = post_client_pool;
}

void UserTimelineHandler::WriteUserTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::map<std::string, std::string> &carrier) {
  // // Initialize a span
  // TextMapReader reader(carrier);
  // std::map<std::string, std::string> writer_text_map;
  // TextMapWriter writer(writer_text_map);
  // auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  // auto span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  // opentracing::Tracer::Global()->Inject(span->context(), writer);

  mongoc_client_t *mongodb_client =
      mongoc_client_pool_pop(_mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "user-timeline", "user-timeline");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection user-timeline from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  bson_t *query = bson_new();

  BSON_APPEND_INT64(query, "user_id", user_id);
  bson_t *update =
      BCON_NEW("$push", "{", "posts", "{", "$each", "[", "{", "post_id",
               BCON_INT64(post_id), "timestamp", BCON_INT64(timestamp), "}",
               "]", "$position", BCON_INT32(0), "}", "}");
  bson_error_t error;
  bson_t reply;
  // auto update_span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_mongo_insert_client",
  //     {opentracing::ChildOf(&span->context())});
  bool updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                   update, nullptr, false, true,
                                                   true, &reply, &error);
  // update_span->Finish();

  if (!updated) {
    // update the newly inserted document (upsert: false)
    updated = mongoc_collection_find_and_modify(collection, query, nullptr,
                                                update, nullptr, false, false,
                                                true, &reply, &error);
    if (!updated) {
      LOG(error) << "Failed to update user-timeline for user " << user_id
                 << " to MongoDB: " << error.message;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = error.message;
      bson_destroy(update);
      bson_destroy(query);
      bson_destroy(&reply);
      mongoc_collection_destroy(collection);
      mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
      throw se;
    }
  }

  bson_destroy(update);
  bson_destroy(&reply);
  bson_destroy(query);
  mongoc_collection_destroy(collection);
  mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);

  // Update user's timeline in redis
  // auto redis_span = opentracing::Tracer::Global()->StartSpan(
  //     "write_user_timeline_redis_update_client",
  //     {opentracing::ChildOf(&span->context())});

  using PostIDPair = std::pair<int64_t, double>;
  std::vector<PostIDPair> cached_vec;
  size_t cached_raw_len = 0;
  PostIDPair *cached_raw = reinterpret_cast<PostIDPair *>(
      postid_cache->get(&user_id, sizeof(user_id), &cached_raw_len));
  if (cached_raw) {
    size_t nr_pairs = cached_raw_len / sizeof(PostIDPair);
    bool inserted = false;
    bool updated = false;
    for (int i = 0; i < nr_pairs; i++) {
      if (cached_raw[i].first == post_id)
        break;
      if (inserted || cached_raw[i].second < timestamp)
        cached_vec.emplace_back(cached_raw[i]);
      else {
        cached_vec.emplace_back(std::make_pair(post_id, (double)timestamp));
        inserted = true;
        updated = true;
      }
    }
    free(cached_raw);
    cached_raw = nullptr;
    if (updated)
      if (!postid_cache->set(&user_id, sizeof(user_id), cached_vec.data(),
                             cached_vec.size() * sizeof(cached_vec[0])))
        LOG(warning) << "Midas postid cache set failed for user " << user_id;
  }

  // redis_span->Finish();
  // span->Finish();
}

void UserTimelineHandler::ReadUserTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start,
    int stop, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop <= start || start < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_user_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<int64_t> post_ids;
  using PostIDPair = std::pair<int64_t, double>;
  size_t cached_ids_len = 0;
  PostIDPair *cached_ids = reinterpret_cast<PostIDPair *>(
      postid_cache->get(&user_id, sizeof(user_id), &cached_ids_len));
  if (cached_ids) {
    size_t nr_pairs = cached_ids_len / sizeof(PostIDPair);
    for (int i = start; i < stop && i < nr_pairs; i++)
      post_ids.emplace_back(cached_ids[i].first);
    if (stop <= nr_pairs) { // all post_ids are cached so no update needed
      free(cached_ids);
      cached_ids = nullptr;
    }
  }

  // find in mongodb
  int mongo_start = start + post_ids.size();
  std::vector<PostIDPair> update_pairs;
  if (mongo_start < stop) {
    // Instead find post_ids from mongodb
    mongoc_client_t *mongodb_client =
        mongoc_client_pool_pop(_mongodb_client_pool);
    if (!mongodb_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to pop a client from MongoDB pool";
      throw se;
    }
    auto collection = mongoc_client_get_collection(
        mongodb_client, "user-timeline", "user-timeline");
    if (!collection) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MONGODB_ERROR;
      se.message = "Failed to create collection user-timeline from MongoDB";
      throw se;
    }

    bson_t *query = BCON_NEW("user_id", BCON_INT64(user_id));
    bson_t *opts = BCON_NEW("projection", "{", "posts", "{", "$slice", "[",
                            BCON_INT32(0), BCON_INT32(stop), "]", "}", "}");

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "user_timeline_mongo_find_client",
        {opentracing::ChildOf(&span->context())});
    mongoc_cursor_t *cursor =
        mongoc_collection_find_with_opts(collection, query, opts, nullptr);
    find_span->Finish();
    const bson_t *doc;
    bool found = mongoc_cursor_next(cursor, &doc);
    if (found) {
      bson_iter_t iter_0;
      bson_iter_t iter_1;
      bson_iter_t post_id_child;
      bson_iter_t timestamp_child;
      int idx = 0;
      bson_iter_init(&iter_0, doc);
      bson_iter_init(&iter_1, doc);
      while (bson_iter_find_descendant(
                 &iter_0, ("posts." + std::to_string(idx) + ".post_id").c_str(),
                 &post_id_child) &&
             BSON_ITER_HOLDS_INT64(&post_id_child) &&
             bson_iter_find_descendant(
                 &iter_1,
                 ("posts." + std::to_string(idx) + ".timestamp").c_str(),
                 &timestamp_child) &&
             BSON_ITER_HOLDS_INT64(&timestamp_child)) {
        auto curr_post_id = bson_iter_int64(&post_id_child);
        auto curr_timestamp = bson_iter_int64(&timestamp_child);
        if (idx >= mongo_start) {
          //In mixed workload condition, post may composed between redis and mongo read
          //mongodb index will shift and duplicate post_id occurs
          if ( std::find(post_ids.begin(), post_ids.end(), curr_post_id) == post_ids.end() ) {
            post_ids.emplace_back(curr_post_id);
          }
        }
        update_pairs.emplace_back(
            std::make_pair(curr_post_id, (double)curr_timestamp));
        bson_iter_init(&iter_0, doc);
        bson_iter_init(&iter_1, doc);
        idx++;
      }
    }
    bson_destroy(opts);
    bson_destroy(query);
    mongoc_cursor_destroy(cursor);
    mongoc_collection_destroy(collection);
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
  }

  // midas query cache
  std::set<int64_t> post_ids_not_cached(post_ids.begin(), post_ids.end());
  if (post_ids_not_cached.size() != post_ids.size()) {
    LOG(error)<< "Post_ids are duplicated";
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Post_ids are duplicated";
    throw se;
  }
  std::map<int64_t, Post> return_map;
  for (auto &post_id : post_ids) {
    size_t post_len = 0;
    char *post_store = reinterpret_cast<char *>(
        post_cache->get(&post_id, sizeof(post_id), &post_len));
    if (post_store) {
      Post new_post;
      json post_json = json::parse(
          std::string(post_store, post_store + post_len));
      json_utils::JsonToPost(post_json, new_post);
      return_map.insert(std::make_pair(new_post.post_id, new_post));
      // filter out
      post_ids_not_cached.erase(new_post.post_id);
      free(post_store);
    }
  }

  std::future<std::vector<Post>> post_future;
  if (!post_ids_not_cached.empty()) {
    post_future =
        std::async(std::launch::async, [&]() {
          auto post_client_wrapper = _post_client_pool->Pop();
          if (!post_client_wrapper) {
            ServiceException se;
            se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
            se.message = "Failed to connect to post-storage-service";
            throw se;
          }
          std::vector<Post> _return_posts;
          auto post_client = post_client_wrapper->GetClient();
          try {
            std::vector<int64_t> post_ids_(post_ids_not_cached.begin(),
                                           post_ids_not_cached.end());
            post_client->ReadPosts(_return_posts, req_id, post_ids_,
                                   writer_text_map);
          } catch (...) {
            _post_client_pool->Remove(post_client_wrapper);
            LOG(error) << "Failed to read posts from post-storage-service";
            throw;
          }
          _post_client_pool->Keepalive(post_client_wrapper);
          return _return_posts;
        });
  }

  if (update_pairs.size() > 0) {
    std::vector<PostIDPair> cached_pairs;
    if (cached_ids) {
      size_t nr_pairs = cached_ids_len / sizeof(PostIDPair);
      std::sort(
          update_pairs.begin(), update_pairs.end(),
          [](PostIDPair &p1, PostIDPair &p2) { return p1.second < p2.second; });
      for (int i = 0, j = 0; i < nr_pairs;) {
        cached_pairs.emplace_back(cached_ids[i]);
      }
      free(cached_ids);
      cached_ids = nullptr;
    }
    std::vector<PostIDPair> merged_pairs;
    merged_pairs.reserve(update_pairs.size() + cached_pairs.size());
    std::merge(
        cached_pairs.begin(), cached_pairs.end(), update_pairs.begin(),
        update_pairs.end(), std::back_inserter(merged_pairs),
        [](PostIDPair &p1, PostIDPair &p2) { return p1.second < p2.second; });
    // postid_cache->remove(&user_id, sizeof(user_id));
    if (!postid_cache->set(&user_id, sizeof(user_id), merged_pairs.data(),
                           merged_pairs.size() * sizeof(merged_pairs[0])))
      LOG(warning) << "Midas postid cache set failed for user " << user_id;
  }
  if (cached_ids) {
    free(cached_ids);
    cached_ids = nullptr;
  }

  if (!post_ids_not_cached.empty()) {
    try {
      // merge
      auto posts_not_cached = post_future.get();
      for (auto &post : posts_not_cached) {
        return_map.insert(std::make_pair(post.post_id, post));

        json post_json;
        json_utils::PostToJson(post, post_json);
        std::string post_json_str = post_json.dump();
        int64_t post_id = post.post_id;
        if (!post_cache->set(&post_id, sizeof(post_id), post_json_str.c_str(),
                             post_json_str.length()))
          LOG(error) << "Failed to set post " << post.post_id << " into Midas";
      }
    } catch (...) {
      LOG(error) << "Failed to get post from post-storage-service";
      throw;
    }
  }
  if (return_map.size() < post_ids.size())
    LOG(error) << "Failed to get all posts!";
  std::vector<Post> return_vec;
  for (auto &post_id : post_ids) {
    return_vec.emplace_back(return_map[post_id]);
  }
  _return = return_vec;
  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_USERTIMELINESERVICE_USERTIMELINEHANDLER_H_
