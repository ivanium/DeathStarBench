#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <signal.h>
#include <string>
#include <thread>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <boost/program_options.hpp>

#include "../../gen-cpp/media_service_types.h"
#include "../../gen-cpp/UserService.h"
#include "../../gen-cpp/TextService.h"
#include "../../gen-cpp/UniqueIdService.h"
#include "../../gen-cpp/MovieIdService.h"
#include "../../gen-cpp/PageService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../utils.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace media_service;

// constexpr static uint32_t kNumUsers = 962;
constexpr static char kCharSet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
constexpr static uint32_t kTextLen = 256;
constexpr static uint32_t kNumThd = 4;
constexpr static uint32_t kNumUsers = 1000;
constexpr static bool kSkewed = false;
constexpr static float kSkewness = 0.99;
constexpr static float kMinReviews = 25;

// constexpr static char kDatasetPath[] = "/datasets/social-graph";
// constexpr static char kDatasetName[] = "soc-twitter-follows-mun";
// constexpr static char kDatasetName[] = "socfb-Reed98";

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

template <typename Service> class WrappedClientPool {
public:
  void init(std::string service_name) {
    if (load_config_file("/config/service-config.json", &config_json) != 0) {
      exit(EXIT_FAILURE);
    }

    std::cout << service_name << std::endl;

    int port = config_json[service_name]["port"];
    std::string addr = config_json[service_name]["addr"];
    // int conns = config_json[service_name]["connections"];
    int conns = 256;
    int timeout = 10000;
    int keepalive = 10000;

    std::cout << port << " " << addr << " " << timeout << " done" << std::endl;

    _client_pool.reset(new ClientPool<Service>(service_name.append("-client"),
                                               addr, port, 0, conns, timeout));
  }

  ClientPool<Service> *get() { return _client_pool.get(); }

private:
  json config_json;
  std::shared_ptr<ClientPool<Service>> _client_pool;
};

struct SocialNetState {
  WrappedClientPool<ThriftClient<MovieIdServiceClient>> movieid_service_client;
  WrappedClientPool<ThriftClient<UserServiceClient>> user_service_client;
  WrappedClientPool<ThriftClient<TextServiceClient>> text_service_client;
  WrappedClientPool<ThriftClient<UniqueIdServiceClient>> uniqueid_service_client;
  WrappedClientPool<ThriftClient<PageServiceClient>> page_service_client;

  json movie_names;
  json movie_ids;

  std::random_device rd;
  std::unique_ptr<std::mt19937> gen;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_1_10;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_1_numusers;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_1_nummovies;
  std::unique_ptr<std::uniform_int_distribution<>> uniform_0_charsetsize;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> uniform_0_maxint64;

  int loadMovieName() {
    if (load_config_file("/config/names.json", &movie_names) != 0) {
      exit(EXIT_FAILURE);
    }
    return 0;
  }

  int loadMovieIds() {
    if (load_config_file("/config/ids.json", &movie_ids) != 0) {
      exit(EXIT_FAILURE);
    }
    return 0;
  }

  int getNumMovies() {
    return movie_names.size();
  }

  int init() {
    this->user_service_client.init("user-service");
    this->movieid_service_client.init("movie-id-service");
    this->uniqueid_service_client.init("unique-id-service");
    this->text_service_client.init("text-service");
    this->page_service_client.init("page-service");

    loadMovieName();
    loadMovieIds();

    this->gen.reset(new std::mt19937((this->rd)()));
    this->uniform_1_10.reset(new std::uniform_int_distribution<>(1, 10));
    this->uniform_1_numusers.reset(new std::uniform_int_distribution<>(1, kNumUsers));
    this->uniform_1_nummovies.reset(
        new std::uniform_int_distribution<>(1, getNumMovies()));
    this->uniform_0_charsetsize.reset(
        new std::uniform_int_distribution<>(0, sizeof(kCharSet) - 2));
    this->uniform_0_maxint64.reset(new std::uniform_int_distribution<int64_t>(
        0, std::numeric_limits<int64_t>::max()));
    return 0;
  }
} state;

std::string random_string(uint32_t len, const SocialNetState &state) {
  std::string str = "";
  for (uint32_t i = 0; i < kTextLen; i++) {
    auto idx = (*state.uniform_0_charsetsize)(*state.gen);
    str += kCharSet[idx];
  }
  return str;
}

int64_t random_int64() {
  return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

std::atomic<int64_t> failed_count(0);

std::atomic<int64_t> read_failed_count(0);


int UploadUserId(int64_t req_id, std::string username) {
  std::map<std::string, std::string> carrier;
  auto clientPool = state.user_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->UploadUserWithUsername(req_id, username, carrier);
  } catch (...) {
    std::cout << "Failed to upload user" << username << " from user-service"
              << std::endl;
    // state.user_service_client.get()->Remove(clientWrapper);
     state.user_service_client.get()->Push(clientWrapper);
    throw;
  }
  state.user_service_client.get()->Push(clientWrapper);
  return 0;
}

int UploadUniqueId(int64_t req_id) {
  std::map<std::string, std::string> carrier;
  auto clientPool = state.uniqueid_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->UploadUniqueId(req_id, carrier);
  } catch (...) {
    std::cout << "Failed to upload unique id" << req_id << " from unique-id-service"
              << std::endl;
    // state.uniqueid_service_client.get()->Remove(clientWrapper);
    state.uniqueid_service_client.get()->Push(clientWrapper);
    throw;
  }
  state.uniqueid_service_client.get()->Push(clientWrapper);
  return 0;
}

int UploadText(int64_t req_id, std::string text) {
  std::map<std::string, std::string> carrier;
  auto clientPool = state.text_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->UploadText(req_id, text, carrier);
  } catch (...) {
    std::cout << "Failed to upload text" << text << " from text-service"
              << std::endl;
    // state.text_service_client.get()->Remove(clientWrapper);
    state.text_service_client.get()->Push(clientWrapper);
    throw;
  }
  state.text_service_client.get()->Push(clientWrapper);
  return 0;
}

int UploadMovieId(int64_t req_id, std::string title, int32_t rating) {
  std::map<std::string, std::string> carrier;
  auto clientPool = state.movieid_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->UploadMovieId(req_id, title, rating, carrier);
  } catch (...) {
    std::cout << "Failed to upload movie" << title << " from movie-id-service"
              << std::endl;
    // state.movieid_service_client.get()->Remove(clientWrapper);
    state.movieid_service_client.get()->Push(clientWrapper);
    throw;
  }
  state.movieid_service_client.get()->Push(clientWrapper);
  return 0;
}


int ComposeReview(int tid) {
  
  // std::cout << "Started Compose Review"<<std::chrono::high_resolution_clock::now().time_since_epoch().count()<<std::flush;
  std::map<std::string, std::string> carrier;
  int64_t tid_64 = tid;
  int64_t req_id = (((u_int64_t)random_int64() << 4) | (tid_64));
  int32_t user_id = ((*state.uniform_1_numusers)(*state.gen));
  std::string username = std::string("username_") + std::to_string(user_id);
  std::string movie = state.movie_names[(*state.uniform_1_nummovies)(*state.gen) - 1];
  std::string text = random_string(kTextLen, state);
  int32_t rating = (*state.uniform_1_10)(*state.gen);
  std::vector<std::future<int>> futures;
  std::atomic<int> count(0);
  futures.push_back(std::async(std::launch::async, [req_id=req_id, username=username, count_addr=&count]() {
    try{
      UploadUserId(req_id, username);
    }catch (...) {
      *count_addr += 1;
    }
    return 0;
  }));
  futures.push_back(std::async(std::launch::async, [req_id=req_id, title=movie, rating=rating, count_addr=&count]() {
    try{
      UploadMovieId(req_id, title, rating);
    }
    catch (...) {
      *count_addr += 1;
    }
    return 0;
  }));
  futures.push_back(std::async(std::launch::async, [req_id=req_id, text=text, count_addr=&count]() {
    try{
      UploadText(req_id, text);
    }
    catch (...) {
      *count_addr += 1;
    }
    return 0;
  }));
  futures.push_back(std::async(std::launch::async, [req_id=req_id, count_addr=&count]() {
    try{
      UploadUniqueId(req_id);
    }
    catch (...) {
      *count_addr += 1;
    }
    return 0;
  }));

  for (auto &future : futures)
    future.get();
  
  if(count != 0) {
    std::cout << "Compose Review Failed: fail service count"<< count <<std::endl<<std::flush;  
    failed_count ++;
  }

  return 0;
}

int compose_reviews(int64_t num_reviews) {
  const int BATCH_SIZE = 10;
  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished { 0 };
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid=tid, num_reviews=num_reviews]() {
      std::vector<std::future<int>> futures;
      int64_t chunkSize = (num_reviews + kNumThd - 1) / kNumThd;
      int64_t stt = chunkSize * tid;
      int64_t end = std::min(stt + chunkSize, num_reviews);
      for (int64_t i = stt; i < end; i++) {
        futures.push_back(std::async(std::launch::async, [tid=tid]() {
          ComposeReview(tid);
          return 0;
        }));

        if (futures.size() % BATCH_SIZE == 0) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 10000 == 0) {
              std::cout << "Finished reviews: " << numFinished.load()
                        << std::endl;
          }
        }
      }
      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto dur =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - stt).count();
  std::cout << "Duration: " << dur << " ms" << std::endl;
  std::cout << "Failed Count: " << failed_count << std::endl;

  std::cout << "Throughput: " << 1.0 * num_reviews / dur << " Kops"
            << std::endl;

  return 0;
}


int PreComposeReview(int tid, int64_t movie_index) {
  
  // std::cout << "Started Compose Review"<<std::chrono::high_resolution_clock::now().time_since_epoch().count()<<std::flush;
  std::map<std::string, std::string> carrier;
  int64_t tid_64 = tid;
  std::string movie = state.movie_names[movie_index];

  int success_count = 0;

  for (int i = 0 ; i < kMinReviews; i ++ ){
    int64_t req_id = (((u_int64_t)random_int64() << 4) | (tid_64));
    int32_t user_id = ((*state.uniform_1_numusers)(*state.gen));
    std::string username = std::string("username_") + std::to_string(user_id);
    std::string text = random_string(kTextLen, state);
    int32_t rating = (*state.uniform_1_10)(*state.gen);
    std::vector<std::future<int>> futures;
    std::atomic<int> count(0);
    futures.push_back(std::async(std::launch::async, [req_id=req_id, username=username, count_addr=&count]() {
      try{
        UploadUserId(req_id, username);
      }catch (...) {
        *count_addr += 1;
      }
      return 0;
    }));
    futures.push_back(std::async(std::launch::async, [req_id=req_id, title=movie, rating=rating, count_addr=&count]() {
      try{
        UploadMovieId(req_id, title, rating);
      }
      catch (...) {
        *count_addr += 1;
      }
      return 0;
    }));
    futures.push_back(std::async(std::launch::async, [req_id=req_id, text=text, count_addr=&count]() {
      try{
        UploadText(req_id, text);
      }
      catch (...) {
        *count_addr += 1;
      }
      return 0;
    }));
    futures.push_back(std::async(std::launch::async, [req_id=req_id, count_addr=&count]() {
      try{
        UploadUniqueId(req_id);
      }
      catch (...) {
        *count_addr += 1;
      }
      return 0;
    }));

    for (auto &future : futures)
      future.get();
    
    if(count == 0) {
      success_count ++;
    }
  }
  
  if(success_count < kMinReviews - 5) {
    std::cout << "Pre load reviews count not enough for movie: "<< movie << " and the succeeded count is " << success_count <<std::endl<<std::flush;
  }
  // else {
  //   std::cout << "Pre load reviews count enough for movie: "<< movie << " and the succeeded count is " << success_count <<std::endl<<std::flush;
  // }

  return 0;
}

int pre_compose_reviews() {
  const int BATCH_SIZE = 10;
  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished { 0 };
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid=tid, num_movies=state.getNumMovies()]() {
      std::vector<std::future<int>> futures;
      int64_t chunkSize = (num_movies + kNumThd - 1) / kNumThd;
      int64_t stt = chunkSize * tid;
      int64_t end = std::min(stt + chunkSize, (int64_t)num_movies);
      for (int64_t i = stt; i < end; i++) {
        futures.push_back(std::async(std::launch::async, [tid=tid, index=i]() {
          PreComposeReview(tid, index);
          return 0;
        }));

        if (futures.size() % BATCH_SIZE == 0) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 1000 == 0) {
              std::cout << "Finished reviews for movies: " << numFinished.load()
                        << std::endl;
          }
        }
      }
      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto dur =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - stt).count();
  std::cout << "Duration: " << dur << " ms" << std::endl;
  std::cout << "Failed Count: " << failed_count << std::endl;

  std::cout << "Throughput: " << 1.0 * state.getNumMovies() * kMinReviews / dur << " Kops"
            << std::endl;

  return 0;
}


int CallPageService(Page& page, int64_t req_id, std::string movie_id, int32_t start, int32_t end) {
  std::map<std::string, std::string> carrier;
  auto clientPool = state.page_service_client.get();
  auto clientWrapper = clientPool->Pop();
  if (!clientWrapper) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
    se.message = "Failed to connect to user-service";
    throw se;
  }
  auto client = clientWrapper->GetClient();
  try {
    client->ReadPage(page, req_id, movie_id, start, end, carrier);
  } catch (...) {
    std::cout << "Failed to read page" << movie_id << " from page-service"
              << std::endl;
    // state.movieid_service_client.get()->Remove(clientWrapper);
    state.page_service_client.get()->Push(clientWrapper);
    throw;
  }
  state.page_service_client.get()->Push(clientWrapper);
  return 0;
}



int ReadPage(int tid) {
  Page page;
  std::map<std::string, std::string> carrier;
  int64_t tid_64 = tid;
  int64_t req_id = (((u_int64_t)random_int64() << 4) | (tid_64));
  std::string movie_id = state.movie_ids[(*state.uniform_1_nummovies)(*state.gen) - 1];
  int32_t start = (*state.uniform_1_10)(*state.gen) - 1;
  int32_t end = start + (*state.uniform_1_10)(*state.gen);
  // int32_t start = 0;
  // int32_t end = 1;
  try{
    CallPageService(page, req_id, movie_id, start, end);
  }catch (...) {
    std::cout << "Read page failed"<<std::endl<<std::flush;  
    read_failed_count ++;
  }
  return 0;
}


int read_pages(int64_t num_pages) {
  const int BATCH_SIZE = 10;
  auto stt = std::chrono::high_resolution_clock::now();
  std::atomic<int64_t> numFinished { 0 };
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThd; tid++) {
    thds.push_back(std::thread([&numFinished, tid=tid, num_pages=num_pages]() {
      std::vector<std::future<int>> futures;
      int64_t chunkSize = (num_pages + kNumThd - 1) / kNumThd;
      int64_t stt = chunkSize * tid;
      int64_t end = std::min(stt + chunkSize, num_pages);
      for (int64_t i = stt; i < end; i++) {
        futures.push_back(std::async(std::launch::async, [tid=tid]() {
          ReadPage(tid);
          return 0;
        }));

        if (futures.size() % BATCH_SIZE == 0) {
          for (auto &future : futures)
            future.get();
          futures.clear();
          if (numFinished.fetch_add(BATCH_SIZE) % 100 == 0) {
              std::cout << "Finished reading pages: " << numFinished.load()
                        << std::endl;
          }
        }
      }
      for (auto &future : futures)
        future.get();
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto dur =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - stt).count();
  std::cout << "Duration: " << dur << " ms" << std::endl;
  std::cout << "Failed Count: " << read_failed_count << std::endl;

  std::cout << "Throughput: " << 1.0 * num_pages / dur << " Kops"
            << std::endl;

  return 0;
}


int main(int argc, char *argv[]) {
  state.init();
  int64_t num_reviews = 1000;
  int64_t num_pages = 10;
  if (argc > 1) {
    num_reviews = atoll(argv[1]);
  }
  if (argc > 2) {
    num_pages = atoll(argv[2]);
  }
  pre_compose_reviews();
  read_pages(num_pages);
  compose_reviews(num_reviews);
  return 0;
}
