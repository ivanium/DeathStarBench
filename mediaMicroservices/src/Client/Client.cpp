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
constexpr static uint32_t kNumThd = 2;
constexpr static uint32_t kNumUsers = 1000;
constexpr static bool kSkewed = false;
constexpr static float kSkewness = 0.99;
constexpr static float kMinReviews = 25;

uint64_t CPU_FREQ = 0;

static inline uint64_t rdtscp() {
  uint32_t a, d, c;
  asm volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

bool init_timer() {
  auto stt_time = std::chrono::high_resolution_clock::now();
  uint64_t stt_cycles = rdtscp();
  volatile int64_t sum = 0;
  for (int i = 0; i < 10000000; i++) {
    sum += i;
  }
  uint64_t end_cycles = rdtscp();
  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t dur_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - stt_time)
          .count();
  CPU_FREQ = (end_cycles - stt_cycles) * 1000 / dur_ns;
  std::cout << "Timer initialied, CPU Freq: " << CPU_FREQ << "MHz" << std::endl;
  return true;
}

inline uint64_t microtime() {
  return rdtscp() / CPU_FREQ;
}


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
    int conns = 64;
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
  const int BATCH_SIZE = 50;
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
  int64_t tid_64 = tid;
  std::string movie = state.movie_names[movie_index];

  int success_count = 0;

  for (int i = 0 ; i < kMinReviews; i ++ ){
    std::map<std::string, std::string> carrier;
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
  const int BATCH_SIZE = 50;
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




class PerfRequest {
public:
  virtual int SendRequest() = 0;
};

enum RType{
  tCompose,
  tRead,
};
class ComposeRequest: public PerfRequest {
public:
  int64_t req_id;
  std::string movie;
  int32_t user_id;
  std::string username;
  std::string text;
  int32_t rating;
  ComposeRequest(int64_t tid){
    req_id = (((u_int64_t)random_int64() << 4) | (tid));
    user_id = ((*state.uniform_1_numusers)(*state.gen));
    username = std::string("username_") + std::to_string(user_id);
    movie = state.movie_names[(*state.uniform_1_nummovies)(*state.gen) - 1];
    text = random_string(kTextLen, state);
    rating = (*state.uniform_1_10)(*state.gen);
  }
  
  int SendRequest() {
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
      std::cout << "Compose Review Failed: fail service count"<< count << "for request id: " << req_id <<std::endl<<std::flush;  
      return 0;
    }
    return 1;
  }
};

class ReadRequest: public PerfRequest {
public:
  int64_t req_id;
  std::string movie_id;
  int32_t start;
  int32_t end;
  
  ReadRequest(int64_t tid) {
    req_id = (((u_int64_t)random_int64() << 4) | (tid));
    movie_id = state.movie_ids[(*state.uniform_1_nummovies)(*state.gen) - 1];
    start = (*state.uniform_1_10)(*state.gen) - 1;
    end = start + (*state.uniform_1_10)(*state.gen);
  }
  int SendRequest() {
    Page page;
    std::map<std::string, std::string> carrier;
    try{
      CallPageService(page, req_id, movie_id, start, end);
    }catch (...) {
      std::cout << "Read page failed for req_id: " << req_id << "for movie: " << movie_id <<std::endl<<std::flush;  
      return 0;
    }
    return 1;
  }
};

struct PerfRequestWithTime {
  uint64_t start_us;
  PerfRequest* req;
};

struct Trace {
  uint64_t absl_start_us;
  uint64_t start_us;
  uint64_t duration_us;
};


enum TraceFormat { kUnsorted, kSortedByDuration, kSortedByStart };

struct TraceRecords {
  TraceFormat trace_format_;
  double real_mops_;
  std::vector<Trace> traces_;
} ComposeT, ReadT;



std::vector<PerfRequestWithTime> compose_reqs[kNumThd];
std::vector<PerfRequestWithTime> read_reqs[kNumThd];


void reset(TraceRecords& traces) {
  traces.traces_.clear();
  traces.trace_format_ = kUnsorted;
  traces.real_mops_ = 0;
}

void gen_reqs(
    std::vector<PerfRequestWithTime> *all_reqs,
    uint32_t num_threads, double target_mops, uint64_t duration_us, RType type) {

  std::vector<std::thread> threads;
  for (int tid = 0; tid < kNumThd; tid++) {
    threads.push_back(std::thread([&, &reqs = all_reqs[tid], tid = tid, type=type]() {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::exponential_distribution<double> d(target_mops / num_threads);
      uint64_t cur_us = 0;

      while (cur_us < duration_us) {
        auto interval = std::max(1l, std::lround(d(gen)));
        PerfRequestWithTime req_with_time;
        req_with_time.start_us = cur_us;
        if(type==tCompose) {
          req_with_time.req = new ComposeRequest(tid);
        }
        else if(type==tRead) {
          req_with_time.req = new ReadRequest(tid);
        } else {
          std::cout << "Error: unknown request type" << std::endl;
          exit(1);
        }
        reqs.emplace_back(std::move(req_with_time));
        cur_us += interval;
      }
    }));
  }

  for (auto &thd : threads) {
    thd.join();
  }
}

std::vector<Trace> benchmark(
    std::vector<PerfRequestWithTime> *all_reqs,
    uint32_t num_threads, uint64_t miss_ddl_thresh_us) {
  std::vector<std::thread> threads;
  std::vector<Trace> all_traces[num_threads];
  int64_t failed_count = 0;

  for (uint32_t i = 0; i < num_threads; i++) {
    all_traces[i].reserve(all_reqs[i].size());
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back([&, &reqs = all_reqs[i], &traces = all_traces[i]] {
      auto start_us = microtime();
      for (const auto &req : reqs) {
        auto relative_us = microtime() - start_us;
        if (req.start_us > relative_us) {
          usleep(req.start_us - relative_us);
        } else if (req.start_us + miss_ddl_thresh_us < relative_us) {
          continue;
        }
        Trace trace;
        trace.absl_start_us = microtime();
        trace.start_us = trace.absl_start_us - start_us;
        bool ok = req.req->SendRequest();
        trace.duration_us = microtime() - start_us - trace.start_us;
        if (ok) {
          traces.push_back(trace);
        }
        else {
          failed_count++;
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::vector<Trace> gathered_traces;
  for (uint32_t i = 0; i < num_threads; i++) {
    gathered_traces.insert(gathered_traces.end(), all_traces[i].begin(),
                           all_traces[i].end());
  }

  std::cout << "Failed count: " << failed_count << std::endl << std::flush;

  return gathered_traces;
}



int main(int argc, char *argv[]) {
  // init_timer();
  reset(ComposeT);
  reset(ReadT);
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




  // gen_reqs(read_reqs, kNumThd, 1000, 100000000, tRead);
  // gen_reqs(compose_reqs, kNumThd, 1000, 100000000, tCompose);

  // ReadT.traces_ = move(benchmark(compose_reqs, kNumThd, 1000000));
  // auto real_duration_us = std::accumulate(ComposeT.traces_.begin(), ComposeT.traces_.end(), static_cast<uint64_t>(0),
  //                           [](uint64_t ret, Trace t) {
  //                             return std::max(ret, t.start_us + t.duration_us);
  //                           });
  // ReadT.real_mops_ = static_cast<double>(ReadT.traces_.size()) / real_duration_us;
  // std::cout << "Read Duration: " << real_duration_us/1000000 << " s" << std::endl;
  // std::cout << "Throughput: " << ComposeT.real_mops_ << " Mops"
  //           << std::endl;


  // ComposeT.traces_ = move(benchmark(read_reqs, kNumThd, 1000000));
  // real_duration_us = std::accumulate(ComposeT.traces_.begin(), ComposeT.traces_.end(), static_cast<uint64_t>(0),
  //                           [](uint64_t ret, Trace t) {
  //                             return std::max(ret, t.start_us + t.duration_us);
  //                           });
  // ComposeT.real_mops_ = static_cast<double>(ComposeT.traces_.size()) / real_duration_us;
  // std::cout << "Compose Duration: " << real_duration_us/1000000 << " s" << std::endl;
  // std::cout << "Throughput: " << ComposeT.real_mops_ << " Mops"
  //           << std::endl;



  return 0;
}




// void Perf::run(uint32_t num_threads, double target_mops, uint64_t duration_us,
//                uint64_t warmup_us, uint64_t miss_ddl_thresh_us) {
//   run_multi_clients(std::span<const netaddr>(), num_threads, target_mops,
//                     duration_us, warmup_us, miss_ddl_thresh_us);
// }

// void Perf::run_multi_clients(std::span<const netaddr> client_addrs,
//                              uint32_t num_threads, double target_mops,
//                              uint64_t duration_us, uint64_t warmup_us,
//                              uint64_t miss_ddl_thresh_us) {
//   std::vector<std::unique_ptr<PerfThreadState>> thread_states;
//   create_thread_states(&thread_states, num_threads);
//   std::vector<PerfRequestWithTime> all_warmup_reqs[num_threads];
//   std::vector<PerfRequestWithTime> all_perf_reqs[num_threads];
//   gen_reqs(all_warmup_reqs, thread_states, num_threads, target_mops, warmup_us);
//   gen_reqs(all_perf_reqs, thread_states, num_threads, target_mops, duration_us);
//   benchmark(all_warmup_reqs, thread_states, num_threads, miss_ddl_thresh_us);
//   tcp_barrier(client_addrs);
//   traces_ = move(
//       benchmark(all_perf_reqs, thread_states, num_threads, miss_ddl_thresh_us));
//   auto real_duration_us =
//       std::accumulate(traces_.begin(), traces_.end(), static_cast<uint64_t>(0),
//                       [](uint64_t ret, Trace t) {
//                         return std::max(ret, t.start_us + t.duration_us);
//                       });
//   real_mops_ = static_cast<double>(traces_.size()) / real_duration_us;
// }



// uint64_t Perf::get_average_lat() {
//   if (trace_format_ != kSortedByDuration) {
//     std::sort(traces_.begin(), traces_.end(),
//               [](const Trace &x, const Trace &y) {
//                 return x.duration_us < y.duration_us;
//               });
//     trace_format_ = kSortedByDuration;
//   }

//   auto sum = std::accumulate(
//       std::next(traces_.begin()), traces_.end(), 0ULL,
//       [](uint64_t sum, const Trace &t) { return sum + t.duration_us; });
//   return sum / traces_.size();
// }

// uint64_t Perf::get_nth_lat(double nth) {
//   if (trace_format_ != kSortedByDuration) {
//     std::sort(traces_.begin(), traces_.end(),
//               [](const Trace &x, const Trace &y) {
//                 return x.duration_us < y.duration_us;
//               });
//     trace_format_ = kSortedByDuration;
//   }

//   size_t idx = nth / 100.0 * traces_.size();
//   return traces_[idx].duration_us;
// }

// std::vector<Trace> Perf::get_timeseries_nth_lats(uint64_t interval_us,
//                                                  double nth) {
//   std::vector<Trace> timeseries;
//   if (trace_format_ != kSortedByStart) {
//     std::sort(
//         traces_.begin(), traces_.end(),
//         [](const Trace &x, const Trace &y) { return x.start_us < y.start_us; });
//     trace_format_ = kSortedByStart;
//   }

//   auto cur_win_us = traces_.front().start_us;
//   auto absl_cur_win_us = traces_.front().absl_start_us;
//   std::vector<uint64_t> win_durations;
//   for (auto &trace : traces_) {
//     if (cur_win_us + interval_us < trace.start_us) {
//       std::sort(win_durations.begin(), win_durations.end());
//       if (win_durations.size() >= 100) {
//         size_t idx = nth / 100.0 * win_durations.size();
//         timeseries.emplace_back(absl_cur_win_us, cur_win_us,
//                                 win_durations[idx]);
//       }
//       cur_win_us += interval_us;
//       absl_cur_win_us += interval_us;
//       win_durations.clear();
//     }
//     win_durations.push_back(trace.duration_us);
//   }

//   return timeseries;
// }

// double Perf::get_real_mops() const { return real_mops_; }

// const std::vector<Trace> &Perf::get_traces() const { return traces_; }

