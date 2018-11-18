#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include <queue>
#include <set>
#include <algorithm>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  std::map<lock_protocol::lockid_t, std::queue<std::string> > lock_map;
  std::map<lock_protocol::lockid_t, std::set<std::string> > wait_set;
  static pthread_mutex_t mutex;
  static pthread_cond_t cond;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
