// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache():
  nacquire (0)
{
  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lock_map[lid].empty())
  // the lock is free
  {
    lock_map[lid].push(id);
    r = lock_protocol::OK;
  }else if (find(wait_set[lid].begin(), wait_set[lid].end(), id) == wait_set[lid].end())
  // lock is not free and the client haven't acquire before
  {
    ret = lock_protocol::RETRY;
    lock_map[lid].push(id);
    wait_set[lid].insert(id);
    if (wait_set[lid].size() == 1)
    // there is no other client waiting for it
    {
      std::string t = lock_map[lid].front();
      handle h(t);
      rpcc *cl = h.safebind();
      int tr = 9;
      int rev_ret;
      pthread_mutex_unlock(&mutex);
      rev_ret = cl->call(rlock_protocol::rev, lid, tr);
      pthread_mutex_lock(&mutex);
      if (rev_ret == 1) 
      {
        wait_set[lid].erase(lock_map[lid].front());
        lock_map[lid].pop();
        if (lock_map[lid].size() > 1)
          ret = lock_protocol::RPCERR;
        else
          ret = 0;
        pthread_mutex_unlock(&mutex);
        cl->call(rlock_protocol::rev, lid, tr);
        pthread_mutex_lock(&mutex);
      }
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }else{
    r = lock_protocol::OK;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

int lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lock_map[lid].empty() && lock_map[lid].front() != id)
  {
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  lock_map[lid].pop();
  wait_set[lid].erase(id);
  if (!lock_map[lid].empty())
  {
    std::string t = lock_map[lid].front();
    int state = lock_map[lid].size() > 1;
    handle h(t);
    rpcc *cl = h.safebind();
    int tr = 9;
    pthread_mutex_unlock(&mutex);
    cl->call(rlock_protocol::retry, lid, state, tr);
    return ret;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

