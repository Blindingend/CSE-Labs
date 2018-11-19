// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;
static u_int sendtime=5;
enum {unlock, locked, apply, revoke, hold, discard} state;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  for (int i = 0; i < 1100; i++)
  {
    cond[i] = PTHREAD_COND_INITIALIZER;
  }
  mutex = PTHREAD_MUTEX_INITIALIZER;
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  while(lock_map[lid] == discard)
  {
    pthread_cond_wait(&cond[lid], &mutex);
  }
  if(lock_map[lid] == unlock)
  {
    lock_map[lid] = apply;
    int tr = 9;
    int ac_ret;
    pthread_mutex_unlock(&mutex);
    ac_ret = cl->call (lock_protocol::acquire, lid, id, tr);
    pthread_mutex_lock(&mutex);
    int stat = lock_map[lid];
    if(stat == revoke)
    {
      
    }
    else if(ac_ret == lock_protocol::RETRY)
    {
      pthread_cond_wait(&cond[lid], &mutex);
    }
    else if(ac_ret == rlock_protocol::REVOKE)
    {
      lock_map[lid] = revoke;
    }
    else if(lock_map[lid] == apply)
    {
      lock_map[lid] = locked;
    }
  }
  else if(lock_map[lid] == hold)
  {
    lock_map[lid] = locked;
  }
  else{
    pthread_cond_wait(&cond[lid], &mutex);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(!pthread_cond_destroy(&cond[lid]))
  {
    pthread_cond_init(&cond[lid], NULL);
    if (lock_map[lid] == revoke)
    {
      lock_map[lid] = discard;
      pthread_mutex_unlock(&mutex);
      int tr = 9;
      cl->call(lock_protocol::release, lid, id, tr);
      pthread_mutex_lock(&mutex);
      lock_map[lid] = unlock;
    }else{
      lock_map[lid] = hold;
    }
  }
  else{
    pthread_cond_signal(&cond[lid]);
  }
  pthread_mutex_unlock(&mutex);
  return ret;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &r)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lock_map[lid] == hold){
    lock_map[lid] = discard;
    ret = 1;
  }else if (lock_map[lid] == discard){
    lock_map[lid] = unlock;
    pthread_cond_broadcast(&cond[lid]);
  }else if (lock_map[lid] > unlock){
    lock_map[lid] = revoke;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                  int &r)
{
 pthread_mutex_lock(&mutex);
  int ret = rlock_protocol::OK;
  if (lock_map[lid] == apply){
    if (state) lock_map[lid] = revoke;
    else lock_map[lid] = locked;
    pthread_cond_signal(&cond[lid]);
    r = rlock_protocol::OK;
  }else{
    r = 2;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}



