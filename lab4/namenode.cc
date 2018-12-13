#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

static int tick = 1;

void NameNode::init(const string &extent_dst, const string &lock_dst)
{
	ec = new extent_client(extent_dst);
	lc = new lock_client_cache(lock_dst);
	yfs = new yfs_client(ec, lc);

	/* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino)
{
	fprintf(stderr, "getblock %d \n------------------------------\n", ino);
	fflush(stderr);

	std::list<blockid_t> blockids;
	list<LocatedBlock> LBList;
	ec->get_block_ids(ino, blockids);
	extent_protocol::attr attr;
	ec->getattr(ino, attr);
	long long offset = 0;
	long int i = 0;
	for (auto item : blockids)
	{
		i++;
		LocatedBlock lb(item, offset, (i < blockids.size() ? BLOCK_SIZE : (attr.size - offset)), GetDatanodes());
		LBList.push_back(lb);
		offset += BLOCK_SIZE;
	}
	return LBList;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size)
{
	fprintf(stderr, "complete %d %d \n------------------------------\n", ino, new_size);
	fflush(stderr);

	extent_protocol::status res = ec->complete(ino, new_size);
	if (res == extent_protocol::OK)
	{
		lc->release(ino);
		return true;
	}
	return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino)
{
	fprintf(stderr, "appendblock %d \n------------------------------\n", ino);
	fflush(stderr);

	blockid_t blockid;
	extent_protocol::attr attr;
	ec->getattr(ino, attr);
	fprintf(stderr, "append getattr ok \n------------------------------\n");
	fflush(stderr);
	ec->append_block(ino, blockid);
	fprintf(stderr, "append ok %d \n------------------------------\n", blockid);
	fflush(stderr);

	LocatedBlock lb(blockid, attr.size, (attr.size % BLOCK_SIZE) ? attr.size & BLOCK_SIZE : BLOCK_SIZE, GetDatanodes());
	return lb;
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name)
{
	fprintf(stderr, "rename %d %s %d %s \n---------------------------\n", src_dir_ino, src_name.c_str(), dst_dir_ino, dst_name.c_str());
	fflush(stderr);

	string src_buf, dst_buf;
	ec->get(src_dir_ino, src_buf);
	ec->get(dst_dir_ino, dst_buf);
	bool found = false;
	int i = 0;
	yfs_client::inum dst_ino;
	const char *nametemp;
	while (i < src_buf.size())
	{
		nametemp = src_buf.c_str() + i;
		if (!strcmp(nametemp, src_name.c_str()))
		{
			found = true;
			dst_ino = *(uint32_t *)(nametemp + strlen(nametemp) + 1);
			src_buf.erase(i, strlen(nametemp) + 1 + sizeof(uint));
			break;
		}
		i += strlen(nametemp) + 1 + sizeof(uint);
	}
	if (found)
	{
		if (src_dir_ino == dst_dir_ino)
		{
			dst_buf = src_buf;
		}
		dst_buf += dst_name;
		dst_buf.resize(dst_buf.size() + 5, 0);
		*(uint32_t *)(dst_buf.c_str() + dst_buf.size() - 4) = dst_ino;
		ec->put(src_dir_ino, src_buf);
		ec->put(dst_dir_ino, dst_buf);
	}
	return found;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
	// std::cout << "mkdir:" << name << "mode:" << mode << std::endl;
	// cout.flush();
	fprintf(stderr, "mkdir %s %d in %d \n------------------------------\n", name.c_str(), ino_out);
	fflush(stderr);

	bool res = yfs->mkdir(parent, name.c_str(), mode, ino_out);
	return !res;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
	// std::cout << "create:" << name << "mode:" << mode << std::endl;
	// cout.flush();
	fprintf(stderr, "create %d %s %d \n------------------------------\n", parent, name.c_str(), ino_out);
	fflush(stderr);
	lc->release(parent);
	bool res = yfs->create(parent, name.c_str(), mode, ino_out);

	return !res;
}

bool NameNode::Isfile(yfs_client::inum inum) {
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
      printf("error getting attr\n");
      return false;
  }

  if (a.type == extent_protocol::T_FILE) {
      printf("isfile: %lld is a file\n", inum);
      return true;
  }else if (a.type == extent_protocol::T_SYMLK) {
      printf("isfile: %lld is a symlink\n", inum);
      return false;
  } 
  return false;
}

bool NameNode::Isdir(yfs_client::inum inum) {
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
      printf("error getting attr\n");
      return false;
  }
  if (a.type == extent_protocol::T_DIR) {
      printf("isfile: %lld is a dir\n", inum);
      return true;
  } 
  printf("isfile: %lld is not a dir\n", inum);
  return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &fin) {
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK) {
      return false;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;

  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &din) {
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        return false;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;
    return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  std::string buf;
  ec->get(ino, buf);
  int pos = 0;
  while(pos < buf.size()){
      struct yfs_client::dirent temp;
      temp.name = std::string(buf.c_str()+pos);
      temp.inum = *(uint32_t *)(buf.c_str() + pos + temp.name.size() + 1);
      dir.push_back(temp);
      pos += temp.name.size() + 1 + sizeof(uint32_t);
  }
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino)
{
	fprintf(stderr, "unlink parent: %d, name: %s, ino: %d \n------------------------------\n", parent, name.c_str(), ino);
	fflush(stderr);

	// bool res = !yfs->unlink(parent, name.c_str());
	std::string buf;
	ec->get(parent, buf);

	int pos = 0;
	while (pos < buf.size())
	{
		const char *t = buf.c_str() + pos;
		if (strcmp(t, name.c_str()) == 0)
		{
			uint32_t ino = *(uint32_t *)(t + strlen(t) + 1);
			buf.erase(pos, strlen(t) + 1 + sizeof(uint32_t));
			ec->put(parent, buf);
			ec->remove(ino);
			goto done;
		}
		pos += strlen(t) + 1 + sizeof(uint32_t);
	}
	return false;
done:
	return true;
	fprintf(stderr, "unlink ok\n");
	fflush(stderr);
	// return res;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id)
{
}

void NameNode::RegisterDatanode(DatanodeIDProto id)
{
}

list<DatanodeIDProto> NameNode::GetDatanodes()
{
	list<DatanodeIDProto> l;
	l.push_back(master_datanode);
	return l;
}
