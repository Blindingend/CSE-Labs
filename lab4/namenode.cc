#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst)
{
	ec = new extent_client(extent_dst);
	lc = new lock_client_cache(lock_dst);
	yfs = new yfs_client(extent_dst, lock_dst);

	/* Add your init logic here */
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino)
{
	std::cout << "get locations:" << ino << std::endl;
	
	cout.flush();

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
		offset+=BLOCK_SIZE;
	}
	return LBList;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size)
{
	std::cout << "complete:" << ino << "size:" << new_size << std::endl;
	cout.flush();

	extent_protocol::status res = ec->complete(ino, new_size);
	if(res == extent_protocol::OK)
	{
		lc->release(ino);
		return true;
	}
	lc->release(ino);
	return false;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino)
{
	std::cout << "append:" << ino << std::endl;
	cout.flush();
	
	blockid_t blockid;
	extent_protocol::attr attr;
	ec->getattr(ino, attr);
	ec->append_block(ino, blockid);
	
	LocatedBlock lb(blockid, attr.size, (attr.size % BLOCK_SIZE) ? attr.size & BLOCK_SIZE : BLOCK_SIZE, GetDatanodes());
	return lb;
	
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name)
{
	std::cout << "rename:" << src_dir_ino << src_name << " dst:" << dst_dir_ino << dst_name << std::endl;
	cout.flush();
	
	string src_buf, dst_buf;
	ec->get(src_dir_ino, src_buf);
	ec->get(dst_dir_ino, dst_buf);
	bool found = false;
	int i = 0;
	yfs_client::inum dst_ino;
	const char * nametemp;
	while(i < src_buf.size())
	{
		nametemp = src_buf.c_str() + i;
		if(strcmp(nametemp, src_name.c_str()))
		{
			found = true;
			dst_ino = *(uint32_t *)(nametemp + strlen(nametemp) + 1);
			src_buf.erase(i, strlen(nametemp) + 1 + sizeof(uint));
			break;
		}
		i += strlen(nametemp) + 1 + sizeof(uint);
	}
	if(found)
	{
		if (src_dir_ino == dst_dir_ino)
		{
     	 	dst_buf = src_buf;
    	}
		dst_buf += dst_name;
		dst_buf.resize(dst_buf.size() + 5, 0);
		*(uint32_t *)(dst_buf.c_str()+dst_buf.size()-4) = dst_ino;
		ec->put(src_dir_ino, src_buf);
		ec->put(dst_dir_ino, dst_buf);
	}
	return found;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
	// std::cout << "mkdir:" << name << "mode:" << mode << std::endl;
	// cout.flush();
	fprintf(stderr, "mkdir woshilog\n");
	fflush(stderr);

	
	bool res = yfs->mkdir(parent, name.c_str(), mode, ino_out);
	return !res;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out)
{
	// std::cout << "create:" << name << "mode:" << mode << std::endl;
	// cout.flush();
	fprintf(stderr, "create woshilog\n");
	fflush(stderr);
	
	bool res = yfs->create(parent, name.c_str(), mode, ino_out);
	return !res;
}

bool NameNode::Isfile(yfs_client::inum ino)
{
	fprintf(stderr, "isfile woshilog\n");
	fflush(stderr);

	// bool res  = yfs->isfile(ino);

	extent_protocol::attr a;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        printf("error getting attr\n");
		fflush(stdout);

        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", ino);
		fflush(stdout);

        return true;
    } 
    printf("isfile: %lld is a dir\n", ino);
	fflush(stdout);

    return false;
}

bool NameNode::Isdir(yfs_client::inum ino)
{
	fprintf(stderr, "isdir woshilog\n");
	fflush(stderr);

	// bool res = yfs->isdir(ino);
	extent_protocol::attr a;
	if (ec->getattr(ino, a) != extent_protocol::OK)
	{
		printf("get attr error");
		fflush(stdout);

		return false;
	}
	if (a.type == extent_protocol::T_DIR)
	{
		printf("isdir: %lld is a dir\n", ino);
		fflush(stdout);

		return true;
	}
	return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info)
{
	fprintf(stderr, "getfile woshilog\n");
	fflush(stderr);

	extent_protocol::attr attr;
	if(ec->getattr(ino, attr) != extent_protocol::OK)
	{
		return false;
	}
	info.atime = attr.atime;
	info.mtime = attr.mtime;
	info.ctime = attr.ctime;
	info.size = attr.size;

	return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info)
{
	fprintf(stderr, "getdir woshilog\n");
	fflush(stderr);

	extent_protocol::attr attr;
	if(ec->getattr(ino, attr) != extent_protocol::OK)
	{
		return false;
	}
	info.atime = attr.atime;
	info.mtime = attr.mtime;
	info.ctime = attr.ctime;


	fprintf(stderr, "getdir2 woshilog\n");
	fflush(stderr);
	return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir)
{
	fprintf(stderr, "readdir woshilog\n");
	fflush(stderr);

	// bool res = yfs->readdir(ino, dir);

	dir.clear();
	std:string buf;
	yfs_client::dirent entry;
	if(ec->get(ino, buf) != extent_protocol::OK)
	{
		printf("readdir: error with get\n");
		fflush(stderr);
		return false;
	}
	std::istringstream ist(buf);
	while(std::getline(ist, entry.name, '\0'))
	{
		ist >> entry.inum;
		dir.push_back(entry);
	}
	return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino)
{
	// std::cout << "unlink" << parent << " and " << name << std::endl;
	// cout.flush();

	fprintf(stderr, "unlink woshilog\n");
	fflush(stderr);


	bool res = yfs->unlink(parent, name.c_str());
	return res;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id)
{

}

void NameNode::RegisterDatanode(DatanodeIDProto id)
{
}

list<DatanodeIDProto> NameNode::GetDatanodes()
{
	return list<DatanodeIDProto>();
}
