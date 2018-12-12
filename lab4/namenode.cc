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
	yfs = new yfs_client(extent_dst, lock_dst);

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
		offset+=BLOCK_SIZE;
	}
	return LBList;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size)
{
	fprintf(stderr, "complete %d %d \n------------------------------\n", ino, new_size);
	fflush(stderr);

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
	fprintf(stderr, "rename %d %s -> %d %s \n------------------------------\n",src_dir_ino, src_name.c_str(), dst_dir_ino, dst_name.c_str());
	fflush(stderr);
	
	bool found = false;
	std::list<yfs_client::dirent> src_dir_list;
	std::list<yfs_client::dirent> dst_dir_list;
	yfs->readdir(src_dir_ino, src_dir_list);
	yfs->readdir(dst_dir_ino, dst_dir_list);
	
	std::list<yfs_client::dirent>::iterator it;
	yfs_client::dirent temp;

	for(it = src_dir_list.begin(); it != src_dir_list.end(); it++)
	{
		if(it->name == src_name)
		{
			temp.name = it->name;
			temp.inum = it->inum;
			found = true;
			break;
		}
	}
	if(found)
	{
		if(src_dir_ino == dst_dir_ino)
		{
			dst_dir_list = src_dir_list;
		}
		dst_dir_list.push_back(temp);
		yfs->writedir(src_dir_ino, src_dir_list);
		yfs->writedir(dst_dir_ino, dst_dir_list);
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
	
	bool res = yfs->create(parent, name.c_str(), mode, ino_out);
	// if(tick == 1)
	// {
	// 	unsigned long long int a = 3;
	// 	yfs->create(1, "test_put", mode, a);
	// 	tick -= 1;
	// }†
	return !res;
}

bool NameNode::Isfile(yfs_client::inum ino)
{
	// fprintf(stderr, "isfile \n------------------------------\n");
	// fflush(stderr);

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
	// fprintf(stderr, "isdir \n------------------------------\n");
	// fflush(stderr);

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
	fprintf(stderr, "getfile %d \n------------------------------\n", ino);
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
	// fprintf(stderr, "getdir \n------------------------------\n");
	// fflush(stderr);

	extent_protocol::attr attr;
	if(ec->getattr(ino, attr) != extent_protocol::OK)
	{
		return false;
	}
	info.atime = attr.atime;
	info.mtime = attr.mtime;
	info.ctime = attr.ctime;


	// fprintf(stderr, "getdir2 \n------------------------------\n");
	// fflush(stderr);
	return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir)
{
	fprintf(stderr, "readdir \n------------------------------\n");
	fflush(stderr);

	// bool res = yfs->readdir(ino, dir);

	dir.clear();
	std:string buf;
	yfs_client::dirent entry;
	if(ec->get(ino, buf) != extent_protocol::OK)
	{
		printf("readdir: error with get\n");
		fflush(stdout);
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
	fprintf(stderr, "unlink parent: %d, name: %s, ino: %d \n------------------------------\n", parent, name.c_str(), ino);
	fflush(stderr);

	bool res = !yfs->unlink(parent, name.c_str());
	fprintf(stderr, "unlink ok\n");
	fflush(stderr);
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
	list<DatanodeIDProto> l;
	l.push_back(master_datanode);
	return l;
}
