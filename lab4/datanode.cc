#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr)
{
	ec = new extent_client(extent_dst);

	// Generate ID based on listen address
	id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
	id.set_hostname(GetHostname());
	id.set_datanodeuuid(GenerateUUID());
	id.set_xferport(ntohs(bindaddr->sin_port));
	id.set_infoport(0);
	id.set_ipcport(0);

	// Save namenode address and connect
	make_sockaddr(namenode.c_str(), &namenode_addr);
	if (!ConnectToNN())
	{
		delete ec;
		ec = NULL;
		return -1;
	}

	// Register on namenode
	if (!RegisterOnNamenode())
	{
		delete ec;
		ec = NULL;
		close(namenode_conn);
		namenode_conn = -1;
		return -1;
	}

	/* Add your initialization here */
	NewThread(this, &DataNode::heartbeat);

	if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir

	return 0;
}

void DataNode::heartbeat()
{
	while(true)
	{
		SendHeartbeat();
		sleep(1);
	}
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf)
{
	/* Your lab4 part 2 code */
	fprintf(stderr, "dn read block bid: %d offset: %d len: %d \n----------------------------------\n", bid, offset, len);
	fflush(stderr);
	string ini_buf;
	ec->read_block(bid, ini_buf);
	if(offset > ini_buf.size())
		buf = "";
	else
		buf = ini_buf.substr(offset, len);
	printf("buf is : ", buf.c_str());
	fflush(stdout);

	return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf)
{
	/* Your lab4 part 2 code */
	fprintf(stderr, "dn write block bid: %d offset: %d len: %d buf: %s  \n----------------------------------\n", bid, offset, len, buf.c_str());
	fflush(stderr);
	if (buf.size() == 0)
	{
		printf("size 0\n");
		fflush(stdout);
		return true;		
	}
	string ini_buf;
	ec->read_block(bid, ini_buf);
	ini_buf = ini_buf.substr(0, offset) + buf + ini_buf.substr(offset+len);
	ec->write_block(bid, ini_buf);

	return true;
}
