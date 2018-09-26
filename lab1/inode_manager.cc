#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  if (buf && id < BLOCK_NUM)
  {
    memcpy(buf, (char *)blocks[id], BLOCK_SIZE);
  }
}

void disk::write_block(blockid_t id, const char *buf)
{
  if (buf && id < BLOCK_NUM)
  {
    memcpy((char *)blocks[id], buf, BLOCK_SIZE);
  }
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  int bitmap_n = BLOCK_NUM / BPB; // the number of blocks contains bitmap
  char bitmap[BLOCK_SIZE];
  for(int i = 0; i < bitmap_n; i++)
  {
    read_block(i+2,bitmap); // skip root block and superblock
    for(int j = 0; j < BPB;j++)
    {
      if(bitmap[j] == 1)
      {
        continue;
      }
      bitmap[j]=1;
      write_block(i+2,bitmap);
      return i*BPB + j;
    }
  }
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char bitmap[BLOCK_SIZE];
  read_block((id/BPB)+2,bitmap); // read corresponding block of the bitmap contianing the block
  bitmap[id%BPB] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  struct inode *ino_disk;
  char buf[BLOCK_SIZE];
  uint32_t i = 1;
  for (i; i <= bm->sb.ninodes; i++)
  {
    bm->read_block(IBLOCK(i, bm->sb.nblocks), buf);
    for (int j = 0; j < IPB; j++)
    {
      ino_disk = (struct inode *)buf + j;
      if (ino_disk->type == 0)
      {
        ino_disk->type = type;
        ino_disk->size = 0;
        ino_disk->ctime = time(0);
        ino_disk->atime = time(0);
        ino_disk->mtime = time(0);
        bm->write_block(IBLOCK(i, bm->sb.nblocks), buf);
        return i;
      }
    }
  }
}

void inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM)
  {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode *)buf + inum % IPB;
  if (ino_disk->type == 0)
  {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define BLOCK_QUANTITY(size) size/BLOCK_SIZE + !(size%BLOCK_SIZE == 0)

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  int cursor = 0; // current reading position
  struct inode *ino = get_inode(inum);
  int block_q = BLOCK_QUANTITY(ino->size); //data block number
  char *buf = (char*)malloc(block_q*BLOCK_SIZE);
  int indirect[NINDIRECT];
  if(block_q>NDIRECT)
    bm->read_block(ino->blocks[NDIRECT],(char*)indirect);
  for(int i = 0; i < block_q;i++)
  {
    if(i<NDIRECT)
    {
      bm->read_block(ino->blocks[i], buf+cursor); //read from direct block
    }
    else
    {
      bm->read_block(indirect[i], buf+cursor); // read from indirect block
    }
    cursor += BLOCK_SIZE;
  }

  *size = ino->size;
  *buf_out = buf;
  ino->atime++;
  put_inode(inum,ino);
  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino = get_inode(inum);
  int new_block_q = BLOCK_QUANTITY(size);
  int old_block_q = BLOCK_QUANTITY(ino->size);
  int indirect[NINDIRECT];
  int cursor = 0; // record current position
  if(new_block_q>NDIRECT)
    bm->read_block(ino->blocks[NDIRECT],(char*)indirect);
  blockid_t blockid_temp;
  if(new_block_q > old_block_q) // alloc free blocks if needed 
  {
    for(int i = old_block_q; i < new_block_q; i++)
    {
      blockid_temp = bm->alloc_block();
      if(i<NDIRECT)
      {
        ino->blocks[i] = blockid_temp;
      }
      else
      {
      indirect[i-NDIRECT]=blockid_temp;
      }
    }
    bm->write_block(ino->blocks[NDIRECT],(char*)indirect);
  }

  for(int j = 0; j , new_block_q; j++) // write file
  {
    if(j < NINDIRECT)
    {
      bm->write_block(ino->blocks[j], buf+cursor);
    }
    else
    {
      bm->write_block(indirect[j-NINDIRECT], buf+cursor);
    }
    cursor += BLOCK_SIZE;
  }

  ino->size = size;
  ino->mtime++;
  ino->atime++;
  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode *ino;
  ino = get_inode(inum);
  if (ino != NULL)
  {
    a.size = ino->size;
    a.type = ino->type;
    a.atime = ino->atime;
    a.ctime = ino->ctime;
    a.mtime = ino->mtime;
  }
  free(ino);
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  return;
}
