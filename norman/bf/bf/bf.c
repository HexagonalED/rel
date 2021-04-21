#include "minirel.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "bf.h"
#include "bf_side.h"

int BFerrno;

/* Returns -1 on read error */
int Init_bpage(BFreq bq, BFpage *page)  
{
  page->dirty = FALSE;
  page->count = 1;
  page->fd = bq.fd;
  page->unixfd = bq.unixfd;
  page->pagenum = bq.pagenum;
  page->nextpage = NULL;
  page->prevpage = NULL;

  lseek(bq.unixfd, PAGE_SIZE + (PAGE_SIZE * bq.pagenum), SEEK_SET);
  return read(bq.unixfd, page->fpage.pagebuf, PAGE_SIZE);
}

/* Variables */
BFpage *free_list_head;
BFpage *lru_head;
BFhash_entry **hash_table;


void BF_Init(void)
{
  free_list_head = FL_Init(BF_MAX_BUFS);
  hash_table = HT_Init(BF_HASH_TBL_SIZE);
}


int BF_GetBuf(BFreq bq, PFpage **fpage)
{
  BFpage *bpage;

  /* First look in the hash table */
  bpage = HT_Find(hash_table, bq.fd, bq.pagenum);
  if (bpage)
  {
    ++bpage->count;
  } else {
    if (free_list_head)
    {
      /* if there is available space in FreeList just use that */
      bpage = FL_Pop(&free_list_head);
    }
    else
    {
      /* Will set bpage to the now cleared page */
      BFerrno = LRU_ClearLast(lru_head, hash_table, &bpage);
      if (BFerrno != BFE_OK)
      {
        return BFerrno;
      }
    }

    if (Init_bpage(bq, bpage) == -1)
    {
      BFerrno = BFE_INCOMPLETEREAD;
      return BFE_INCOMPLETEREAD;
    }

    LRU_Push(&lru_head, bpage);
    HT_Add(hash_table, bpage);
  }

  *fpage = &bpage->fpage;
  BFerrno = BFE_OK;
  return BFE_OK;
}


int BF_AllocBuf(BFreq bq, PFpage **fpage)
{
  BFpage *bpage;

  bpage = HT_Find(hash_table, bq.fd, bq.pagenum);
  if (bpage)
  {
    return BFE_PAGEINBUF;
  }

  if (free_list_head)
  {
    bpage = FL_Pop(&free_list_head);
  } else {
    BFerrno = LRU_ClearLast(lru_head, hash_table, &bpage);
    if (BFerrno != BFE_OK)
    {
      return BFerrno;
    }
  }

  bpage->dirty = FALSE;
  bpage->count = 1;
  bpage->fd = bq.fd;
  bpage->unixfd = bq.unixfd;
  bpage->pagenum = bq.pagenum;
  bpage->nextpage = NULL;
  bpage->prevpage = NULL;

  LRU_Push(&lru_head, bpage);
  HT_Add(hash_table, bpage);

  *fpage = &bpage->fpage;
  return BFE_OK;
}


int BF_UnpinBuf(BFreq bq)
{
  BFpage *page = HT_Find(hash_table, bq.fd, bq.pagenum);

  if (!hash_table)
  {
    BFerrno = BFE_HASHNOTFOUND;
    return BFE_HASHNOTFOUND;
  }

  if (!page)
  {
    BFerrno = BFE_PAGENOTINBUF;
    return BFE_PAGENOTINBUF;
  }

  if (page->count <= 0)
  {
    BFerrno = BFE_PAGEUNPINNED;
    return BFE_PAGEUNPINNED;
  }

  --(page->count);
  BFerrno = BFE_OK;
  return BFE_OK;
}


int BF_TouchBuf(BFreq bq)
{
  BFpage *page = HT_Find(hash_table, bq.fd, bq.pagenum);

  if (!hash_table)
  {
    BFerrno = BFE_HASHNOTFOUND;
    return BFE_HASHNOTFOUND;
  }

  if (!page)
  {
    BFerrno = BFE_PAGENOTINBUF;
    return BFE_PAGENOTINBUF;
  }

  if (page->count <= 0)
  {
    BFerrno = BFE_PAGEUNPINNED;
    return BFE_PAGEUNPINNED;
  }

  page->dirty = TRUE;

  LRU_Remove(&lru_head, page);
  LRU_Push(&lru_head, page);
  BFerrno = BFE_OK;
  return BFE_OK;
}


int BF_FlushBuf(int fd)
{
  BFpage *bpage, *next;

  bpage = lru_head;

  while (bpage)
  {
    next = bpage->nextpage;

    if (bpage->fd == fd)
    {
      if (bpage->count > 0)
      {
        BFerrno = BFE_PAGEPINNED;
        return BFE_PAGEPINNED;
      }

      if (bpage->dirty)
      {
        
        BFerrno = BFE_UNIX*lseek(bpage->unixfd, PAGE_SIZE + (PAGE_SIZE * bpage->pagenum), SEEK_SET);
        if (BFerrno == BFE_UNIX)
        {
          return BFE_UNIX;
        }
        if (write(bpage->unixfd, bpage->fpage.pagebuf, PAGE_SIZE) == -1)
        {
          BFerrno = BFE_INCOMPLETEWRITE;
          return BFE_INCOMPLETEWRITE;
        }
      }

      HT_Remove(hash_table, bpage->fd, bpage->pagenum);
      LRU_Remove(&lru_head, bpage);

      FL_Push(&free_list_head, bpage);
    }

    bpage = next;
  }
  BFerrno = BFE_OK;
  return BFE_OK;
}


void BF_ShowBuf(void)
{
  unsigned int i;
  BFpage *bpage;

  i = 0;
  bpage = lru_head;

  printf("\n\tBF_SHowBuf start: \n\n");
  while (bpage)
  {
    printf("%u [ fd: %i, pagenum: %i, unixfd: %i, dirty: %i ]\n", i++,
           bpage->fd, bpage->pagenum, bpage->unixfd, bpage->dirty);
    bpage = bpage->nextpage;
  }
  printf("\n\n\tBF_ShowBuf end... \n");
}



/* BFE_MISSDIRTY ; BFE_INVALIDTID ; BFE_MSGERR ; BFE_HASHPAGEEXIST unused*/
void BF_PrintError(const char *s)
{
  int errcode;
  BF_ShowBuf();
  errcode = BFerrno;
  switch(errcode)
  {
    case BFE_OK: fprintf(stderr, "\n no error \n\n");

    case BFE_NOMEM: fprintf(stderr, "\n BF: no memory \n\n");

    case BFE_NOBUF: fprintf(stderr, "\n BF: empty lru \n\n");

    case BFE_PAGEPINNED: fprintf(stderr, "\n BF: can not flush pinned page \n\n");

    case BFE_PAGEUNPINNED: fprintf(stderr, "\n BF: unpinned page in use/dirty \n\n");

    case BFE_PAGEINBUF: fprintf(stderr, "\n BF: page in buffer pool \n\n");
    
    case BFE_PAGENOTINBUF: fprintf(stderr, "\n BF: page not in buffer pool \n\n");

    case BFE_INCOMPLETEWRITE: fprintf(stderr, "\n BF: incomplete write on disk \n\n ");

    case BFE_INCOMPLETEREAD: fprintf(stderr, "\n BF: incomplete read \n\n ");

    case BFE_HASHNOTFOUND: fprintf(stderr, "\n BF: could not find page in hashtable \n");
    
    case BFE_UNIX: fprintf(stderr, "\n BF: error on pwrite/pread \n\n ");

    default: fprintf(stderr, "\n BF: unused error code : %d \n\n ", errcode);
  }
  fprintf(stderr, "Comment provided: %s\n", s);

  exit(-1);
}
