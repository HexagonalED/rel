#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "minirel.h"
#include "bf_side.h"
#include "bf.h"


/***********************/
/* HashTable functions */
/***********************/
BFhash_entry **HT_Init(unsigned int size)
{
  unsigned int i;
  BFhash_entry **table;

  table = malloc(sizeof(BFhash_entry *) * size);
  if (!table)
  {
    BFerrno=BFE_NOMEM;
  }
  for (i = 0; i < size; ++i)
  {
    table[i] = NULL;
  }

  return table;
}


void HT_Clean(BFhash_entry **table, unsigned int size)
{
  unsigned int i;
  BFhash_entry *curr, *next;

  for (i = 0; i < size; ++i)
  {
    if (table[i] != NULL)
    {
      curr = table[i];

      while (curr)
      {
        next = curr->nextentry;
        free(curr);
        curr = next;
      }
    }
  }

  free(table);
}

/*
 * Returns the index in the hash table of the given fd and pagenum
 * Uses a very simple hash algorithm that may be improved
 */
unsigned int HT_Index(int fd, int pagenum)
{
  return (BF_HASH_TBL_SIZE * fd + pagenum) % BF_HASH_TBL_SIZE;
}


BFpage *HT_Find(BFhash_entry **table, int fd, int pagenum)
{
  unsigned int index;
  BFhash_entry *entry;

  index = HT_Index(fd, pagenum);
  entry = table[index];

  while (entry)
  {
    if (entry->fd == fd && entry->pagenum == pagenum)
    {
      return entry->bpage;
    }

    entry = entry->nextentry;
  }

  return NULL;
}


void HT_Add(BFhash_entry **table, BFpage *page)
{
  BFhash_entry *entry, *prev;
  const unsigned int index = HT_Index(page->fd, page->pagenum);

  entry = table[index];
  prev = NULL;

  while (entry)
  {
    prev = entry;
    entry = entry->nextentry;
  }

  entry = malloc(sizeof(BFhash_entry));
  if (!entry)
  {
    BFerrno = BFE_NOMEM;
  }
  entry->bpage = page;
  entry->fd = page->fd;
  entry->pagenum = page->pagenum;
  entry->preventry = NULL;
  entry->nextentry = NULL;

  if (prev)
  {
    prev->nextentry = entry;
    entry->preventry = prev;
  }

  if (table[index] == NULL)
  {
    table[index] = entry;
  }
}


void HT_Remove(BFhash_entry **table, int fd, int pagenum)
{
  BFhash_entry *entry;
  const unsigned int index = HT_Index(fd, pagenum);

  entry = table[index];

  while (entry)
  {
    if (entry->fd == fd && entry->pagenum == pagenum)
    {
      if (entry->preventry)
      {
        entry->preventry->nextentry = entry->nextentry;
      }
      if (entry->nextentry)
      {
        entry->nextentry->preventry = entry->preventry;
      }

      if (entry == table[index])
      {
        table[index] = entry->nextentry;
      }

      free(entry);
      break;
    }

    entry = entry->nextentry;
  }
}




/*****************/
/* LRU functions */
/*****************/
void LRU_Push(BFpage **head, BFpage *new_node)
{
  if (*head == new_node)
  {
    return;
  }

  new_node->prevpage = 0;
  new_node->nextpage = *head;
  if (*head)
  {
    (*head)->prevpage = new_node;
  }
  *head = new_node;
}


void LRU_Remove(BFpage **head, BFpage *page)
{
  if (!page)
  {
    return;
  }

  if (*head == page)
  {
    *head = page->nextpage;
  }

  if (page->prevpage)
  {
    page->prevpage->nextpage = page->nextpage;
  }
  if (page->nextpage)
  {
    page->nextpage->prevpage = page->prevpage;
  }
}


/*
 * This function does a bit more than the name suggests
 * It handles everything regarding removing an element from the LRU,
 * writing to disk, and removes it from the hash table
 *
 * Returns BFE_INCOMPLETEWRITE on write error,
 * BFE_PAGEFIXED if no pages can be replaced,
 * BFE_OK otherwise
 * Writes the cleared BFpage to *ret_bpage
 */
int LRU_ClearLast(BFpage *lru_head, BFhash_entry **hash_table,
                  BFpage **ret_bpage)
{
  BFpage *bpage = lru_head;
  /*
   * Find least recently used object
   */
  if (lru_head == NULL)
  {
    BFerrno = BFE_NOBUF;
    return BFerrno;
  }
  while (bpage->nextpage)
  {
    bpage = bpage->nextpage;
  }

  /* We need to find a page that's not pinned */
  while (bpage && bpage->count > 0)
  {
    bpage = bpage->prevpage;
  }

  if (!bpage)
  {
    BFerrno = BFE_PAGEPINNED ;
    return BFerrno;
  }

  if (bpage->dirty)
  {
    lseek(bpage->unixfd, PAGE_SIZE + (PAGE_SIZE * bpage->pagenum), SEEK_SET);
    if (write(bpage->unixfd, bpage->fpage.pagebuf, PAGE_SIZE) == -1)
    {
      BFerrno = BFE_INCOMPLETEWRITE ;
      return BFerrno;
    }
  }

  HT_Remove(hash_table, bpage->fd, bpage->pagenum);

  LRU_Remove(&lru_head, bpage);

  *ret_bpage = bpage;
  return 0;
}





/***********************/
/* Free list functions */
/***********************/
BFpage *FL_Init(unsigned int size)
{
  unsigned int i;
  BFpage *head, *node;

  head = node = malloc(sizeof(BFpage));
  if (!head)
  {
    BFerrno = BFE_NOMEM;
  }

  for (i = 1; i < size; ++i)
  {
    node->nextpage = malloc(sizeof(BFpage));
    if (!node)
    {
      BFerrno = BFE_NOMEM;
    }

    node = node->nextpage;
  }

  node->nextpage = NULL;

  return head;
}

void FL_Clean(BFpage *node)
{
  BFpage *next;

  while (node) {
    next = node->nextpage;

    free(node->nextpage);
    free(node);

    node = next;
  }
}


BFpage *FL_Pop(BFpage **head)
{
  BFpage *page = *head;
  *(head) = page->nextpage;

  return page;
}


void FL_Push(BFpage **head, BFpage *page)
{
  page->nextpage = *head;
  *head = page;
}
