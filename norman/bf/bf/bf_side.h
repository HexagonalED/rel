#ifndef __BF_HEADER_H__
#define __BF_HEADER_H__


/********************/
/* Given structures */
/********************/
typedef struct BFpage
{
	int unixfd; 	 				/* PF file descriptor*/
	PFpage fpage;   				/*page data from a file*/
	struct BFpage *nextpage;		/*next in the linked list of buffer pages	*/
	struct BFpage *prevpage;		/* prev in the linked list of buffer pages*/
	bool_t dirty; 					/*TRUE if page is dirty*/
	short count; 					/* pin count associated with the page*/
	int pagenum;					/*page number of this page*/
	int fd; 						/* PF file descriptor of this page*/
}BFpage;


typedef struct BFhash_entry
{
  struct BFhash_entry *nextentry; /* next hash table element or NULL */
  struct BFhash_entry *preventry; /* prev hash table element or NULL */
  int fd;                         /* file descriptor                 */
  int pagenum;                    /* page number                     */
  struct BFpage *bpage;           /* ptr to buffer holding this page */
}BFhash_entry;


/***********************/
/* HashTable functions */
/***********************/
BFhash_entry **HT_Init(unsigned int size);

void HT_Clean(BFhash_entry **table, unsigned int size);

unsigned int HT_Index(int fd, int pagenum);

BFpage *HT_Find(BFhash_entry **table, int fd, int pagenum);

void HT_Add(BFhash_entry **table, BFpage *page);

void HT_Remove(BFhash_entry **table, int fd, int pagenum);



/***********************/
/* Free list functions */
/***********************/
BFpage *FL_Init(unsigned int size);

void FL_Clean(BFpage *node);

BFpage *FL_Pop(BFpage **head);

void FL_Push(BFpage **head, BFpage *page);



/*****************/
/* LRU functions */
/*****************/
void LRU_Push(BFpage **head, BFpage *new_node);

void LRU_Remove(BFpage **head, BFpage *page);

int LRU_ClearLast(BFpage *lru_head, BFhash_entry **hash_table, BFpage **ret_bpage);


#endif