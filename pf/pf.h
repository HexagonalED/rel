//
// Created by HexagonalED on 2021-03-28.
//

#ifndef MINIREL_PF_H
#define MINIREL_PF_H


#define PF_FTAB_SIZE 1024
#define PAGE_SIZE 4096

#define PFE_OK 0
#define PFE_ERROR -1
#define TRUE 1
#define FALSE 0

typedef struct PFftab_ele {
    bool_t    valid;       /* set to TRUE when a file is open. */
    ino_t     inode;       /* inode number of the file         */
    char      *fname;      /* file name                        */
    int       unixfd;      /* Unix file descriptor             */
    PFhdr_str hdr;         /* file header                      */
    short     hdrchanged;  /* TRUE if file header has changed  */
} PFftab_ele;

typedef struct PFhdr_str {
    int    numpages;      /* number of pages in the file */
} PFhdr_str;


typedef struct PFpage {
    char pagebuf[PAGE_SIZE];
} PFpage;


PFftab_ele* pfTable;
int pfTable_index=0;

PFhdr_str* PF_initHeader(void){
    PFhdr_str* ret = (PFhdr_str*)malloc(sizeof(PFhdr_str));
    return ret;
}

void PF_Init(void);

int PF_CreateFile (const char *filename);  /*const char     *filename; /* name of file to be created */

int PF_DestroyFile (const char *filename); /*const char     *filename; /* name of file to be destroyed */

int PF_OpenFile (const char *filename); /*const char     *filename; /* name of the file to be opened */

int PF_CloseFile (int fd); /*int     fd;           /* PF file descriptor */

int PF_AllocPage (int fd, int *pageNum, char **pagebuf);
/*
int     fd;          /* PF file descriptor
int     *pageNum;    /* return the number of the page allocated
char    **pagebuf;   /* return a pointer to the page content */

int PF_GetNextPage (int fd, int *pageNum, char **pagebuf);
/*
int     fd;            /* PF file descriptor
int     *pageNum;      /* return the number of the next page
char    **pagebuf;     /* return a pointer to the page content */


int PF_GetFirstPage(int fd, int *pageNum, char **pagebuf);
/*
int     fd;            /* PF file descriptor
int     *pageNum;      /* return the number of the first page
char    **pagebuf;     /* return a pointer to the page content */

int PF_GetThisPage (int fd, int pageNum, char **pagebuf);
/*
int     fd;            /* PF file descriptor
int     pageNum;       /* number of page to retrieve
char    **pagebuf;     /* return the content of the page data */

int PF_DirtyPage(int fd, int pageNum);
/*
int     fd;            /* PF file descriptor
int     pageNum;       /* number of page to be marked dirty */

int PF_UnpinPage(int fd, int pageNum, int dirty);
/*
int     fd;            /* PF file descriptor
int     pageNum;       /* number of page to be unpinned
int     dirty;         /* dirty indication */

void PF_PrintError (const char *errString);
/*
const char    *errString;  /* pointer to an error message */

#endif //MINIREL_PF_H
