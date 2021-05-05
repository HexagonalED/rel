#ifndef MINIREL_PF_H
#define MINIREL_PF_H


#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "../h/minirel.h"

#define TRUE 1
#define FALSE 0


#define PF_FTAB_SIZE 1024

#define PFE_OK 0
#define PFE_ERROR -1
#define PFE_FILEOPEN -2
#define PFE_WRITE -3
#define PFE_SYSCALL -4
#define PFE_NOFILE -5
#define PFE_FULLTABLE -6
#define PFE_FREE -7
#define PFE_PAGE -8
#define PFE_EOF -9
#define PF_BF -10

typedef struct PFhdr_str {
    int    numpages;      /* number of pages in the file */
} PFhdr_str;


typedef struct PFftab_ele {
    bool_t    valid;       /* set to TRUE when a file is open. */
    ino_t     inode;       /* inode number of the file         */
    char      *fname;      /* file name                        */
    int       unixfd;      /* Unix file descriptor             */
    PFhdr_str hdr;         /* file header                      */
    short     hdrchanged;  /* TRUE if file header has changed  */
} PFftab_ele;


PFftab_ele* pfTable;


void PF_Init(void);

int PF_CreateFile (char *filename);  /*const char     *filename; /* name of file to be created */

int PF_DestroyFile (char *filename); /*const char     *filename; /* name of file to be destroyed */

int PF_OpenFile (char *filename); /*const char     *filename; /* name of the file to be opened */

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

#endif 
