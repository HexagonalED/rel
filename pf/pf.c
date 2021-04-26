#include "minirel.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>

#include "pf.h"
#include "bf.h"

void PF_Init(void){
    int i;
    BF_Init();

    pfTable = (PFftab_ele*)malloc(sizeof(PFftab_ele)*PF_FTAB_SIZE);
    for(i=0;i<PF_FTAB_SIZE;i++){
        pfTable[i].valid = FALSE;
        pfTable[i].inode = -1;
        pfTable[i].fname="";
        pfTable[i].unixfd = -1;
        pfTable[i].hdr.numpages=-1;
        pfTable[i].hdrchanged=-1;
    }

    return;
}

int PF_CreateFile (const char *filename){/*const char     *filename; /* name of file to be created */
  int fd, wsize,cr;
  PFhdr_str fh;
  fd=open(filename,O_RDONLY);
  if(fd != -1){
    close(fd);
    PF_PrintError("PFE_FILEOEPN");
    return PFE_FILEOPEN;
  }
  fd = open(filename, O_WRONLY|O_CREAT);
  fh.numpages= 0;
  wsize = write(fd,&fh,sizeof(PFhdr_str));
  if(wsize!=sizeof(PFhdr_str)){
    PF_PrintError("PFE_WRITE");
    return PFE_WRITE;
  }
  cr = close(fd);
  
  if(cr==0) return PFE_OK;
  else{
    PF_PrintError("PFE_SYSCALL");
    return PFE_SYSCALL;
  }
}

int PF_DestroyFile (const char *filename){/*const char     *filename;  name of file to be destroyed */
  int i;
  int fd;
  fd=open(filename,O_RDONLY);
  if(fd==-1){
    PF_PrintError("PFE_NOFILE");
    return PFE_NOFILE;
  }
  close(fd);
  
  for(i=0;i<PF_FTAB_SIZE;i++){
    if ((pfTable[i].fname!="")&&(strcmp(pfTable[i].fname,filename)==0)&&(pfTable[i].valid == TRUE)){
      PF_PrintError("PFE_FILEOPEN");
      return PFE_FILEOPEN;
    }
  }  
  if(remove(filename)==0)
    return PFE_OK;
  else{
    PF_PrintError("PFE_SYSCALL");
    return PFE_SYSCALL;
  }
}

int PF_OpenFile (const char *filename){/*const char     *filename;  name of the file to be opened */
    int i=0;
    int fd,statret;
    struct stat stat_file;
    for(i = 0; i < PF_FTAB_SIZE; i++){
        if ((pfTable[i].fname != "") && (strcmp(pfTable[i].fname, filename) == 0) && (pfTable[i].valid == TRUE)){
            PF_PrintError("PFE_FILEOPEN");
            return PFE_FILEOPEN;
        }
    }
    fd = open(filename, O_RDWR);
    if(fd==-1){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }
    
    i=0;
    for (i= 0; i< PF_FTAB_SIZE;i++){
      if (pfTable[i].valid == FALSE){
        if(read(fd, &pfTable[i].hdr, sizeof(PFhdr_str)) != sizeof(PFhdr_str)){
          close(fd);
          PF_PrintError("PFE_SYSCALL");
          return PFE_SYSCALL;
        }
        
        /* use system call stat() to retrieve UNIX file information */
        statret = stat(filename, &stat_file);
        if(statret!=0){
          PF_PrintError("PFE_SYSCALL");
          return PFE_SYSCALL;
        }
        
        pfTable[i].valid = TRUE;
        pfTable[i].inode = stat_file.st_ino;
        pfTable[i].fname = (char *)malloc(sizeof(char)*strlen(filename));
        strcpy(pfTable[i].fname,filename);
        pfTable[i].unixfd = fd;
        pfTable[i].hdrchanged = FALSE;
        return i;
      }
    }
    close(fd);
    PF_PrintError("PFE_FULLTABLE");
    return PFE_FULLTABLE;
}

int PF_CloseFile (int fd) {/*int     fd;            PF file descriptor */
  int ret,cls;
    PFftab_ele* target = &(pfTable[fd]);
    if(target->valid == FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }
    if(BF_FlushBuf(fd)!=BFE_OK){
        PF_PrintError("PFE_FREE");
        return PFE_FREE;
    }
    if(target->hdrchanged==TRUE){
        ret = pwrite(target->unixfd,&(target->hdr),sizeof(PFhdr_str),0);
        if(ret!=sizeof(PFhdr_str)){
            PF_PrintError("PFE_WRITE");
            return PFE_WRITE;
        }
    }

    cls = close(target->unixfd);
    if(cls!=0){
        PF_PrintError("PFE_SYSCALL");
        return PFE_SYSCALL;
    }else{
        pfTable[fd].valid=FALSE;
        return PFE_OK;
    }
}

int PF_AllocPage (int fd, int *pageNum, char **pagebuf){
    /*int     fd;           PF file descriptor
    int     *pageNum;     return the number of the page allocated
    char    **pagebuf;    return a pointer to the page content */

  int allocret, dirtyret;
  BFreq* bq;

    PFpage* pg=NULL;
    PFftab_ele* target = &(pfTable[fd]);
    if(target->valid==FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }

    bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd = fd;
    bq->unixfd=target->unixfd;
    bq->pagenum=(target->hdr).numpages;
    bq->dirty=TRUE;

    allocret = BF_AllocBuf(*bq,&pg);
    if(allocret!=BFE_OK){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }

    printf("%d,%d",fd,bq->pagenum);

    *pageNum = (target->hdr).numpages; /* copy the index of allocated page to *pagenum */
    *pagebuf = pg->pagebuf; /* assign the address of page content to given pointer */
    (target->hdr).numpages++;
    target->hdrchanged = TRUE;
    

    dirtyret = PF_DirtyPage(fd,bq->pagenum);
    if(dirtyret!=PFE_OK){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }
    if(pageNum!=NULL && pagebuf!=NULL){
        return PFE_OK;
    }
    PF_PrintError("PFE_PAGE");
    return PFE_PAGE;
}

int PF_GetNextPage (int fd, int *pageNum, char **pagebuf) {
    /*
    int     fd;             PF file descriptor
    int     *pageNum;       return the number of the next page
    char    **pagebuf;      return a pointer to the page content */


    PFftab_ele* target = &(pfTable[fd]);

    if(target->valid == FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }else if ((target->hdr).numpages == (*pageNum+1)) {
        PF_PrintError("PFE_EOF");
        return PFE_EOF;
    }else if((target->hdr).numpages < (*pageNum+1) || (*pageNum+1)<0){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }

    *pageNum = *pageNum+1;
    return PF_GetThisPage(fd,*pageNum,pagebuf);
}


int PF_GetFirstPage(int fd, int *pageNum, char **pagebuf){
    /*
    int     fd;             PF file descriptor
    int     *pageNum;       return the number of the first page
    char    **pagebuf;      return a pointer to the page content */
    *pageNum= -1;
    return PF_GetNextPage(fd,pageNum,pagebuf);
}

int PF_GetThisPage (int fd, int pageNum, char **pagebuf) {
    /*
    int     fd;             PF file descriptor
    int     pageNum;        number of page to retrieve
    char    **pagebuf;      return the content of the page data */
    PFftab_ele* target = &(pfTable[fd]);
    BFreq* bq;
    PFpage* pg;
    int getret;

    if(target->valid == FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }else if ((target->hdr).numpages == (pageNum)) {
        PF_PrintError("PFE_EOF");
        return PFE_EOF;
    }else if((target->hdr).numpages < (pageNum) || (pageNum)<0){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }
    bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd = fd;
    bq->unixfd=target->unixfd;
    bq->pagenum=pageNum;

    getret = BF_GetBuf(*bq,&pg);
    if(getret!=BFE_OK){
      PF_PrintError("PF_BF");
      return PF_BF;
    }else{
        *pagebuf=pg->pagebuf;
        return PFE_OK;
    }
}

int PF_DirtyPage(int fd, int pageNum) {
    /*
    int     fd;             PF file descriptor
    int     pageNum;        number of page to be marked dirty */
  
  int touchret;
  BFreq* bq;
    PFftab_ele* target = &(pfTable[fd]);
    if(target->valid == FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }else if ((target->hdr).numpages == (pageNum)) {
        PF_PrintError("PFE_EOF");
        return PFE_EOF;
    }else if((target->hdr).numpages < (pageNum) || (pageNum)<0){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }

    bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd=fd;
    bq->pagenum=pageNum;

    touchret = BF_TouchBuf(*bq);
    if(touchret!=BFE_OK){
      PF_PrintError("PF_BF");
      return PF_BF;
    }else{
        return PFE_OK;
    }
}

int PF_UnpinPage(int fd, int pageNum, int dirty) {
    /*
    int     fd;             PF file descriptor
    int     pageNum;        number of page to be unpinned
    int     dirty;          dirty indication */

  int touchret,unpinret;
  BFreq* bq;

    PFftab_ele* target = &(pfTable[fd]);
    if(target->valid == FALSE){
        PF_PrintError("PFE_NOFILE");
        return PFE_NOFILE;
    }else if ((target->hdr).numpages == (pageNum)) {
        PF_PrintError("PFE_EOF");
        return PFE_EOF;
    }else if((target->hdr).numpages < (pageNum) || (pageNum)<0){
        PF_PrintError("PFE_PAGE");
        return PFE_PAGE;
    }

    bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd=fd;
    bq->pagenum=pageNum;

    if(dirty){
        touchret = BF_TouchBuf(*bq);
        if(touchret!=BFE_OK){
          PF_PrintError("PF_BF");
          return PF_BF;
        }
    }
    unpinret = BF_UnpinBuf(*bq);
    if(unpinret==BFE_OK){
        return PFE_OK;
    }else{
          PF_PrintError("PF_BF");
          return PF_BF;
    }
}

void PF_PrintError (const char *errString){
  if(strcmp(errString,"PFE_ERROR")==0){
    fprintf(stderr, "PFE_ERROR : Generic error occurred\n");
  }else if(strcmp(errString,"PFE_FILEOPEN")==0){
    fprintf(stderr, "PFE_FILEOPEN : File already exists or opened\n");
  }else if(strcmp(errString,"PFE_WRITE")==0){
    fprintf(stderr, "PFE_WRITE : Nont written enough or write error occurred\n");
  }else if(strcmp(errString,"PFE_SYSCALL")==0){
    fprintf(stderr, "PFE_SYSCALL : Syscall returned error\n");
  }else if(strcmp(errString,"PFE_NOFILE")==0){
    fprintf(stderr, "PFE_NOFILE : There is no such file or file failed to open\n");
  }else if(strcmp(errString,"PFE_FULLTABLE")==0){
    fprintf(stderr, "PFE_FULLTABLE : Filetable is full\n");
  }else if(strcmp(errString,"PFE_FREE")==0){
    fprintf(stderr, "PFE_FREE : Page alreade freed\n");
  }else if(strcmp(errString,"PFE_PAGE")==0){
    fprintf(stderr, "PFE_PAGE : Invalid page\n");
  }else if(strcmp(errString,"PFE_EOF")==0){
    fprintf(stderr, "PFE_EOF : Reached EOF\n");
  }else if(strcmp(errString,"PF_BF")==0){
    fprintf(stderr, "PF_BF : BF layer function returned error\n");
  }else{
    fprintf(stderr,"Undefined Error : %s\n",errString);
  }
}
