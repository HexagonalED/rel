//
// Created by HexagonalED on 2021-03-28.
//

#include "minirel.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "pf.h"
#include "bf.h"
#include "bf_side.h"

void PF_Init(void){
    //Init BF
    BF_Init();
    //initialize file table
    pfTable = (PFftab_ele*)malloc(sizeof(PFftab_ele)*PF_FTAB_SIZE);
    int i;
    for(i=0;i<PF_FTAB_SIZE;i++){
        pfTable[i].valid = FALSE;
        pfTable[i].inode = -1;
        pfTable[i].fname="";
        pfTable[i].unixfd = -1;
    }
    pfTable_index=0;

    return;
}

int PF_CreateFile (const char *filename){/*const char     *filename; /* name of file to be created */
    //create file ( open file) that should not exist
    if(access(filename, F_OK ) == 0 ) {
        // file existence error
        PF_PrintError("file exists");
        return PFE_ERROR;
    }else{
        int fd = open(filename, O_CREAT);
        //PF header initialized and written
        PFhdr_str* fh = PF_initHeader();
        int wsize = write(fd,fh,sizeof(PFhdr_str));
        if(wsize!=sizeof(PFhdr_str)){
            PF_PrintError("not written enough");
            return PFE_ERROR;
        }
        int cr = close(fd);
        if(cr==0) return PFE_OK;
    }

    return PFE_ERROR;
}

int PF_DestroyFile (const char *filename){/*const char     *filename; /* name of file to be destroyed */
    //destroys the file filename. The file should have existed, and should not be already open

    if(access(filename, F_OK ) == 0 ) {
        for(i = 0; i < PF_FTAB_SIZE; i++){
            if ((pfTable[i].fname != "") && (strcmp(pfTable[i].fname, filename) == 0) && (pfTable[i].valid == TRUE)){
                PF_PrintError("file is opened");
                return PFE_ERROR;
            }
        }
        if(remove(filename)==0){
            return PFE_OK;
        }
    }else {
        PF_PrintError("file doesn't exists");
        return PFE_ERROR;
    }
    return PFE_ERROR;
}

int PF_OpenFile (const char *filename){/*const char     *filename; /* name of the file to be opened */
    int i=0;
    //check if file is already opened
    for(i = 0; i < PF_FTAB_SIZE; i++){
        if ((pfTable[i].fname != "") && (strcmp(pfTable[i].fname, filename) == 0) && (pfTable[i].valid == TRUE)){
            PF_PrintError("file is opened");
            return PFE_ERROR;
        }
    }
    int fd = open(filename, O_RDONLY);
    if (fd >0) {
        i=0;
        for (i= 0; i< PF_FTAB_SIZE;i++){
            if (pfTable[i].valid == FALSE){
                if(read(file_fd, &pfTable[i].hdr, sizeof(PFhdr_str)) != sizeof(PFhdr_str)){
                    close(file_fd);
                    return PFE_ERROR; /* when read() failed and an error code was returned */
                }

                /* use system call stat() to retrieve UNIX file information */
                int stat_file = stat(filename, &stat_file)

                pfTable[i].valid = TRUE;
                pfTable[i].inode = stat_file.st_ino;
                pfTable[i].fname = (char *)malloc(sizeof(char)*strlen(filename));
                strcpy(&pfTable[i].fname,filename);
                pfTable[i].unixfd = fd;
                pfTable[i].hdrchanged = FALSE;
                return i;
            }
        }
    }else{
        PF_PrintError("file doesn't exist");
        return PFE_ERROR
    }
    close(fd);
    return PFE_ERROR;
}

int PF_CloseFile (int fd) {/*int     fd;           /* PF file descriptor */
    //This function closes the file associated with PF file descriptor fd
    PFftab_ele* target = &(pfTable[fd]);
    if(rm->valid == FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }
    //This entailes releasing all the buffer pages belonging to the file from the LRU list to the free list.
    if(BF_FlushBuf(fd)!=BFE_OK){
        PF_PrintError("pagefree error")
        return PFE_ERROR;
    }
    if(target->hdrchanged==TRUE){
        int ret = pwrite(target->unixfd,&(target->hdr),sizeof(PFhdr_str),0);
        if(ret!=sizeof(PFhdr_str)){
            PF_PrintError("not written enough");
            return PFE_ERROR;
        }
    }

    int cls = close(target->unixfd);
    if(csl!=0){
        PF_PrintError("close fail");
        return PFE_ERROR;
    }else{
        return PFE_OK;
    }
}

int PF_AllocPage (int fd, int *pageNum, char **pagebuf){
    /*int     fd;          /* PF file descriptor
    int     *pageNum;    /* return the number of the page allocated
    char    **pagebuf;   /* return a pointer to the page content */

    PFftab_ele* target = &(pfTable[fd]);
    if(target->valid==FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }

    BFreq* bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd = fd;
    bq->unixfd=target->unixfd;
    bq->pagenum=(target->hdr).numpages;
    bq->dirty=TRUE;

    PFpage* pg=NULL;
    int allocret = BF_AllocBuf(bq,&pg);
    if(allocret!=BFE_OK){
        PF_PrintError("Allocation error");
        return PFE_ERROR;
    }


    *pageNum = (target->hdr).numpages; /* copy the index of allocated page to *pagenum */
    *pagebuf = pg->pagebuf; /* assign the address of page content to given pointer */
    (target->hdr).numpages++;
    target->hdrchanged = TRUE;

    int dirtyret = PF_DirtyPage(fd,bq->pageNum)!=PFE_OK;
    if(dirtyret!=PFE_OK){
        PF_PrintError("dirty call error");
        return PFE_ERROR;
    }
    if(pageNum!=NULL && pagebuf!=NULL){
        return PFE_OK;
    }
    return PFE_ERROR;
}

int PF_GetNextPage (int fd, int *pageNum, char **pagebuf) {
    /*
    int     fd;            /* PF file descriptor
    int     *pageNum;      /* return the number of the next page
    char    **pagebuf;     /* return a pointer to the page content */


    PFftab_ele* target = pfTable[fd];

    //check if next page exists
    if(target->valid == FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }else if ((target->hdr).numpages == (*pageNum+1)) {
        PF_PrintError("end of file")
        return PFE_ERROR;
    }else if((target->hdr).numpages < (*pageNum+1) || (*pageNum+1)<0){
        PF_PrintError("page allocation error");
        return PFE_ERROR;
    }

    *pageNum = *pageNum+1;
    return PF_GetThisPage(fd,*pageNum,pagebuf);
}


int PF_GetFirstPage(int fd, int *pageNum, char **pagebuf){
    /*
    int     fd;            /* PF file descriptor
    int     *pageNum;      /* return the number of the first page
    char    **pagebuf;     /* return a pointer to the page content */
    *pageNum= -1;
    return PF_GetNextPage(fg,pageNum,pagebuf);
}

int PF_GetThisPage (int fd, int pageNum, char **pagebuf) {
    /*
    int     fd;            /* PF file descriptor
    int     pageNum;       /* number of page to retrieve
    char    **pagebuf;     /* return the content of the page data */
    PFftab_ele* target = pfTable[fd];

    //check if next page exists
    if(target->valid == FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }else if ((target->hdr).numpages == (*pageNum)) {
        PF_PrintError("end of file")
        return PFE_ERROR;
    }else if((target->hdr).numpages < (*pageNum) || (*pageNum)<0){
        PF_PrintError("page allocation error");
        return PFE_ERROR;
    }
    BFreq* bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fd = fd;
    bq->unixfd=target->unixfd;
    bq->pagenum=pageNum;

    PFpage* pg;
    if getret = BF_GetBuf(*bq,&pg);
    if(getret!=BFE_OK){
        PF_PrintError("BF GetBuf error");
        return PFE_ERROR;
    }else{
        *pagebuf=pg->pagebuf;
        return PFE_OK;
    }
}

int PF_DirtyPage(int fd, int pageNum) {
    /*
    int     fd;            /* PF file descriptor
    int     pageNum;       /* number of page to be marked dirty */

    PFftab_ele* target = pfTable[fd];
    if(target->valid == FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }else if ((target->hdr).numpages == (*pageNum)) {
        PF_PrintError("end of file")
        return PFE_ERROR;
    }else if((target->hdr).numpages < (*pageNum) || (*pageNum)<0) {
        PF_PrintError("page allocation error");
        return PFE_ERROR;
    }

    BFreq* bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fb=fd;
    bq->pagenum=pageNum;

    int touchret = BF_TouchBuf(*bq);
    if(touchret!=BFE_OK){
        PF_PrintError("touch error");
        return PFE_ERROR;
    }else{
        return PFE_OK;
    }
}

int PF_UnpinPage(int fd, int pageNum, int dirty) {
    /*
    int     fd;            /* PF file descriptor
    int     pageNum;       /* number of page to be unpinned
    int     dirty;         /* dirty indication */
    PFftab_ele* target = pfTable[fd];
    if(target->valid == FALSE){
        PF_PrintError("file not opened");
        return PFE_ERROR;
    }else if ((target->hdr).numpages == (*pageNum)) {
        PF_PrintError("end of file")
        return PFE_ERROR;
    }else if((target->hdr).numpages < (*pageNum) || (*pageNum)<0) {
        PF_PrintError("page allocation error");
        return PFE_ERROR;
    }

    BFreq* bq=(BFreq*)malloc(sizeof(BFreq));
    bq->fb=fd;
    bq->pagenum=pageNum;
    if(dirty){
        int touchret = BF_TouchBuf(*bq);
        if(touchret!=BFE_OK){
            PF_PrintError("touch error");
            return PFE_ERROR;
        }
    }
    int unpinret = BF_UnpinBuf(*bq);
    if(unpinret==BFE_OK){
        return PFE_OK;
    }else{
        PF_PrintError("unpin error");
        return PFE_ERROR;
    }
}

void PF_PrintError (const char *errString){
    //return error to stderr then writes the last error message produced by the PF layer onto stderr as well.
    printf(errString);
}
/*
const char    *errString;  /* pointer to an error message */