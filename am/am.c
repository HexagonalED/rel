#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>
#include "am_side.h"
#include "pf.h"
#include "../pf/pfUtils.h"
#include "hf.h"
#include "../hf/hfUtils.h"

int AMerrno;

AMitab_ele *AMitab;
AMscantab_ele *AMscantab;
size_t AMitab_length;
size_t AMscantab_length;



void AM_Init(void)
{
  AMitab = malloc(sizeof(AMitab_ele) * AM_ITAB_SIZE);
  AMscantab = malloc(sizeof(AMscantab_ele) * MAXSCANS);
  AMitab_length = 0;
  if (!AMitab) AMerrno = AME_NOMEM;
  if (!AMscantab) AMerrno = AME_NOMEM;
  HF_Init();
}



int AM_CreateIndex(char *fileName, int indexNo, char attrType, int attrLength, bool_t isUnique)
{
  int error, fd, pagenum;
  AMitab_ele* pt;
  char* headerbuf;
/**********************add verif on all attribute check HF_OpenScan */
  /* fill the array of the hf file table*/ 
  if (AMitab_length >= AM_ITAB_SIZE) return AME_FULLTABLE;
  pt = AMitab + AMitab_length;
  AMitab_length++;

  sprintf(pt->iname, "%s.%d", fileName, indexNo);

  error = PF_CreateFile(pt->iname);
  if(error != PFE_OK)
    PF_ErrorHandler(error);

  fd = PF_OpenFile(pt->iname);
  if(fd < 0)
    return AME_PF;

  pt->fd = fd;
  pt->valid = TRUE;
  pt->dirty = TRUE;
  pt->header.racine_page = -1;

  /* Since on a internal node, there  is 3 int(is_leaf, pagenum of parent, number of key*/ 
  pt->fanout = ( (PF_PAGE_SIZE ) - 2*sizeof(int) - sizeof(bool_t)) / (sizeof(int) + pt->header.attrLength) + 1;
  pt->fanout_leaf = ( (PF_PAGE_SIZE - 3*sizeof(int) -sizeof(bool_t) ) / (attrLength + sizeof(RECID)) ) + 1; /* number of recid into a leaf page */
  pt->header.indexNo = indexNo;
  pt->header.attrType = attrType;
  pt->header.attrLength = attrLength;

  pt->header.height_tree = 0;
  pt->header.nb_leaf = 0;
  pt->header.num_pages=2;


  error = PF_AllocPage(pt->fd, &pagenum, &headerbuf);
  if (error != PFE_OK)
    PF_ErrorHandler(error);

  if (pagenum != 1)
    return AME_PF;

  memcpy((char*) (headerbuf), (int*) &pt->header.indexNo, sizeof(int));
  memcpy((char*) (headerbuf + sizeof(int)), (int*) &pt->header.attrType, sizeof(char));
  memcpy((char*) (headerbuf + sizeof(int) + sizeof(char)), (int*) &pt->header.attrLength, sizeof(int));
  memcpy((char*) (headerbuf + 2*sizeof(int) + sizeof(char)), (int*) &pt->header.height_tree, sizeof(int));
  memcpy((char*) (headerbuf + 3*sizeof(int) + sizeof(char)), (int*) &pt->header.nb_leaf, sizeof(int));
  memcpy((char*) (headerbuf + 4*sizeof(int) + sizeof(char)), (int*) &pt->header.racine_page, sizeof(int));
  memcpy((char*) (headerbuf + 5*sizeof(int) + sizeof(char)), (int*) &pt->header.num_pages, sizeof(int));

  pt->valid = FALSE;
  
  error = PF_UnpinPage(pt->fd, pagenum, 1);
  if (error != PFE_OK) return PF_ErrorHandler(error);

  error = PF_CloseFile(pt->fd);
  if (error != PFE_OK) PF_ErrorHandler(error);


  AMitab_length--;
  printf("Index : %s created\n", pt->iname);
  return AME_OK;
}



int AM_DestroyIndex(char *fileName, int indexNo)
{
  int error;
  char* new_filename;

  new_filename = malloc(sizeof(fileName) + sizeof(int));
  sprintf(new_filename, "%s.%i", fileName, indexNo);
  error = PF_DestroyFile(new_filename);
  if (error != PFE_OK) PF_ErrorHandler(error);

  printf("Index : %s has been destroyed\n", new_filename);
  free(new_filename);
  return AME_OK;
}



int AM_OpenIndex(char *fileName, int indexNo){

  int error, pffd, fileDesc;
  AMitab_ele *pt;
  char *headerbuf;
  char* new_filename;
  
  /*Initialisation */
  new_filename = malloc(sizeof(fileName) + sizeof(int));
  
  
  /*parameters cheking */
  if( AMitab_length >= AM_ITAB_SIZE) return AME_FULLTABLE;

  sprintf(new_filename, "%s.%i", fileName, indexNo);

  pffd = PF_OpenFile(new_filename);
  if(pffd < 0) PF_ErrorHandler(pffd);

  /* read the header which are stored on the second page of the file (index = 1) */ 
  error = PF_GetThisPage(pffd, 1, &headerbuf);
  if(error != PFE_OK) PF_ErrorHandler(error);


  /* Fill the array of the AM index table */
  pt = AMitab + AMitab_length;
  
  pt->fd = pffd;
  pt->valid = TRUE;
  pt->dirty = FALSE;

  
  memcpy(pt->iname, new_filename, sizeof(new_filename));
  free(new_filename);
  memcpy( (int*) &pt->header.indexNo,(char*) (headerbuf), sizeof(int));
  memcpy((int*) &pt->header.attrType,(char*) (headerbuf + sizeof(int)),  sizeof(char));
  memcpy( (int*) &pt->header.attrLength, (char*) (headerbuf + sizeof(int) + sizeof(char)),sizeof(int));
  memcpy((int*) &pt->header.height_tree,(char*) (headerbuf + 2*sizeof(int) + sizeof(char)),  sizeof(int));
  memcpy( (int*) &pt->header.nb_leaf,(char*) (headerbuf + 3*sizeof(int) + sizeof(char)), sizeof(int));
  memcpy((int*) &pt->header.racine_page, (char*) (headerbuf + 4*sizeof(int) + sizeof(char)), sizeof(int));
  
  memcpy((int*) &pt->header.num_pages, (char*) (headerbuf + 5*sizeof(int) + sizeof(char)), sizeof(int));

  pt->fanout = ( (PF_PAGE_SIZE ) - 2*sizeof(int) - sizeof(bool_t)) / (sizeof(int) + pt->header.attrLength) + 1;
  pt->fanout_leaf = ( (PF_PAGE_SIZE - 3*sizeof(int) -sizeof(bool_t) ) / (pt->header.attrLength + sizeof(RECID)) ) + 1; /* number of recid into a leaf page */



  /*increment the size of the table*/
  AMitab_length ++;
  fileDesc = AMitab_length -1 ;
  /*unpin and touch the header page */
  error = PF_UnpinPage(pt->fd, 1, 1);
  if(error != PFE_OK) PF_ErrorHandler(error);
  return fileDesc;
}

int AM_CloseIndex(int fileDesc){

  AMitab_ele* pt;
  int error;
  char* headerbuf;
  int i;
  i=0;

  if (fileDesc < 0 || fileDesc >= AMitab_length) return AME_FD;
  pt=AMitab + fileDesc ;
  /*check is file is not already close */
  if (pt->valid!=TRUE) return AME_INDEXNOTOPEN;

  /* check the header */
  if (pt->dirty==TRUE)
  { /* header has to be written again */
    error=PF_GetThisPage( pt->fd, 1, &headerbuf);
    if (error!=PFE_OK) PF_ErrorHandler(error);


    /* write the header  */
    memcpy((char*) (headerbuf), (int*) &pt->header.indexNo, sizeof(int));
    memcpy((char*) (headerbuf + sizeof(int)), (int*) &pt->header.attrType, sizeof(char));
    memcpy((char*) (headerbuf + sizeof(int) + sizeof(char)), (int*) &pt->header.attrLength, sizeof(int));
    memcpy((char*) (headerbuf + 2*sizeof(int) + sizeof(char)), (int*) &pt->header.height_tree, sizeof(int));
    memcpy((char*) (headerbuf + 3*sizeof(int) + sizeof(char)), (int*) &pt->header.nb_leaf, sizeof(int));
    memcpy((char*) (headerbuf + 4*sizeof(int) + sizeof(char)), (int*) &pt->header.racine_page, sizeof(int));
    memcpy((char*) (headerbuf + 5*sizeof(int) + sizeof(char)), (int*) &pt->header.num_pages, sizeof(int));

    /* the page is dirty now ==> last arg of PF_UnpinPage ("dirty") set to one */
    error = PF_UnpinPage(pt->fd, 1, 1);
    if (error != PFE_OK) PF_ErrorHandler(error);
  }
  /* close the file using pf layer */
  error=PF_CloseFile(pt->fd);
  if(error!=PFE_OK)PF_ErrorHandler(error);

  /* check that there is no scan in progress involving this file*/
  /* by scanning the scan table */
  for (i=0;i<AMscantab_length;i++)
  {
    if((AMscantab+i)->AMfd==fileDesc) return AME_SCANOPEN;
  }
  
  /*deletion */
  /* a file can be deleted only if it is at then end of the table in order to have static file descriptor */
  /* In any case the boolean valid is set to false to precise that this file is closed */
  pt->valid==FALSE;
  if(fileDesc==(AMitab_length-1))
  { /* it is the last file of the table */ 
    AMitab_length--;
    if(AMitab_length >0)
    {
      AMitab_length--;
      while (AMitab_length>0 &&  ((AMitab+ AMitab_length-1)->valid==FALSE))
      {
        AMitab_length--; /* delete all the closed file, which are the end of the table */
      }
    }
  }
  return AME_OK;
}



int AM_InsertEntry(int fileDesc, char *value, RECID recId){
  int leafNum, nodeNum, root_pagenum, new_leaf_page;
  int splitIndex, pos;
  int couple_size, num_keys, last_pt;
  int first_leaf,last_leaf, next, previous;
  int error, offset, i;
  int left_node, right_node;
  int parent;

  bool_t is_leaf;
  char attrType;
  int* visitedNode; /* array of node visited : [root, level1, ,leaf ] */
  int len_visitedNode;

  char* pagebuf;
  char* tempbuffer;
  char* new_leaf_buf;
  char *new_node_buf;
  AMitab_ele* pt;
  
  int ivalue;
  float fvalue;
  char* cvalue;


  /******************ADD PARAMETER checking **********/
  if (fileDesc < 0 || fileDesc >= AMitab_length) return AME_FD;

  pt = AMitab + fileDesc;
  
  if (pt->valid != TRUE) return AME_INDEXNOTOPEN;


  /* CASE : NO ROOT */
  if (pt->header.racine_page == -1)
  {
    /* Create a tree */
    error = PF_AllocPage(pt->fd, &root_pagenum, &pagebuf);
    
    if (error != PFE_OK) PF_ErrorHandler(error);

    /* Write the node into the buffer: what kind of node */
    offset = 0;
    is_leaf = TRUE;
    num_keys = 1;
    memcpy((char*)(pagebuf + offset),(bool_t *) &is_leaf, sizeof(bool_t)); /* is leaf */
    offset += sizeof(bool_t);
    memcpy((char*)(pagebuf + offset),(int *) &num_keys, sizeof(int)); /* Number of key*/
    offset += sizeof(int);
    first_leaf = FIRST_LEAF;
    memcpy((char*)(pagebuf + offset ),(int *) &first_leaf, sizeof(int)); /* previous leaf pagenum */
    /* sizeof( recid) : space for LAST RECID */
    offset += sizeof(int);
    last_leaf = LAST_LEAF;
    memcpy((char*)(pagebuf + offset),(int *) &last_leaf, sizeof(int)); /* next leaf pagenum */
    offset += sizeof(int);
    memcpy((char*)(pagebuf + offset), (RECID*) &recId, sizeof(RECID));
    offset += sizeof(RECID);
    
    switch (pt->header.attrType)
    {
      case 'c':
        memcpy((char*)(pagebuf + offset), (char*) value, pt->header.attrLength);
        break;
      case 'i':
        memcpy((char*)(pagebuf + offset), (int*) value, pt->header.attrLength);
        break;
      case 'f':
        memcpy((char*)(pagebuf + offset), (float*) value, pt->header.attrLength);
        break;
      default:
        return AME_INVALIDATTRTYPE;
        break;

    }

    /* change the AMi_elem */
    pt->header.racine_page = 2;
    pt->header.height_tree = 1;
    pt->header.nb_leaf = 1;
    pt->header.num_pages++;
    pt->dirty = TRUE;
    
    /* Unpin page */
    error = PF_UnpinPage(pt->fd, root_pagenum, 1);
    if (error != PFE_OK) PF_ErrorHandler(error);
    return AME_OK;
  }

  /* FIND LEAF */
  visitedNode = malloc( pt->header.height_tree * sizeof(int));
  
  /* pos < 0 : error,  pos >= 0 : position where to insert on the page */
  pos = AM_FindLeaf(fileDesc, value, visitedNode);
  if(pos < 0) return AME_KEYNOTFOUND;

  len_visitedNode = pt->header.height_tree;
  leafNum = visitedNode[(pt->header.height_tree)-1];

  error = PF_GetThisPage(pt->fd, leafNum, &pagebuf);
  if (error != PFE_OK)
  {
    PF_ErrorHandler(error);
  }

  memcpy((int*) &num_keys, (char*)(pagebuf + sizeof(bool_t)), sizeof(int));

  couple_size = sizeof(RECID) + pt->header.attrLength;

  /* CASE : STILL SOME PLACE */
  if( num_keys < pt->fanout_leaf - 1)
  {
    /* Insert into leaf without splitting */
    offset = sizeof(bool_t) + 3 * sizeof(int) + couple_size*pos;
    /* ex : insert 3 
     * num_keys = 4
     * pos = 2 
     * pagebuf : header |(,1)|(,2)|(,4)|(,5)| | |  
     * src = headerbuf + sizeofheader + pos*sizeofcouple
     * dest = src + sizeofcouple
     * size = (num_keys - pos) * (sizeofcouple)
     with sizeof couple = sizeof recid + attrLength
     */
    memmove((char*) (pagebuf + offset + couple_size), (char*) (pagebuf + offset), couple_size*(num_keys - pos));
    /* Add the new couple at the right place */
    memcpy((char *) (pagebuf + offset), (RECID*) &recId, sizeof(RECID));
    offset += sizeof(RECID);
    switch (pt->header.attrType)
    {
      case 'c':
        memcpy((char*)(pagebuf + offset), (char*) value, pt->header.attrLength);
        break;
      case 'i':
        memcpy((char*)(pagebuf + offset), (int*) value, pt->header.attrLength);
        break;
      case 'f':
        memcpy((char*)(pagebuf + offset), (float*) value, pt->header.attrLength);
        break;
      default:
        return AME_INVALIDATTRTYPE;
        break;
    }
    num_keys++;
    memcpy((char*)(pagebuf + sizeof(bool_t)), (int*) &num_keys, sizeof(int));

    /* Unpin page */
    error = PF_UnpinPage(pt->fd, leafNum, 1);
    if (error != PFE_OK) PF_ErrorHandler(error);
    free(visitedNode);
    return AME_OK;
  }
  /* CASE : NO MORE PLACE */
  else
  {
    printf("No more space in the leaf\n");

    tempbuffer = malloc(couple_size*(num_keys + 1));
    offset = sizeof(bool_t) + 3 * sizeof(int);

    /* cpy in the buffer the part of the node before the key , then the couple , then the part after */
    memcpy((char*) (tempbuffer) , (char*) (pagebuf + offset), couple_size*pos);
    offset += couple_size * pos;
    /* copy the couple */
    memcpy((char *) (tempbuffer + couple_size * pos ), (RECID*) &recId, sizeof(RECID));
    switch (pt->header.attrType)
    {
      case 'c':
        cvalue = malloc(pt->header.attrLength);
        memcpy((char*)(tempbuffer + couple_size * pos + sizeof(RECID)), (char*) value, pt->header.attrLength);
        break;
      case 'i':
        memcpy((char*)(tempbuffer + couple_size * pos + sizeof(RECID)), (int*) value, pt->header.attrLength);
        break;
      case 'f':
        memcpy((char*)(tempbuffer + couple_size * pos + sizeof(RECID)), (float*) value, pt->header.attrLength);
        break;
      default:
        return AME_INVALIDATTRTYPE;
        break;
    }
    offset += couple_size;
    /* copy the right part of the leaf */
    memcpy((char*) (tempbuffer + couple_size*(pos+1)), (char*) (pagebuf + offset), couple_size*(num_keys - pos));
  
    /* Find the split index */
    splitIndex = AM_LeafSplitPos(tempbuffer, num_keys, pt->header.attrLength, pt->header.attrType);
    
    if(splitIndex==0) return AME_DUPLICATEKEY;
    /* Alloc a page for the new leaf*/
    error = PF_AllocPage(pt->fd, &new_leaf_page, &new_leaf_buf);
    if (error != PFE_OK) PF_ErrorHandler(error);

    /*update new_leaf*/
    is_leaf = 1;
    previous = leafNum;
    memcpy((int *) &next, (char*) (pagebuf + sizeof(bool_t) + 2*sizeof(int)), sizeof(int));
    num_keys = (pt->fanout_leaf - 1) - splitIndex;

    offset = 0;
    memcpy((char*)(new_leaf_buf + offset), (bool_t*)&is_leaf, sizeof(bool_t));
    offset = sizeof(bool_t);
    memcpy((char*)(new_leaf_buf + offset), (int*)&num_keys, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)(new_leaf_buf + offset), (int*)&previous, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)(new_leaf_buf + offset), (int*)&next, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)(new_leaf_buf + offset), (char*)(tempbuffer + couple_size*splitIndex), couple_size * num_keys);

    /* update old_leaf */
    num_keys = splitIndex;
    next = new_leaf_page;
    
    offset = sizeof(bool_t); /*is_leaf is unchanged */
    memcpy((char*)(pagebuf + offset), (int*)&num_keys, sizeof(int) );
    offset += 2*sizeof(int); /* previous page unchanged */
    memcpy((char *)(pagebuf + offset), (int*)&next, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)(pagebuf + offset), (char *)(tempbuffer), couple_size * splitIndex);
    
    /* get the value to insert into the upper internal node before unpinning */
    offset = sizeof(bool_t) + 3*sizeof(int) + sizeof(RECID);
    switch (pt->header.attrType)
    {
      case 'c':
        memcpy((char*) cvalue, (char*)(new_leaf_buf + offset), pt->header.attrLength);
        printf("Value to insert in upper node : %s\n", cvalue);
        break;
      case 'i':
        memcpy((int*) &ivalue, (char*)(new_leaf_buf + offset ), pt->header.attrLength);
        printf("Value to insert in upper node : %d\n", ivalue);
        break;
      case 'f':
        memcpy((float*) &fvalue, (char*)(new_leaf_buf + offset ), pt->header.attrLength);
        printf("Value to insert in upper node : %f\n", fvalue);
        break;
      default:
        return AME_INVALIDATTRTYPE;
        break;
    }

    free(tempbuffer);

    error = PF_UnpinPage(pt->fd, new_leaf_page, 1); 
    if (error != PFE_OK) PF_ErrorHandler(error);

    error = PF_UnpinPage(pt->fd, leafNum, 1); 
    if (error != PFE_OK) PF_ErrorHandler(error);

    printf("leaf %d splitted to leaf %d\n", leafNum, new_leaf_page);
    printf("pushing the key up\n\n");
    
    pt->header.nb_leaf++;
    pt->header.num_pages++;
    pt->dirty = TRUE;

    /* INSERT INTO PARENT : LOOP*/
    leafNum = visitedNode[(pt->header.height_tree)-1];
    parent = 0;
    for (i = len_visitedNode; i>0; i--)
    {
      printf("%d ", visitedNode[len_visitedNode - i]);
    }

    left_node = leafNum;
    right_node = new_leaf_page;

    for(i = len_visitedNode; i>0; i--)
    {
      parent = visitedNode[i - 1];
      printf("parent page : %d\n", parent);
      
      /* CASE : PARENT IS ROOT */
      if(parent == len_visitedNode)
      {
        printf("LOL\n");
      }

      if(parent == pt->header.racine_page)
      {
        printf("create internal node in the root\n");

        error = PF_AllocPage(pt->fd, &nodeNum, &pagebuf);
        if (error!=PFE_OK)
          PF_ErrorHandler(error);

        is_leaf = 0;
        num_keys = 1;
        last_pt = right_node;

        offset = 0;
        memcpy((char*)(pagebuf + offset), (int*)&is_leaf, sizeof(bool_t));
        offset += sizeof(bool_t);
        memcpy((char*)(pagebuf + offset), (int*)&num_keys, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)(pagebuf + offset), (int*)&last_pt, sizeof(int));
        offset += sizeof(int);

        memcpy((char*)(pagebuf + offset), (int*)&left_node, sizeof(int));
        offset += sizeof(int);

        /*insert key */
        switch (pt->header.attrType)
        {
          case 'c':
            memcpy((char*)(pagebuf + offset), (char*) cvalue, pt->header.attrLength);
            break;
          case 'i':
            memcpy((char*)(pagebuf + offset), (int*) &ivalue, pt->header.attrLength);
            break;
          case 'f':
            memcpy((char*)(pagebuf + offset), (float*) &fvalue, pt->header.attrLength);
            break;
          default:
            return AME_INVALIDATTRTYPE;
            break;
        }
        offset += pt->header.attrLength;


        pt->header.height_tree++;
        pt->header.num_pages++;
        pt->header.racine_page = nodeNum;
        pt->dirty = TRUE;

        error = PF_UnpinPage(pt->fd, nodeNum, 1);
        if (error!=PFE_OK) PF_ErrorHandler(error);

        printf("New root on page : %d\n", nodeNum);
        
        
        free(visitedNode);
        return AME_OK;
      }


      /* PARENT IS NOT ROOT */
      else
      {
        parent = visitedNode[i - 2];
        printf("Insert left node : %d, right node : %d  Into parent : %d\n", left_node, right_node, parent);
        
        /* get parent page ; if not full insert and return AME_OK */
        error = PF_GetThisPage(pt->fd, parent, &pagebuf);
        if (error != PFE_OK) PF_ErrorHandler(error);

        offset = sizeof(bool_t);
        memcpy((int*) &num_keys, (char*)(pagebuf+offset), sizeof(int));
        offset += sizeof(int);

        couple_size = sizeof(int) + pt->header.attrLength;

        pos = num_keys; /* NEED TO BE CHANGED :get the good position 'ask paul'*/
        
        /* temp buffer hold the node with the inserted value
         * if good size, copy yo pagebuf
         * else split in 2 nodes
         */
        tempbuffer = malloc(couple_size*(num_keys + 1) + sizeof(bool_t) + 2 * sizeof(int) );
        memcpy((char*) tempbuffer, (char*)(pagebuf), sizeof(bool_t) + 2 * sizeof(int) + couple_size*(num_keys + 1) );
        
        if (pos == num_keys)
        {
          /*INSERT THE KEY IN LAST POSITION*/
          /* create a couple with the value and the ex last pointer, insert it  */
          memcpy((int*)&last_pt, (char *) (tempbuffer + offset), sizeof(int));
          offset += sizeof(int) + pos*couple_size;

          /* insert right_node pagenum of couple */
          memcpy((char*)(tempbuffer + offset), (int *)&last_pt, sizeof(int));
          offset += sizeof(int);

          /*insert key */
          switch (pt->header.attrType)
          {
            case 'c':
              memcpy((char*)(tempbuffer + offset), (char*) cvalue, pt->header.attrLength);
              break;
            case 'i':
              memcpy((char*)(tempbuffer + offset), (int*) &ivalue, pt->header.attrLength);
              break;
            case 'f':
              memcpy((char*)(tempbuffer + offset), (float*) &fvalue, pt->header.attrLength);
              break;
            default:
              return AME_INVALIDATTRTYPE;
              break;
          }
        }
        else
        {
          /*INSERT THE KEY IN ANOTHER POSITION */
          offset = sizeof(bool_t) + 2 * sizeof(int) + sizeof(int) + couple_size*pos; /* the last sizeof int represent the first pointer that won't move*/
          /*
           * node : header+ p4 + |p1| key1 |p2| key 2|p3| key3
           insert key4, p5 in pos 0 => node : header+ p4 + |p1| key4|p5| key1 |p2| key 2|p3| key3
          */

          memmove((char*) (tempbuffer + offset + couple_size), (char*) (tempbuffer + offset), couple_size*(num_keys - pos)-sizeof(int));
          
          /*insert the couple starting with the key */
          switch (pt->header.attrType)
          {
            case 'c':
              memcpy((char*)(tempbuffer + offset), (char*) cvalue, pt->header.attrLength);
              break;
            case 'i':
              memcpy((char*)(tempbuffer + offset), (int*) &ivalue, pt->header.attrLength);
              break;
            case 'f':
              memcpy((char*)(tempbuffer + offset), (float*) &fvalue, pt->header.attrLength);
              break;
            default:
              return AME_INVALIDATTRTYPE;
              break;
          }
          offset += pt->header.attrLength;
        }

        offset = sizeof(bool_t) + sizeof(int);

        /* update last pt */
        memcpy((char*)(tempbuffer + offset), (int *)&right_node, sizeof(int));  
        

        if (num_keys < pt->fanout -1)
        {
          
          if (pos == num_keys )
          {
            
            memcpy((char*)(pagebuf), (char*) tempbuffer, sizeof(bool_t) + 2 * sizeof(int) + couple_size*(num_keys + 1) );
            free(tempbuffer);
          }

          /* insert the new pointer and the num key */
          else
          {
            
            memcpy((char*)(pagebuf), (char*) tempbuffer, sizeof(bool_t) + 2 * sizeof(int) + couple_size*(num_keys + 1) );
            free(tempbuffer);
          }

          /*then update the number of key*/
          num_keys++;
          memcpy((char*)(pagebuf + sizeof(bool_t)), (int*)&num_keys, sizeof(int));
          error = PF_UnpinPage(pt->fd, parent, 1);
          if (error !=PFE_OK)
            PF_ErrorHandler(error);

          free(visitedNode);
          return AME_OK;
        }

        else
        {
          splitIndex = num_keys / 2;

          /* Allocate a page for the new node */
          error = PF_AllocPage(pt->fd, &right_node, &new_node_buf);
          if (error != PFE_OK)
          {
            PF_ErrorHandler(error);
          }

          /* Update new node */
          is_leaf = 0;
          num_keys = (pt->fanout -1 ) - splitIndex;
          memcpy((int*) &last_pt, (char*)(tempbuffer + sizeof(bool_t) + sizeof(int)), sizeof(int));
          

          offset = 0;
          memcpy((char *) (new_node_buf + offset), (bool_t*) &is_leaf, sizeof(bool_t));
          offset += sizeof(bool_t);
          memcpy((char *) (new_node_buf + offset), (int*) &num_keys, sizeof(int));
          offset += sizeof(int);
          memcpy((char *) (new_node_buf + offset), (int*)&last_pt, sizeof(int));
          offset += sizeof(int);
          memcpy((char *) (new_node_buf + offset), (char *) (tempbuffer + offset + couple_size*splitIndex), couple_size * num_keys);

          
          /* Update old node */
          num_keys = splitIndex;
          memcpy((int*) &last_pt, (char*) (tempbuffer + offset + couple_size*(splitIndex - 1)), sizeof(int) );

          offset = sizeof(bool_t);
          memcpy((char *) (pagebuf + offset), (int*) &num_keys, sizeof(int));
          offset += sizeof(int);
          memcpy((char *) (pagebuf + offset), (int*) &last_pt, sizeof(int));
          offset += sizeof(int);
          memcpy((char *) (pagebuf + offset), (char *)(tempbuffer + offset), couple_size*splitIndex);

          /* get the value to push up for other nodes (aka te first value in the new node) */
          offset = sizeof(bool_t) + 2 * sizeof(int) + sizeof(int);
          switch (pt->header.attrType)
          {
            case 'c':
              memcpy((char*) cvalue, (char*)(new_node_buf + offset), pt->header.attrLength);
              printf("Value to insert in upper node : %s\n", cvalue);
              break;
            case 'i':
              memcpy((int*) &ivalue, (char*)(new_node_buf + offset), pt->header.attrLength);
              printf("Value to insert in upper node : %d\n", ivalue);
              break;
            case 'f':
              memcpy((float*) &fvalue, (char*)(new_node_buf + offset), pt->header.attrLength);
              printf("Value to insert in upper node : %f\n", fvalue);
              break;
            default:
              return AME_INVALIDATTRTYPE;
              break;
          }

          free(tempbuffer);

          error = PF_UnpinPage(pt->fd, right_node, 1);
          if (error!= PFE_OK)
            PF_ErrorHandler(error);

          error = PF_UnpinPage(pt->fd, parent, 1);
          if (error!= PFE_OK)
            PF_ErrorHandler(error);

          printf("Node %d splitted to node %d\n", parent, right_node);
          printf("pushing the key up\n\n");

          pt->header.num_pages++;
          pt->dirty = TRUE;

          left_node = parent;
        }
        return AME_NOINSERTION;
      }
    }
  }
  free(visitedNode);
  return AME_OK;
}



int AM_DeleteEntry(int fileDesc, char *value, RECID recId)
{
  int error, res, pos, num_keys, del_index, leafNum, couple_size, offset,i;
  AMitab_ele* pt;
  char* pagebuf;
  int* visitedNode;
  RECID get_recid;
  
  bool_t found;

  found=FALSE;
  /* NEED TO CHECK RECID */
  if (fileDesc < 0 || fileDesc >= AMitab_length) return AME_FD;

  pt = AMitab + fileDesc;
  
  if (pt->valid != TRUE) return AME_INDEXNOTOPEN;

  visitedNode = malloc( pt->header.height_tree * sizeof(int));
  pos = AM_FindLeaf(fileDesc, value, visitedNode);
  if (pos < 0) return AME_KEYNOTFOUND;

  leafNum = visitedNode[(pt->header.height_tree)-1];
  error = PF_GetThisPage(pt->fd, leafNum, &pagebuf);
  if (error != PFE_OK) PF_ErrorHandler(error);

  couple_size = sizeof(RECID) + pt->header.attrLength;

  /* starting from pos until num_keys, find the index , then memmove*/
  memcpy((int*) &num_keys, (char*) (pagebuf + sizeof(bool_t)), sizeof(int));
  offset = sizeof(bool_t) + 3 * sizeof(int)+pos*couple_size;
  for(del_index = pos; del_index < num_keys; del_index++)
  {
    memcpy((int*) &get_recid.pagenum, (char *) (pagebuf + offset), sizeof(int));
    memcpy((int*) &get_recid.recnum, (char *) (pagebuf + offset+sizeof(int)), sizeof(int));
    
    if(get_recid.pagenum == recId.pagenum && get_recid.recnum == recId.recnum)
    {
      found = TRUE;
      break;
    }

    offset += couple_size;
  }

  if (found)
  {
    /* not tested */
    res = AME_OK;
    
    memmove((char*)(pagebuf + offset), (char*)(pagebuf + offset + couple_size), couple_size * (num_keys - del_index));
    
    num_keys --;
    for (i=0;i<AMscantab_length;i++)
    {
      if((AMscantab+i)->AMfd==fileDesc) (AMscantab+i)->current_key--;
    }
  
    memcpy((char*) (pagebuf + sizeof(bool_t)), (int*)&num_keys, sizeof(int));
  }
  else
  {
    res = AME_RECNOTFOUND;
    exit(-1);
  }

  error = PF_UnpinPage(pt->fd, leafNum, 1);
  if (error != PFE_OK) PF_ErrorHandler(error);
  free(visitedNode);
  return res;
}


/*
 *    int     AM_fd,               file descriptor                 
 *    int     op,                  operator for comparison         
 *    char    *value               value for comparison (or null)  
 *
 * This function opens an index scan over the index represented by the file associated with AM_fd.
 * The value parameter will point to a (binary) value that the indexed attribute values are to be
 * compared with. The scan will return the record ids of those records whose indexed attribute 
 * value matches the value parameter in the desired way. If value is a null pointer, then a scan 
 * of the entire index is desired. The scan descriptor returned is an index into the index scan 
 * table (similar to the one used to implement file scans in the HF layer). If the index scan table
 * is full, an AM error code is returned in place of a scan descriptor.
 *
 * The op parameter can assume the following values (as defined in the minirel.h file provided).
 *
 *    #define EQ_OP           1
 *    #define LT_OP           2
 *    #define GT_OP           3
 *    #define LE_OP           4
 *    #define GE_OP           5
 *    #define NE_OP           6
 *
 * Dev: Patric
 */
int AM_OpenIndexScan(int AM_fd, int op, char *value)
{
  AMitab_ele* amitab_ele;
  AMscantab_ele* amscantab_ele;
  int key, error, val;
  int* tab;
  val = -2147483648; /* = INTEGER_MINVALUE, used if op is NE_OP, to just get leftmost element */

  if (AM_fd < 0 || AM_fd >= AMitab_length) return AME_FD;
  if (op < 1 || op > 6) return AME_INVALIDOP;
  if (AMscantab_length >= AM_ITAB_SIZE) return AME_SCANTABLEFULL;

  amitab_ele = AMitab + AM_fd;
  
  if (amitab_ele->valid != TRUE) return AME_INDEXNOTOPEN;

  amscantab_ele = AMscantab + AMscantab_length;
  

  /* copy the values */
  memcpy((char*) amscantab_ele->value, (char*) value, amitab_ele->header.attrLength);
  amscantab_ele->op = op;
  tab = malloc(amitab_ele->header.height_tree * sizeof(int));
  key = AM_FindLeaf(AM_fd, value, tab);
  if (op == NE_OP) key = AM_FindLeaf(AM_fd, (char*) &val, tab); /* use val if NE_OP, to get leftmost leaf */
   
  amscantab_ele->current_page = tab[(amitab_ele->header.height_tree)-1];
  amscantab_ele->current_key = key;
  amscantab_ele->current_num_keys = 0; /* this is set in findNextEntry */
  amscantab_ele->AMfd = AM_fd;
  amscantab_ele->valid = TRUE;

  free(tab);
  return AMscantab_length++;
}

     

/* 
 * int     scanDesc;           scan descriptor of an index
 *
 * This function returns the record id of the next record that satisfies the conditions specified for an
 * index scan associated with scanDesc. If there are no more records satisfying the scan predicate, then an
 * invalid RECID is returned and the global variable AMerrno is set to AME_EOF. Other types of errors are
 * returned in the same way.
 *
 * Dev: Patric
 */
RECID AM_FindNextEntry(int scanDesc) 
{
  /* Procedure:
    - check operator and go to according direction (left or right) by doing:
      - increment/decrement key_pos while key_pos > 0 resp. < num_keys
      - if first/last couple is reached, jump to next/prev page
      - check on every key if its a match, if yes return and save current pos
  */
  RECID recid;
  AMitab_ele* amitab_ele;
  AMscantab_ele* amscantab_ele;
  char* pagebuf;
  int error, current_key, current_page, direction, tmp_page, tmp_key, offset;
  float f1, f2;
  int i1, i2;
  char *c1, *c2;
  ileaf inod;
  fleaf fnod;
  cleaf cnod;
  bool_t found;

  found = FALSE;

  if (scanDesc < 0 ||  (scanDesc >= AMscantab_length && AMscantab_length !=0))
  {
    recid.pagenum = AME_INVALIDSCANDESC;
    recid.recnum = AME_INVALIDSCANDESC;
    AMerrno = AME_INVALIDSCANDESC;
    return recid;
  }

  amscantab_ele = AMscantab + scanDesc;
  amitab_ele = AMitab + amscantab_ele->AMfd;
  if (amitab_ele->valid != TRUE)
  {
    recid.pagenum = AME_INDEXNOTOPEN;
    recid.recnum = AME_INDEXNOTOPEN;
    AMerrno = AME_INDEXNOTOPEN;
    return recid;
  }
  if (amscantab_ele->valid == FALSE)
  {
    recid.pagenum = AME_SCANNOTOPEN;
    recid.recnum = AME_SCANNOTOPEN;
    AMerrno = AME_SCANNOTOPEN;
    return recid;
  }

  /* read the current node */
  error = PF_GetThisPage(amitab_ele->fd, amscantab_ele->current_page, &pagebuf);
  if(error != PFE_OK) PF_ErrorHandler(error);

  /* read num_keys and write it in scantab_ele */
  memcpy((int*) &(amscantab_ele->current_num_keys), (int*) (pagebuf+ OffsetLeafNumKeys), sizeof(int));

  /* iterate to right-to-left if less-operation */
  direction = (amscantab_ele->op == LT_OP || amscantab_ele->op == LE_OP) ? -1 : 1;
  
  /* while there is a next page (if there is none, it is set to LAST_PAGE resp. FIRST_PAGE = -1) */
  while ( (amscantab_ele->current_page>=2 && (amscantab_ele->current_key>=0)) && ((amscantab_ele->current_page<=amitab_ele->header.num_pages) && (amscantab_ele->current_key<amscantab_ele->current_num_keys)))
  {
    /* iterate through keys while there is a next key and we found no result */
    while (found == FALSE && amscantab_ele->current_key >= 0 && amscantab_ele->current_key < amscantab_ele->current_num_keys)
    {
      /* compare and return if match */
      switch (amitab_ele->header.attrType)
      {
      case 'i':
        /* get the the values to compare */
        i1=*((int*) amscantab_ele->value);
        memcpy((int*) &i2,(char*) (pagebuf+OffsetLeafCouple+amscantab_ele->current_key*(2*sizeof(int)+amitab_ele->header.attrLength)+2*sizeof(int)),  amitab_ele->header.attrLength);
        if (compareInt( i2, i1, amscantab_ele->op) == TRUE) found = TRUE;
        amscantab_ele->current_key += direction;
        break;
      case 'f':
        f1=*((float*) amscantab_ele->value);
        memcpy((float*) &f2,(char*) (pagebuf+OffsetLeafCouple+amscantab_ele->current_key*(2*sizeof(int)+amitab_ele->header.attrLength)+2*sizeof(int)),  amitab_ele->header.attrLength);     
        printf("compared values %f %f \n",f1,f2);
        if (compareFloat( f2, f1, amscantab_ele->op) == TRUE) found = TRUE;
        amscantab_ele->current_key += direction;
        break;
      case 'c':
        
        if (compareChars( (char*) (pagebuf+OffsetLeafCouple+amscantab_ele->current_key*(2*sizeof(int)+amitab_ele->header.attrLength)+2*sizeof(int)), amscantab_ele->value, amscantab_ele->op, amitab_ele->header.attrLength) == TRUE) found = TRUE;
        amscantab_ele->current_key += direction;
        break;
      }
    }
    if(found==FALSE)
    {
      if(amscantab_ele->op == EQ_OP )
      {
        error = PF_UnpinPage(amitab_ele->fd, amscantab_ele->current_page, 0);
        if(error != PFE_OK) PF_ErrorHandler(error);
         break;/*for eq-op scan the record which can are in the page given in openscan and no one else, because duplicate key are only in one leaf */
      }
      
      tmp_page = amscantab_ele->current_page;
      /* update amscantab_ele by reading next/prev page. the next/prev page will be -1 if its nonexistent. */
      if (direction < 0)
      {
        memcpy((int*) &(amscantab_ele->current_page), (int*) (pagebuf + sizeof(bool_t) + sizeof(int)), sizeof(int));
      }
      else
      {
        memcpy((int*) &(amscantab_ele->current_page), (int*)(pagebuf + sizeof(bool_t) + sizeof(int)*2), sizeof(int));
      }
      error = PF_UnpinPage(amitab_ele->fd, tmp_page, 0);
      if(error != PFE_OK) PF_ErrorHandler(error);
      
      error = PF_GetThisPage(amitab_ele->fd, amscantab_ele->current_page, &pagebuf);
      if(error != PFE_OK) PF_ErrorHandler(error);
    
      if (direction < 0)
      {
        /* read num_keys and write it in scantab_ele */
        memcpy((int*) &(amscantab_ele->current_key), (int*) pagebuf + sizeof(bool_t), sizeof(int));
      }
      else
      {
        /* if iterating right-to-left, first key on next page will just be 0 */
        amscantab_ele->current_key = 0;
      }
    }

    /* return here, so the update and cleanup above is done in every case */
    else
    {
      /*unpin the current page before returning */
      
      error = PF_UnpinPage(amitab_ele->fd, amscantab_ele->current_page, 0);
      if(error != PFE_OK) PF_ErrorHandler(error);
      memcpy((int*) &recid.pagenum,(char*) (pagebuf+OffsetLeafCouple+(amscantab_ele->current_key-direction)*(2*sizeof(int)+amitab_ele->header.attrLength)),  sizeof(int));
        memcpy((int*) &recid.recnum,(char*) (pagebuf+OffsetLeafCouple+(amscantab_ele->current_key-direction)*(2*sizeof(int)+amitab_ele->header.attrLength)+sizeof(int)),  sizeof(int));
        return recid;
      
    }
    
  }

  
  error = PF_UnpinPage(amitab_ele->fd, amscantab_ele->current_page, 0);
  if(error != PFE_OK) PF_ErrorHandler(error);
  recid.recnum = AME_EOF;
  recid.pagenum = AME_EOF;
  AMerrno=AME_EOF;
  return recid;
}

/*
 *   int     scanDesc;           scan descriptor of an index
 *
 * This function terminates an index scan and disposes of the scan state information. It returns AME_OK 
 * if the scan is successfully closed, and an AM error code otherwise.
 *
 * Dev: Patric
 */
int AM_CloseIndexScan(int scanDesc)
{
  AMscantab_ele* amscantab_ele;

  if (scanDesc < 0 ||  (scanDesc >= AMscantab_length && AMscantab_length !=0)) return AME_INVALIDSCANDESC;

  amscantab_ele = AMscantab + scanDesc;

  if (amscantab_ele->valid == FALSE) return AME_SCANNOTOPEN; 

  /* done similar to the closeScan in HF. is this procedure not needed? */
  if (scanDesc == AMscantab_length - 1)
  { /* if last scan in the table */
    if (AMscantab_length > 0) AMscantab_length--;
    while( (AMscantab_length > 0) && ((AMscantab + AMscantab_length - 1)->valid == FALSE))
    {
      AMscantab_length--; /* delete all following scan with valid == FALSE */
    }
  }
  return AME_OK;
}

void AM_PrintError(char *errString)
{
  int errcode;
  errcode = AMerrno;
  switch(errcode)
  {
    case AME_OK: fprintf(stderr, "\n no error \n\n");
    case AME_PF: fprintf(stderr, "\n problem with PF layer \n\n");
    case AME_EOF: fprintf(stderr, "\n problem when finding next entry \n\n");
    case AME_FULLTABLE: fprintf(stderr, "\n Encountered full table when creating index\n\n");
    case AME_INVALIDATTRTYPE: fprintf(stderr, "\n wrong attribute on entry \n\n");
    case AME_FD: fprintf(stderr, "\n problem with file description \n\n");
    case AME_INVALIDOP: fprintf(stderr, "\n invalid opening index \n\n");
    case AME_SCANTABLEFULL: fprintf(stderr, "\n encoutered full table when scanning index\n\n ");
    case AME_INVALIDSCANDESC: fprintf(stderr, "\n invalid scan description when looking for entry \n\n ");
    case AME_RECNOTFOUND: fprintf(stderr, "\n record not found for deletion \n");
    case AME_SCANOPEN: fprintf(stderr, "\n scan still opened while trying to close index \n\n ");
    case AME_NOMEM: fprintf(stderr, "\n no memory when initializing data structures\n\n ");
    case AME_KEYNOTFOUND: fprintf(stderr, "\n key not found in the tree \n\n ");
    case AME_DUPLICATEKEY: fprintf(stderr, "\n node full of duplicate key \n\n ");
    default: fprintf(stderr, "\n BF: unused error code : %d \n\n ", errcode);
  }
  fprintf(stderr, "Comment provided: %s\n", errString);
  exit(-1);
}