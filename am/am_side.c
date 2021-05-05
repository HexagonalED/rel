#include <math.h>
#include <string.h>

#include "am_side.h"
#include "pf.h"



bool_t compareInt(int a, int b, int op)
{
  switch (op)
  {
    case EQ_OP: return a == b ? TRUE : FALSE;
    case LT_OP: return a < b ? TRUE : FALSE;
    case GT_OP: return a > b ? TRUE : FALSE;
    case LE_OP: return a <= b ? TRUE : FALSE;
    case GE_OP: return a >= b ? TRUE : FALSE;
    case NE_OP: return a != b ? TRUE : FALSE;
    default: return FALSE;
  }
}

bool_t compareFloat(float a, float b, int op)
{
  switch (op)
  {
    case EQ_OP: return a == b ? TRUE : FALSE;
    case LT_OP: return a < b ? TRUE : FALSE;
    case GT_OP: return a > b ? TRUE : FALSE;
    case LE_OP: return a <= b ? TRUE : FALSE;
    case GE_OP: return a >= b ? TRUE : FALSE;
    case NE_OP: return a != b ? TRUE : FALSE;
    default: return FALSE;
  }
}


bool_t compareChars(char* a, char* b, int op, int len)
{
  switch (op)
  {
    case EQ_OP: return strncmp(a, b, len) == 0 ? TRUE : FALSE;
    case LT_OP: return strncmp(a, b, len) < 0 ? TRUE : FALSE;
    case GT_OP: return strncmp(a, b, len) > 0 ? TRUE : FALSE;
    case LE_OP: return strncmp(a, b, len) <= 0 ? TRUE : FALSE;
    case GE_OP: return strncmp(a, b, len) >= 0 ? TRUE : FALSE;
    case NE_OP: return strncmp(a, b, len) != 0 ? TRUE : FALSE;
    default: return FALSE;
  }
}



/* B tree*/
/* policy to go through the B tree using a particular value, if the value if len than a key in a node or a leaf then the previous pointer has to be taken, otherwise ( greater or equal) the value is check with the next keys in the node until it is less than one or until the last key (the last pointer of the node is taken then )*/
/* int AM_FindLeaf(int ifd,char* value, int* tab) algorithm: -check parameter: the size of tab is the height of the tree and the first element is the page num of root.
               - go through the Btree with the policy explain above, and stock the pagenum of every scanned node in the array tab , at the end the pagenum of the leaf where the value has to be insert is returned 
               - return error or HFE_OK, in case of duplicate key: the position of the first equal key in the leaf*/
int AM_FindLeaf(int idesc, char* value, int* tab)
{
    int error,  pagenum;
    bool_t is_leaf, pt_find;
    int key_pos;
    AMitab_ele* pt;
    int fanout;
    char* pagebuf;
    int i,j;
    /* all verifications of idesc and root number has already been done by the caller function */
    pt=AMitab+idesc;
    
    tab[0]=pt->header.racine_page;
   
    if(tab[0]<=1) return AME_WRONGROOT;
    
    for(i=0; i<(pt->header.height_tree);i++)
    {

        error=PF_GetThisPage(pt->fd, tab[i], &pagebuf);
        if(error!=PFE_OK) PF_ErrorHandler(error);
        
        /*check if this node is a leaf */
        memcpy((bool_t*) &is_leaf, (char*) pagebuf, sizeof(bool_t));
        
        if( is_leaf==FALSE)
        {
            fanout=pt->fanout;
            pt_find=FALSE;
            j=0;
            /* have to find the next child using comparison */
            /* fanout = n ==> n-1 key */
                while(pt_find==FALSE)
                { /*normally is sure to find a key, since even if value is greater than all key: the last pointer is taken*/
                    pagenum=AM_CheckPointer(j, pt->fanout,  value, pt->header.attrType, pt->header.attrLength, pagebuf);
                   /* printf("pagenum %d, header num pages %d \n ", pagenum, pt->header.num_pages);*/
                    if (pagenum>0 && pt->header.num_pages>=pagenum)
                    {
                    
                    tab[i+1]=pagenum;
                    pt_find=TRUE;
                    }
                    else j++;
                }
        } 
        else
        {
            fanout=pt->fanout;
            j=0;
            /* have to find the next child using comparison */
           
                while(1){ /*normally is sure to find a key, since even if value is greater than all key: the last pointer is taken*/                    
                    key_pos=AM_KeyPos(j, pt->fanout,  value, pt->header.attrType, pt->header.attrLength, pagebuf);
                    if (key_pos>=0)
                    {
                      error=PF_UnpinPage(pt->fd, tab[i], 0);
                      if(error!=PFE_OK) PF_ErrorHandler(error);
                      return key_pos;
                    }
                    else j++;
                }
        }
        error=PF_UnpinPage(pt->fd, tab[i], 0);
        if(error!=PFE_OK) PF_ErrorHandler(error);
     }      
}






/* For any type of node: given a value and the position of a couple ( pointer,key) in a node, 
 * return the pointer if it has to be taken or not by comparing key and value. Return 0 if the pointer is unknown.
 * the fanout is also given, in case of the couple contain the last key, then the last pointer of the node has to be taken
 * this function is created to avoid switch-case inside a loop, in the function 
 */

int AM_CheckPointer(int pos, int fanout, char* value, char attrType, int attrLength,char* pagebuf)
{
    /*use to get any type of node and leaf from a buffer page*/
    inode inod;
    fnode fnod;
    cnode cnod;
    char * key; /* used to get the key of the couple at the index of value "pos" in the array of couple*/
    /* use to get the page an return it */
    int ikey;
    int tmp_ivalue;
    float fkey;
    float tmp_fvalue;
    int pagenum;
    
    switch(attrType)
    {
        case 'i':
          /* fill the struct using offset and cast operations*/
          memcpy((int*)&inod.num_keys, (char*) (pagebuf+OffsetNodeNumKeys ), sizeof(int));
          memcpy((int*)&inod.last_pt, (char*) (pagebuf+OffsetNodeLastPointer), sizeof(int));
        
          memcpy((int*) &ikey,(char*) (pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)+sizeof(int)),  attrLength);
          memcpy((int*) &pagenum,(pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)),  sizeof(int));
          tmp_ivalue=*((int*)value);

          if( pos<(inod.num_keys-1))
          { 
            if( tmp_ivalue<ikey ) return pagenum;
            return 0;
          }
          else if( pos==(inod.num_keys-1))
          {
            if( tmp_ivalue<ikey) return pagenum;
            return inod.last_pt;
          }
          else
          {
            printf( "DEBUG: pb with fanout or position given \n");
            exit( -1);
          }
          break;


        case 'c':  /* fill the struct using offsets */
          memcpy((int*)&cnod.num_keys, (char*) (pagebuf+OffsetNodeNumKeys ), sizeof(int));
          memcpy((int*)&cnod.last_pt, (char*) (pagebuf+OffsetNodeLastPointer), sizeof(int));
              
          memcpy((int*) &pagenum,(pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)),  sizeof(int));
          memcpy((char*) &key,(char*) (pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)+sizeof(int)),  attrLength);
           
              
          if( pos<(cnod.num_keys-1))
          { 
            if( strncmp((char*) value, key,  attrLength) <0)
            {
              return pagenum;
            }
            return -1;
          }
          else if( pos==(cnod.num_keys-1)) 
          {
            if(strncmp((char*) value,(char*) key,  attrLength) >=0) return cnod.last_pt;
            return pagenum;
          }
          else 
          {
            printf( "DEBUG: Node pb with fanout or position given \n");
            return -1;
          }
          break;


        case 'f':  /* fill the struct using offset and cast operations */
          memcpy((int*)&fnod.num_keys, (char*) (pagebuf+sizeof(bool_t)), sizeof(int));
          memcpy((int*)&fnod.last_pt, (char*) (pagebuf+OffsetNodeLastPointer), sizeof(int));
         
              
          memcpy((int*) &pagenum,(pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)),  sizeof(int));
          memcpy((float*) &fkey,(char*) (pagebuf+OffsetNodeCouple+pos*(sizeof(int)+attrLength)+sizeof(int)),  attrLength);
          tmp_fvalue=*((float*)value);
          /*printf("\n node key %f, value %f , la tentative %d\n", fkey, tmp_fvalue,  inod.last_pt);*/
            
          if( pos<(fnod.num_keys-1))
          { /* fanout -1 is the number of couple (pointer, key)*/
            if( tmp_fvalue<fkey ) return pagenum;
            return -1;
          }
          else if(pos==(fnod.num_keys-1)) 
          {
            if(  tmp_fvalue>=fkey ) return fnod.last_pt;
            return pagenum;
          }
          else
          {
            printf( "DEBUG: pb with a too high position given \n");
            exit( -1);
          }
          break;


        default:
            return AME_ATTRTYPE;
    }

}

/* For any type of leaves: given a value and the position of a couple ( pointer,key) in a node, return the position of the couple inside the leaf . Return 0 if the pointer is unknown.
 * the fanout is also given, in case of the couple contain the last key, then the last pointer of the node has to be taken
 * this function is created to avoid switch-case inside a loop, in the function 
 */
int AM_KeyPos(int pos, int fanout, char* value, char attrType, int attrLength, char* pagebuf)
{
  /*use to get any type of node and leaf from a buffer page*/
  ileaf inod;
  fleaf fnod;
  cleaf cnod;
  
  char* key;
  int ikey;
  int tmp_ivalue;
  float fkey;
  float tmp_fvalue;
  int tmp_page;
  
  switch(attrType)
  {
    case 'i': 
        /* fill the struct using offset and cast operations */
        memcpy((int*)&inod.num_keys, (char*) (pagebuf+OffsetLeafNumKeys), sizeof(int));
        inod.couple=(icoupleLeaf *)(pagebuf+OffsetLeafCouple);
        memcpy((int*) &ikey,(char*) (pagebuf+OffsetLeafCouple+pos*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
        tmp_ivalue=*((int*)value);
       
        if( pos<(inod.num_keys-1))
        { /* fanout -1 is the number of couple (pointer, key)*/
          if( tmp_ivalue<=ikey ) return pos;
          return -1;
        }
        else if( pos==(inod.num_keys-1))
        {
          if( tmp_ivalue<=ikey) return pos;
          return pos+1;
        }
        else
        {
          printf( "DEBUG: pb with fanout or position given ooo \n");
          exit( -1);
        }
        break;


    case 'c':  /* fill the struct using offset and cast operations */
        memcpy((int*)&cnod.num_keys, (char*) (pagebuf+OffsetLeafNumKeys), sizeof(int));
        
        memcpy((char*) key,(char*) (pagebuf+OffsetLeafCouple+pos*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
        
        if( pos<(cnod.num_keys-1))
        { /* fanout -1 is the number of couple (pointer, key)*/
          if( strncmp((char*) value,(char*) key ,   attrLength) <=0) return pos;
          return -1;
        }
        else if( pos==(cnod.num_keys-1))
        {
          if(strncmp( (char*) value,(char*)  key,  attrLength) <=0) return pos;     
          return pos+1;
        }
        else
        {
          printf( "DEBUG: pb with fanout or position given \n");
          exit( -1);
        }
        break;


    case 'f':  /* fill the struct using offset and cast operations */
        memcpy((int*)&fnod.num_keys, (char*) (pagebuf+OffsetLeafNumKeys), sizeof(int));
        fnod.couple=(fcoupleLeaf*)(pagebuf+OffsetLeafCouple);
        memcpy((float*) &fkey,(char*) (pagebuf+OffsetLeafCouple+pos*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
        tmp_fvalue=*((float*)value);
      
        if( pos<(fnod.num_keys-1))
        { /* fanout -1 is the number of couple (pointer, key)*/
          if( tmp_fvalue<=fkey ) return pos;
          return -1;
        }
        else if(pos==(fnod.num_keys-1))
        {
          if( tmp_fvalue<=fkey ) return pos;
          return pos+1;
        }
        else
        {
          printf( "DEBUG: pb with fanout or position given \n");
          exit(-1);
        }
        break;


    default:
        return AME_ATTRTYPE;
  }
} 
   


/*
  return the position of the couple which will be the first one in the new node of a split, not always the middle because of duplicate keys  */
int AM_LeafSplitPos( char* pagebuf, int size,  int attrLength, char attrType)
{
  /*use to get any type of node and leaf from a buffer page*/
  ileaf inod;
  fleaf fnod;
  cleaf cnod;
  
  char* key;
  char* key_1;
  
  int ikey;
  int ikey_1;
  
  float fkey;
  float fkey_1;
  
  int tmp_page;
  
  /* use in a loop to find all duplicate keys */
  int i;
  int pos;
  pos=size/2;
  i=0;
  
  switch(attrType)
  {
    case 'i': 
        /* fill the struct using offset and cast operations */
        memcpy((int*) &ikey,(char*) (pagebuf+(pos)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);

        if(pos==size/2)
        { 
          for(i=1;i<=size/2;i++)
          {
            memcpy((int*) &ikey_1,(char*) (pagebuf+(pos-i)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);   
            printf("la key ileaf %d et key_1 %d , la pos de key_1 %d\n", ikey,ikey_1, (pos-i));
            if(  ikey > ikey_1) return pos-i+1;
            if( ikey < ikey_1)
            {
            /* the middle is less than one preceding key ==>problem with the tree, can happen with string of different size */                     printf( "DEBUG: the middle is less than one preceding key ==>problem with the tree, cannot happen with int \n");
            exit(-1);
            }
          }
          i=0;
          /* the loop has been passed ==> duplicate keys from the beginning to the middle*/
          /* now the split pos will be 0 (if node full of duplicated key) or a value >=(size/2)+1 since all the duplicate keys have to be in the same node and there is at least (size/2)+1 duplicate keys */
          for(i=1;i<=size/2;i++)
          {
            memcpy((int*) &ikey_1,(char*) (pagebuf+pos*(2*sizeof(int)+attrLength)-i*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);    
            if( ikey < ikey_1 ) return pos+i; /*minimum is pos+1 == size/2 +1*/
            if( ikey > ikey_1 )
            {
              /* the middle is greater than one following key ==>problem with the tree, can happen with string of different size */                     printf( "\n DEBUG: the middle is greater than one following key ==>problem with the tree, cannot happen with int, \n\n");
              exit(-1);
            }
          }
          return 0;
        }
        else
        {
          printf( "DEBUG: pos!=size/2 in AM_SplitPos \n");
          return -1;
        }
        break;


    case 'c':  /* fill the struct using offset and cast operations */
        memcpy((char*) key,(char*) (pagebuf+pos*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);

        if( pos==size/2)
        { 
          for(i=1;i<=size/2;i++)
          {
            memcpy((char*) key_1,(char*) (pagebuf+(pos-i)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
            if( strncmp((char*) key , key_1, attrLength)>0 )
            {
              return pos-(i)+1;
            }
            if( strncmp((char*) key , key_1, attrLength)<0 )
            {
            /* the middle is less than one preceding key ==>problem with the tree, can happen with string of different size */                     printf( "DEBUG: the middle is less than one preceding key ==>problem with the tree, can happen with string of different size \n");
            return -1; 
            }
          }
          i=0;
          /* the loop has been passed ==> duplicate keys from the beginning to the middle*/
          /* now the split pos will be 0 (if node full of duplicated key) or a value >=(size/2)+1 since all the duplicate keys have to be in the same node and there is at least (size/2)+1 duplicate keys */
          for(i=1;i<=size/2;i++)
          {
            memcpy((char*) key_1,(char*) (pagebuf+(pos-i)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
            if( strncmp((char*) key , key_1, attrLength)<0 ) return pos+i; /*minimum is pos+1 == size/2 +1*/
            if( strncmp((char*) key , key_1, attrLength)>0 )
            {
              /* the middle is greater than one following key ==>problem with the tree, can happen with string of different size */                     printf( "\n DEBUG: the middle is greater than one following key ==>problem with the tree, can happen with string of different size \n\n");
              return pos+i;
            }
          }
          return 0; 
        }
        else
        {
          printf( "DEBUG: pos!=size/2 in AM_SplitPos \n");
          return -1;
        }
        break;


    case 'f':  /* fill the struct using offset and cast operations */
        
        memcpy((float*) &fkey,(char*)(pagebuf+(pos)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);
        if( pos==size/2)
        { 
          for(i=1;i<=size/2;i++)
          {
            memcpy((float*) &fkey_1,(char*) (pagebuf+(pos-i)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);   
            
            if(  fkey > fkey_1) return pos-(i)+1;
            if( fkey < fkey_1)
            {
              /* the middle is less than one preceding key ==>problem with the tree, can happen with string of different size */                     printf( "DEBUG: the middle is less than one preceding key ==>problem with the tree, cannot happen with float the key %f ,preceding key %f pos %d\n\n", fkey, fkey_1, pos-i);
              exit(-1);
            }
          }
          i=0;
          /* the loop has been passed ==> duplicate keys from the beginning to the middle*/
          /* now the split pos will be 0 (if node full of duplicated key) or a value >=(size/2)+1 since all the duplicate keys have to be in the same node and there is at least (size/2)+1 duplicate keys */
          for(i=1;i<=size/2;i++)
          {
              memcpy((float*) &fkey_1,(char*) (pagebuf+(pos-i)*(2*sizeof(int)+attrLength)+2*sizeof(int)),  attrLength);   
            if( fkey < fkey_1 ) return pos+i; /*minimum is pos+1 == size/2 +1*/
            if( fkey > fkey_1 )
            {
              /* the middle is greater than one following key ==>problem with the tree, can happen with string of different size */                     printf( "\n DEBUG: the middle is greater than one following key ==>problem with the tree, cannot happen with float \n\n");
              exit(-1);
            }
          }
          return 0;
        }
        else
        {
          printf( "DEBUG: pos!=size/2 in AM_SplitPos \n");
          return -1;
        }
        break;


    default:
        return AME_ATTRTYPE;
  }
}