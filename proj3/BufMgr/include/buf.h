///////////////////////////////////////////////////////////////////////////////
/////////////  The Header File for the Buffer Manager /////////////////////////
///////////////////////////////////////////////////////////////////////////////


#ifndef BUF_H
#define BUF_H

#include "db.h"
#include "page.h"
#include "new_error.h"

/****************Include basic C packages***************************/
#include<math.h>
#include<stack>
#include<vector>
#include<deque>
#include<list>
#include<queue>
#include<algorithm>
/*******************End Package including***************************/
#define NUMBUF 20   
// Default number of frames, artifically small number for ease of debugging.
#define BuckSize 2
// Default size of the bucket
#define HTSIZE 7
// Hash Table size
#define INT_MAX 4294967200
// define largest integer for storage calcualtion

/****** Global Type ********************************/
typedef struct LL {int frameID; int PageId;} * List;
typedef list<LL> *HL;
/*****************************End Global Variables Declaration*****************/

/*****************************************************************/

/*******************ALL BELOW are purely local to buffer Manager********/

// You could add more enums for internal errors in the buffer manager.
enum bufErrCodes  {HASHMEMORY, HASHDUPLICATEINSERT, HASHREMOVEERROR, HASHNOTFOUND, QMEMORYERROR, QEMPTY, INTERNALERROR, 
            BUFFERFULL, BUFMGRMEMORYERROR, BUFFERPAGENOTFOUND, BUFFERPAGENOTPINNED, BUFFERPAGEPINNED};

class Replacer; // may not be necessary as described below in the constructor

class FrameDesc {

    friend class BufMgr;

    private:
<<<<<<< HEAD
        // variable to show if the page is clean to move on to the next stage
        bool is_clean;
        // The pin count for the page in this frame
        unsigned int num_pin;  
        // page No so we know if it is a valid page or invalid page
        int    pageNo;
=======
        int    pageNo;     // the page within file, or INVALID_PAGE if
                       // the frame is empty.

        unsigned int pin_cnt;  // The pin count for the page in this frame
        bool dirtybit;
>>>>>>> parent of 3bec71e... Finished move include and Global declaration


        FrameDesc() {
            pageNo  = INVALID_PAGE;
            num_pin = 0;
        }

        ~FrameDesc() {}

    public:
        int pin_count() { return(num_pin); }
        int pin() { return(++num_pin); }
        int unpin() {
            num_pin = (num_pin <= 0) ? 0 : num_pin - 1;
            return(num_pin);
        }
};
// HashTable Defnition 
class HashTable{

    friend BufMgr;
    private:
        int a,b;
        int Next,level;
        int partion_flag;
        int hashbuf;
        vector<HL> hash_table;
    public:
        HashTable(int size);
        void hash_build(PageId PageNo,int frameNo);
        void hash_remove(int page);
        int hash_search(int pageID,int &frameNo);
        void print_hash();
        void Hash_delte();
};


class BufMgr {

    private: 
        unsigned int    numBuffers;
        FrameDesc   *bufDescr;
        HashTable *hash;
    public:
        Page* bufPool; // The actual buffer pool
        BufMgr (int numbuf, Replacer *replacer = 0); 
        // Initializes a buffer manager managing "numbuf" buffers.
        // Disregard the "replacer" parameter for now. In the full 
        // implementation of minibase, it is a pointer to an object
        // representing one of several buffer pool replacement schemes.

        ~BufMgr();           // Flush all valid dirty pages to disk

        Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage);
        // Check if this page is in buffer pool, otherwise
        // find a frame for this page, read in and pin it.
        // also write out the old page if it's dirty before reading
        // if emptyPage==TRUE, then actually no read is done to bring
        // the page

        Status unpinPage(PageId globalPageId_in_a_DB, int dirty, int hate);
        // hate should be TRUE if the page is hated and FALSE otherwise
        // if pincount>0, decrement it and if it becomes zero,
        // put it in a group of replacement candidates.
        // if pincount=0 before this call, return error.

        Status newPage(PageId& firstPageId, Page*& firstpage, int howmany=1); 
        // call DB object to allocate a run of new pages and 
        // find a frame in the buffer pool for the first page
        // and pin it. If buffer is full, ask DB to deallocate 
        // all these pages and return error

        Status freePage(PageId globalPageId); 
        // User should call this method if it needs to delete a page
        // this routine will call DB to deallocate the page 

        Status flushPage(PageId pageid);
        // Used to flush a particular page of the buffer pool to disk
        // Should call the write_page method of the DB class

        Status flushAllPages();
        // Flush all pages of the buffer pool to disk, as per flushPage.

        /*** Methods for compatibility with project 1 ***/
        Status pinPage(PageId PageId_in_a_DB, Page*& page, int emptyPage, const char *filename);
        // Should be equivalent to the above pinPage()
        // Necessary for backward compatibility with project 1

        Status unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename);
        void init_frame(unsigned int  value) { numBuffers=value;};
        // Should be equivalent to the above unpinPage()
        // Necessary for backward compatibility with project 1
    
        unsigned int getNumBuffers() const { return numBuffers+1; }
        // Get number of buffers

        unsigned int getNumUnpinnedBuffers();
        // Get number of unpinned buffers
};

#endif
