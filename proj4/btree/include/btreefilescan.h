/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */
 
#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"
// include other header file here to declare global variable
#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"


// errors from this class should be defined in btfile.h

class BTreeFileScan : public IndexFileScan {
public:
    friend class BTreeFile;

    // get the next record
    Status get_next(RID & rid, void* keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    bool is_initialized;

    RID head_ptr, nxt_ptr; // set the pointer to track of the rid in the scan.

    BTLeafPage *leaf_page = new BTLeafPage();
    // declaration of helper functions
    int get_next_not_initial(RID &rid, void* keyptr, Page* currPage, char* recChar, int recordSize);
    int get_next_helper(RID &rid, void* keyptr, char* recChar, int recordSize);

    // destructor
    ~BTreeFileScan();
private:
    // following added by Hoang april 10, needed to modify later
	PageId begin;
    RID  R_Start;
    PageId end;
    RID  Curr_rid;
    RID  R_End;
    AttrType keytype;
};

#endif
