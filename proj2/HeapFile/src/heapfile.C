#include "heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer",
    "end of file encountered",
    "invalid update operation",
    "no space on page for record",
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

#define TrueValue 1
#define MY_SIZE 1024

static error_string_table hfTable(HEAPFILE, hfErrMsgs);

// ********************************************************
// Constructor
HeapFile::HeapFile(const char *name, Status &returnStatus) {
    // check if the HeapFile is already constructed
    if (MINIBASE_DB->get_file_entry(name, firstDirPageId) == OK) {
        // simply return nothing here and exit the function
        return;
    } else {
        // new page variable declaration
        Page *new_page;
        MINIBASE_BM->newPage(firstDirPageId, new_page, TrueValue);
        MINIBASE_DB->add_file_entry(name, firstDirPageId);
        MINIBASE_DB->get_file_entry(name, firstDirPageId);

        HFPage hf;
        // before memcpy we need to init the HF page with our
        // first Dir Page
        hf.init(firstDirPageId);
        // write it to memory
        memcpy(&(*new_page), &hf, MY_SIZE);
        // unpin the page so we know that we are done with it for now
        MINIBASE_BM->unpinPage(firstDirPageId, TRUE, fileName);
    }
}

// ******************
// Destructor
HeapFile::~HeapFile() {
    // fill in the body
    // Flush all the Pages in Minipages and delte Filename
    MINIBASE_BM->flushAllPages();
    if (fileName == NULL)
        deleteFile();
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    struct DataPageInfo *data_page_info = new struct DataPageInfo;
    
    // initialized local variables
    HFPage hf;
    Page *temp_page;   
    char *temp_ptr;
    int num_recs, temp_rec_len;        
    PageId curr_page, next_page; 
    struct RID curr_rid; 
    curr_page = firstDirPageId;
    num_recs = 0;
    


    while (1) {
        // pin the page so that we can use it for getting Record
        MINIBASE_BM->pinPage(curr_page, temp_page, 0, fileName);
        curr_rid.pageNo = curr_page;
        memcpy(&hf, &(*temp_page), MY_SIZE);
        
        // check if there is no record, then move to next Dir Page
        Status curr_state = hf.firstRecord(curr_rid);
        while (curr_state != DONE)
        {
            hf.returnRecord(curr_rid, temp_ptr, temp_rec_len);
            // if there are records to be returned, update data page info
            data_page_info = reinterpret_cast<struct DataPageInfo *>(temp_ptr);
            num_recs = num_recs + data_page_info->recct;
            curr_state = hf.nextRecord(curr_rid, curr_rid);
        }
        
        next_page = hf.getNextPage();
        if (next_page == -1) {
            // unpin here so it will not fail test case 5
            MINIBASE_BM->unpinPage(curr_page, FALSE, fileName);
            //number of recrod ret
            return num_recs;
        }
        // after getting the record, unpin the page so it can be free
        MINIBASE_BM->unpinPage(curr_page, FALSE, fileName);
        curr_page = next_page;
    }
    return -1;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID &outRid)
{
    // Status curr_state = OK;
    // created empty data for insertion
    // will change this into real data at the end of insertion
    struct DataPageInfo empty_data = {0, 0, 0};
    struct DataPageInfo *data_info; 
    data_info = &empty_data;
    Page *temp_page;
    PageId allocDirPageId, myCurrPageId, myNextPageId;
    int temp_len;
    HFPage myHFpage;
    struct RID curr_id, allocDataPageRid;
    char *temp_rec_ptr;
    myCurrPageId = firstDirPageId;
    if (recLen > (MY_SIZE-24))
    {
        MINIBASE_FIRST_ERROR(HEAPFILE, *hfErrMsgs[4]);
        MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
        return HEAPFILE;
    }

    while (1) {
        // pin the page here
        MINIBASE_BM->pinPage(myCurrPageId, temp_page, 0, fileName);
        memcpy(&myHFpage, &(*temp_page), MY_SIZE);
        curr_id.pageNo = myCurrPageId;
        // find first record
        Status curr_state = myHFpage.firstRecord(curr_id);
        // if find nothing then move to inserting
        while (curr_state != DONE && data_info->availspace < recLen) {
            myHFpage.returnRecord(curr_id, temp_rec_ptr, temp_len);
            data_info = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);
            curr_state = myHFpage.nextRecord(curr_id, curr_id);
        }
        // set the rest after intersting
        if (data_info->availspace > recLen) {
            // unpin this so it wont fail test case 5
            MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
            break;
        }
        if (curr_state == DONE && myHFpage.available_space() > sizeof(struct DataPageInfo)) {
            newDataPage(data_info);
            allocateDirSpace(data_info, allocated_dir_id, allocDataPageRid);
            MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
            break;
        }
        
        myNextPageId = myHFpage.getNextPage();
        


        if (myNextPageId == -1) {
            newDataPage(data_info);
            allocateDirSpace(data_info, allocated_dir_id, allocDataPageRid);
            MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
            break;
        }
        MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
        myCurrPageId = myNextPageId;
    }
    

    Page *newInsertedPage;
    if (MINIBASE_BM->pinPage(data_info->pageId, newInsertedPage, 0, fileName) == OK) {
        HFPage newInsertedHFPage;
        memcpy(&newInsertedHFPage, &(*newInsertedPage), MY_SIZE);
        newInsertedHFPage.insertRecord(recPtr, recLen, outRid);
        data_info->recct = data_info->recct + 1;
        data_info->availspace = newInsertedHFPage.available_space();
        memcpy(&(*newInsertedPage), &newInsertedHFPage, MY_SIZE);
        MINIBASE_BM->unpinPage(data_info->pageId, TRUE, fileName);
    }
    // allocate free space for this Data Page Directory
    allocateDirSpace(data_info, allocated_dir_id, allocDataPageRid);
    
    return OK;
}

// ***********************
// delete record from file
Status HeapFile::deleteRecord(const RID &rid) {
    
    // initialize useful variables
    PageId DirPageId, DataPageId;
    int myTempLength;
    HFPage *DirPage, *DataPage;
    char *temp_rec_ptr;
    struct RID dirPage_rid;
    
    // find record to be deleted.
    findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
    // find the record and now we delete.
    DataPage->deleteRecord(rid);
    // return the reccord page for checking
    DirPage->returnRecord(dirPage_rid, temp_rec_ptr, myTempLength);
    struct DataPageInfo *currentDPInfo = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);
    currentDPInfo->recct = currentDPInfo->recct - 1;
    // update avalilabe space of curretn Data page
    currentDPInfo->availspace = DataPage->available_space();
    
    memcpy(temp_rec_ptr, currentDPInfo, sizeof(struct DataPageInfo));
    // dont' forget to unpin the page or else test 5 will not pass
    MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
    MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);

    return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord(const RID &rid, char *recPtr, int recLen) {
    PageId DirPageId, DataPageId;
    HFPage *DirPage, *DataPage;

    struct RID dirPage_rid;

    char *temp_rec_ptr;
    int myTempLength;
    
   
    findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
   
    DataPage->returnRecord(rid, temp_rec_ptr, myTempLength);
    if (myTempLength != recLen) {
        MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
        MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);
        MINIBASE_FIRST_ERROR(HEAPFILE, *hfErrMsgs[3]);

        return HEAPFILE;
    }

    // load back to recpoitner to datapage
    memcpy(temp_rec_ptr, recPtr, recLen);
    DirPage->returnRecord(dirPage_rid, temp_rec_ptr, myTempLength);
    struct DataPageInfo *currentDPInfo = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);

    currentDPInfo->availspace = DataPage->available_space();
    //write changes to the Data Page
    memcpy(temp_rec_ptr, currentDPInfo, sizeof(struct DataPageInfo));
    
    MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
    MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);

    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord(const RID &rid, char *recPtr, int &recLen)
{
    // fill in the body
    PageId DirPageId, DataPageId;

    char *temp_rec_ptr;
    struct RID dirPage_rid;

    int myTempLength;
    
    HFPage *DirPage, *DataPage;
    
    findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
    //update the record in data page
    DataPage->returnRecord(rid, temp_rec_ptr, recLen);
    memcpy(recPtr, temp_rec_ptr, recLen);
    //unpin the pages we get records from so we will pass test 5
    MINIBASE_BM->unpinPage(DataPageId, FALSE, fileName);
    MINIBASE_BM->unpinPage(DirPageId, FALSE, fileName);

    return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status &status)
{
    // fill in the body
    Scan *t_scan = new Scan(this, status);
    return t_scan;
}

// ****************************************************
// Wipes out the heapfile from the database permanently.
Status HeapFile::deleteFile()
{
    // // fill in the body
    // Status curr_state = OK;
    struct DataPageInfo *myDPInfo = new struct DataPageInfo;
    Page *temp_page, *temp_page;        
    int numberOfRecords, temp_len;

    PageId myCurrPageId, myNextPageId; 
     char *temp_rec_ptr;
    HFPage myHFpage;

    struct RID curr_id; 
   
    
    myCurrPageId = firstDirPageId;
    numberOfRecords = 0;
    MINIBASE_BM->flushAllPages();
    while (1)
    {
        MINIBASE_BM->pinPage(myCurrPageId, temp_page, 0, fileName);
        memcpy(&myHFpage, &(*temp_page), MY_SIZE);
        curr_id.pageNo = myCurrPageId;
        Status curr_state = myHFpage.firstRecord(curr_id);
        while (curr_state != DONE)
        {
            curr_state = myHFpage.returnRecord(curr_id, temp_rec_ptr, temp_len);
            myDPInfo = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);
            curr_state = MINIBASE_BM->freePage(myDPInfo->pageId);
            curr_state = myHFpage.nextRecord(curr_id, curr_id);
        }
        

        myNextPageId = myHFpage.getNextPage();
        
        if (myNextPageId == -1) 
        {
            MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
            break;
        }

        MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
        myCurrPageId = myNextPageId;
    }
    myCurrPageId = firstDirPageId;
    while (1)
    {
        MINIBASE_BM->pinPage(myCurrPageId, temp_page, 0, fileName);
        memcpy(&myHFpage, &(*temp_page), 1024);
        myNextPageId = myHFpage.getNextPage();
        MINIBASE_BM->freePage(myCurrPageId);
        if (myNextPageId == -1) {
            break;
        }
        myCurrPageId = myNextPageId;
    }   
    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    Page *newpage;
    int t_pageid;
    
    if (MINIBASE_BM->newPage(t_pageid, newpage, 1) == OK)
    {
        HFPage myHFpage;
        myHFpage.init(t_pageid);
        dpinfop->availspace = myHFpage.available_space();
        
        dpinfop->recct = 0;
        dpinfop->pageId = t_pageid;

        memcpy(&(*newpage), &myHFpage, MY_SIZE);
        MINIBASE_BM->unpinPage(t_pageid, TRUE, fileName);

        
        return OK;
    }
    return OK;
}
// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID &rid,
                              PageId &rpDirPageId, HFPage *&rpdirpage,
                              PageId &rpDataPageId, HFPage *&rpdatapage,
                              RID &rpDataPageRid) {
    Page *temp_page, *t1_page;
    struct RID t_first, temp_data;
    HFPage myHFpage, t_hfp;
    PageId myCurrPageId = firstDirPageId;
    
    PageId myNextPageId, data_pageid;
    
    rpDirPageId = firstDirPageId;
    
    char *temp_rec_ptr;
    struct DataPageInfo *currentDPInfo;
    int temp_len;
    
    while (1) {
        if (MINIBASE_BM->pinPage(rpDirPageId, temp_page, 0, fileName) == OK) {
            memcpy(&myHFpage, &(*temp_page), MY_SIZE);
            rpDataPageRid.pageNo = rpDirPageId;
            Status curr_state = myHFpage.firstRecord(rpDataPageRid);
            if (curr_state != OK) {
                MINIBASE_BM->unpinPage(rpDirPageId, FALSE, fileName);
                return DONE;
            } else {
                while (curr_state == OK) {
                    curr_state = myHFpage.returnRecord(rpDataPageRid, temp_rec_ptr, temp_len);
                    currentDPInfo = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);
                    curr_state = MINIBASE_BM->pinPage(currentDPInfo->pageId, t1_page, 0, fileName);
                    memcpy(&t_hfp, &(*t1_page), 1024);
                    temp_data.pageNo = currentDPInfo->pageId;
                    curr_state = t_hfp.firstRecord(temp_data);
                    while (curr_state == OK) {
                        if (temp_data.pageNo == rid.pageNo) {
                            if (temp_data.slotNo == rid.slotNo) {
                                
                                rpDataPageId = currentDPInfo->pageId;
                                rpdatapage = reinterpret_cast<HFPage *>(t1_page);
                                rpdirpage = reinterpret_cast<HFPage *>(temp_page);
                                return OK;
                            }
                        }
                        curr_state = t_hfp.nextRecord(temp_data, temp_data);
                    }
                    
                    MINIBASE_BM->unpinPage(currentDPInfo->pageId, FALSE, fileName);
                    
                    curr_state = myHFpage.nextRecord(rpDataPageRid, rpDataPageRid);
                }
            }
        }
        else {
            return DONE;
        }
        myNextPageId = myHFpage.getNextPage();
        if (myNextPageId == -1) {
            rpDirPageId = 0;
            rpDataPageId = 0;
            rpdatapage = NULL;
            rpdirpage = NULL;
            MINIBASE_BM->unpinPage(rpDirPageId, FALSE, fileName);
            return DONE;
        }

        MINIBASE_BM->unpinPage(rpDirPageId, FALSE, fileName);
        rpDirPageId = myNextPageId;
    }
    return DONE;
}

// *********************************************************************
// Allocate directory space for a heap file page

Status HeapFile::allocateDirSpace(struct DataPageInfo *dpinfop,
                                  PageId &allocDirPageId,
                                  RID &allocDataPageRid) {
    Page *temp_page, *t1_page;
    HFPage myHFpage;
    struct RID t_first, curr_id;
    PageId myNextPageId;
    char *d, *temp_rec_ptr;
    int i, temp_len;

    PageId myCurrPageId = firstDirPageId;
    
     
    
    struct DataPageInfo *currentDPInfo;
    while (1) 
    {
        MINIBASE_BM->pinPage(myCurrPageId, temp_page, 0, fileName);
        memcpy(&myHFpage, &(*temp_page), 1024);
        curr_id.pageNo = myCurrPageId;
        Status curr_state = myHFpage.firstRecord(curr_id);
        while (curr_state == OK)
        {
            curr_state = myHFpage.returnRecord(curr_id, temp_rec_ptr, temp_len);
            currentDPInfo = reinterpret_cast<struct DataPageInfo *>(temp_rec_ptr);
            if (currentDPInfo->pageId == dpinfop->pageId)
            {
              allocDataPageRid = curr_id;

                allocDirPageId = myCurrPageId;
                
                currentDPInfo->recct = dpinfop->recct;

                currentDPInfo->availspace = dpinfop->availspace;
                
                memcpy(temp_rec_ptr, currentDPInfo, sizeof(struct DataPageInfo));
                memcpy(&(*temp_page), &myHFpage, MY_SIZE);
                curr_state = MINIBASE_BM->unpinPage(myCurrPageId, TRUE, fileName);
                return OK;
            }
            curr_state = myHFpage.nextRecord(curr_id, curr_id);
        }
        myNextPageId = myHFpage.getNextPage();
        if (myNextPageId == -1) //the page doesn't exist hence exit
        {
            curr_state = MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
            break;
        }
        curr_state = MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
        myCurrPageId = myNextPageId;
    }

    myCurrPageId = firstDirPageId;
    while (1)
    {
        if (MINIBASE_BM->pinPage(myCurrPageId, temp_page, 0, fileName) == OK)
        {
            memcpy(&myHFpage, &(*temp_page), 1024);
            if (myHFpage.available_space() > sizeof(*dpinfop))
            {
                char *recPtr = reinterpret_cast<char *>(dpinfop);
                myHFpage.insertRecord(recPtr, sizeof(struct DataPageInfo), t_first);
               
                myHFpage.returnRecord(t_first, d, i);
                struct DataPageInfo *temp = reinterpret_cast<struct DataPageInfo *>(d);
               
                myHFpage.firstRecord(allocDataPageRid);
                memcpy(&(*temp_page), &(myHFpage), MY_SIZE);
                MINIBASE_BM->unpinPage(myCurrPageId, TRUE, fileName);
                return OK;
            }
            else
            {
                myNextPageId = myHFpage.getNextPage();
                if (myNextPageId == -1) 
                {
                    HFPage t_hfp;
                    MINIBASE_BM->newPage(myNextPageId, t1_page, 1);
                    t_hfp.init(myNextPageId);
                    memcpy(&(*t1_page), &t_hfp, MY_SIZE);
                    MINIBASE_BM->unpinPage(myNextPageId, TRUE, fileName);
                    myHFpage.setNextPage(myNextPageId);
                }
                memcpy(&(*temp_page), &(myHFpage), MY_SIZE);
                MINIBASE_BM->unpinPage(myCurrPageId, FALSE, fileName);
                myCurrPageId = myNextPageId;
            }
        }
    }
}

// *******************************************