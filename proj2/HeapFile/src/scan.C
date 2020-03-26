/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan(HeapFile *hf, Status &status)
{
  status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
  // put your code here
  Status currentState;

}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID &rid, char *recPtr, int &recLen)
{
  Status currentState;
  Page *currentTempPage;
  userRid.pageNo = dataPageId;
  char *currentTempRecordPtr;
  

  if (userRid.slotNo == -1)
  {
    currentState = dataPage->firstRecord(userRid);
  }
  else
  {
    currentState = dataPage->nextRecord(userRid, userRid);
  }

  if (currentState == DONE)
  {
  
    currentState = nextDataPage();
    if (currentState == DONE)
    {
      currentState = nextDirPage();
      if (currentState == DONE)
      {
       
        currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, _hf->fileName);
        return DONE;
      }
      else
      {
        firstDataPage();
      }
    }
    userRid.pageNo = dataPageId;

    currentState = dataPage->firstRecord(userRid);
  }

  currentState = dataPage->returnRecord(userRid, currentTempRecordPtr, recLen);
  memcpy(recPtr, currentTempRecordPtr, recLen);
  rid = userRid;
  return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
 
Status currentState;

  Page *currentTempPage;
  
  _hf = hf;
  dirPageId = hf->firstDirPageId;
  currentState = MINIBASE_BM->pinPage(dirPageId, currentTempPage, 0, hf->fileName);
  dirPage = reinterpret_cast<HFPage *>(currentTempPage);
  // currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, hf->fileName);
  dataPageId = -1;
  dataPageRid.slotNo = -1;
  dataPageRid.pageNo = -1;
  
  dataPage = NULL;
  currentState = firstDataPage();
  // MUSA currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, hf->fileName);
  userRid.slotNo = -1;
  userRid.pageNo = -1;
 nxtUserStatus = -1;
  scanIsDone = -1;
  
  return OK;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
  return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
  // scan for the first page and pin it
  Status currentState;
  struct DataPageInfo *currentTempDPInfo;
  char *currentTempRecordPtr;
  Page *currentTempPage;
  int len;
  dataPageRid.pageNo = dirPageId;
  currentState = dirPage->firstRecord(dataPageRid);
 

  if (currentState == DONE)
  {
    currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, _hf->fileName);
    return DONE;
  }

 // printf("Here");
  currentState = dirPage->returnRecord(dataPageRid, currentTempRecordPtr, len);
  currentTempDPInfo = reinterpret_cast<struct DataPageInfo *>(currentTempRecordPtr);
 
  dataPageId = currentTempDPInfo->pageId;
  currentState = MINIBASE_BM->pinPage(dataPageId, currentTempPage, 0, _hf->fileName);
 
  dataPage = reinterpret_cast<HFPage *>(currentTempPage);
  currentState = MINIBASE_BM->unpinPage(dataPageId, FALSE, _hf->fileName);
  // currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, _hf->fileName);
  return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage()
{

  Status currentState;
  struct DataPageInfo *currentTempDPInfo;
  char *currentTempRecordPtr;
  Page *currentTempPage;
  int len;

  currentState = dirPage->nextRecord(dataPageRid, dataPageRid);
  if (currentState == DONE)
    return DONE;
  currentState = dirPage->returnRecord(dataPageRid, currentTempRecordPtr, len);
  currentTempDPInfo = reinterpret_cast<struct DataPageInfo *>(currentTempRecordPtr);
  
  dataPageId = currentTempDPInfo->pageId;
  currentState = MINIBASE_BM->pinPage(dataPageId, currentTempPage, 0, _hf->fileName);
  
  dataPage = reinterpret_cast<HFPage *>(currentTempPage);
  currentState = MINIBASE_BM->unpinPage(dataPageId, FALSE, _hf->fileName);
  
  return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage()
{
  Status currentState;
  PageId next_id;
  
  Page *currentTempPage;
  next_id = dirPage->getNextPage();
  

  if (next_id == -1)
    return DONE;
  currentState = MINIBASE_BM->unpinPage(dirPageId, FALSE, _hf->fileName);
  currentState = MINIBASE_BM->pinPage(next_id, currentTempPage, 0, _hf->fileName);
 

  dirPage = reinterpret_cast<HFPage *>(currentTempPage);
  dirPageId = next_id;
  return OK;
}