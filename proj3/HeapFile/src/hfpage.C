#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
    // initiallize prev and next page with the constant recommended in the project description 
    prevPage = INVALID_PAGE;
    nextPage = INVALID_PAGE;
    // set current page to pageNo 
    curPage = pageNo;
    // usedPtr is the pointer to the first used byte in data
    // data grows from end to beginning
    usedPtr = 1000;//MAX_SPACE;
    // slotCnt is the the number of slots in use
    slotCnt = 1;
    // set the first slot to default values
    // length is -1 bc there are no values stored
    slot[0].length = EMPTY_SLOT;
    // set the offset to -1 to indicate that the slot is empty 
    slot[0].offset = INVALID_SLOT;
    
    // freeSpace on the data array is goint to be
    // MAX_SPACE - DP_FIXED
    // MAX_SPACE = 1024 -- default size of a page
    // DPFIXED = size of one slot + size of page_id + 4 bytes:
    // slotCnt = 1 byte
    // usedPtr = 1 byte
    // freeSpace = 1 byte
    // type = 1 byte -- an arbitrary value used by subclasses as needed
    freeSpace = MAX_SPACE - DPFIXED;
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
  return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
  prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
  return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
  nextPage = pageNo;
}


// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
  // check if there is a slot available
  int i;
  int slotId = -1;

  for(i=0; i < slotCnt; i++){
    if(slot[i].length == EMPTY_SLOT){
      slotId = i;
    }
  }

  // calculate the ammout of space necessary to include the record 
  int spaceRequired=recLen;
  // if there is no slot pre-alocated we that space is required
  if(slotId == -1) {
    spaceRequired = spaceRequired + sizeof(slot_t);
  }

  // check if there is enough space store the record
  if(spaceRequired > freeSpace){
    return DONE;
  }

  // calculate where to put the record
  // IMPORTANT: data grows from the end to the beggining of the file
  int offset = usedPtr - recLen;
  // copy recPtr to data[offSet]
  memcpy(data+offset, recPtr, recLen);
  // update the offset
  usedPtr = offset;
  // update the freespace
  freeSpace = freeSpace - recLen;
  
  //printf("**\n ammount of free space after inserting record %d\n**\n", freeSpace);
  // if an empty slot id was not found, add one;
  // this is safe because we already cheched we have space for this
  
  if(slotId == -1){
    // slotid = slotCnt (currentSlot +1) 
    slotId = slotCnt;
    // increment slot count and decrement space
    slotCnt = slotCnt + 1;
    freeSpace = freeSpace - sizeof(slot_t);
  }

  // fill rid and slot info
  // rid was passed by reference 
  rid.pageNo = curPage;
  rid.slotNo = (short) slotId;
  // slot info
  // using slotId here is safe bc in the worst case we already created
  // another slot previously
  slot[slotId].offset = (short) offset;
  slot[slotId].length = (short) recLen;
  
  // we always want to have a pre allocated slot if possible
  // first check if there is space available
  if((short)sizeof(slot_t) <= freeSpace){
    // slotCnt is equivalent to currentSlot + 1
    slot[slotCnt].offset = INVALID_SLOT;
    slot[slotCnt].length = EMPTY_SLOT;
    // increment the number of slots
    slotCnt = slotCnt + 1;
    // decrement freespace
    freeSpace = freeSpace - sizeof(slot_t);
  }
  
  return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
  /*
    testdata:
      slot0.offset 58
      slot0.len = 2
      slot1.offset = 56
      slot1.len = 2
      slot2.offset = 54
      slot2.len = 2
      
    goal:
      slot0.offset = -1
      slot0.len = -1
      slot1.offset = 58
      slot1.len = 2
      slot2.offset = 56
      slot2.len = 2 
      
      increase the amount of free space by slot0.length (before getting rid of the value)
      update usedPtr with the new value    
  */

  if ((rid.slotNo < 0) | (rid.slotNo >= slotCnt) | (rid.pageNo != curPage)) {
    return FAIL;
  } 
  int slotToBeDeleted = rid.slotNo;
  int i;
  // if the record being deleted corresponds to the last slot
  // compact the slot
  if(slotToBeDeleted == (slotCnt-2)) {
    //printf("**\n compacting slots \n ** \n");
    //## delete data
    // increment free space
    freeSpace = freeSpace + slot[slotToBeDeleted].length;
    // reposition usedPtr
    usedPtr = usedPtr + slot[slotToBeDeleted].length;
    // delete slot
    slot[slotToBeDeleted].length = (short) EMPTY_SLOT;
    slot[slotToBeDeleted].offset = (short) INVALID_SLOT;
    // compact slots
    i = slotToBeDeleted;
    // scan the slots from end to beginning and delete the ones marked as empty
    while(slot[i].length == EMPTY_SLOT){
      // there will be always one slot pre-alocated
      // free space
      freeSpace = freeSpace + sizeof(slot_t);
      // decrease number of slots
      slotCnt = slotCnt - 1;
      i = i - 1;
    }
    return OK;
  }

  // else
  // compact the data 

  // calculate how much space is going to be needed
  int lastSlotWithData;
  // find the last slot of data.
  // this for has to find lastSlotWith data
  // and lastSlotWith data will be greater than slotToBeDeleted bc when deleting the last slot
  // this function coalesce the records
  for(i=0; i < slotCnt; i++) {
    if(slot[i].length != EMPTY_SLOT){
      lastSlotWithData = i; 
    }
  }
  // find the length of the records that are going to be moved
  // it is important to know that the slots grow from the beggining of the page to the end
  // and the data grows from the end of the page to the beggining
  int lengthRecordsToMove = slot[slotToBeDeleted].offset - slot[lastSlotWithData].offset;
  // allocate new pointer with the size of the space
  char * tmpData = (char *) malloc(lengthRecordsToMove);
  // copy existing records to a tmp space
  memcpy(tmpData, data + slot[lastSlotWithData].offset, lengthRecordsToMove); 
  // copy the records from tmp space back to data to fill the hole left by the deleted record
  memcpy(data + slot[lastSlotWithData].offset + slot[slotToBeDeleted].length, tmpData, lengthRecordsToMove); 
  // decrement the amount of space freed
  freeSpace = freeSpace + slot[slotToBeDeleted].length; 
  // change the metadata of the copied records to reflect the new offset (offset - length of the deleted record)
  for(i=slotToBeDeleted+1; i < slotCnt; i++){
    if(slot[i].length != EMPTY_SLOT){
      slot[i].offset = (short) (slot[i].offset + slot[slotToBeDeleted].length);
    }
  }
  
  // set usedPtr correctly
  usedPtr = usedPtr + slot[slotToBeDeleted].length; 

  // change the metadata of the deleted record -- length = -1 and offset = -1
  // TL;DR set the deleted slot to empty
  slot[slotToBeDeleted].length = EMPTY_SLOT;
  slot[slotToBeDeleted].offset = INVALID_SLOT;
 
  return OK; 
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
  int i;
  for(i=0; i < slotCnt; i++){
    if(slot[i].length != EMPTY_SLOT){
      firstRid.slotNo = i;
      firstRid.pageNo = curPage;
      return OK;
    }
  }
  return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
  // sanity check
  if((curRid.slotNo < 0) | (curRid.pageNo != curPage) | empty()) {
    return FAIL;
  }
  
  int i, currentSlot = curRid.slotNo;
  // note that we start from the next record after the current one
  for(i=currentSlot + 1; i < slotCnt; i++){
    if(slot[i].length != EMPTY_SLOT){
      nextRid.slotNo = i;
      nextRid.pageNo = curPage;
      return OK;
    } 
  }

  return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
  // checks
  // if slotNo is out of bondaries
  // trying to get a record from another page
  if ((rid.slotNo < 0) | (rid.slotNo >= slotCnt) | (rid.pageNo != curPage)) {
      return FAIL;
  }
  
  // write reclen (passed by reference)j
  recLen = slot[rid.slotNo].length;
  // this is safe bc conditions were checked before
  // copy data[offset] -> recPtr of size recLen
  memcpy(recPtr, data + slot[rid.slotNo].offset, slot[rid.slotNo].length);
  return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
  if ((rid.slotNo < 0) | (rid.slotNo >= slotCnt) | (rid.pageNo != curPage)) {
    return FAIL;
  }
  recLen = slot[rid.slotNo].length;
  recPtr = data + slot[rid.slotNo].offset;
  return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
  return freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
  // for each slot in the directory 
  for (int i = 0; i < slotCnt; i++) {
    // if any non-empty slot is found, return false 
    if (slot[i].offset != -1) {
      return false;
    }
  }
  // no non-empty slot found
  return true;
}




