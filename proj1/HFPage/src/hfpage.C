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
    prevPage = -1;
    nextPage = -1;
    // set current page to pageNo 
    curPage = pageNo;
    // usedPtr is the pointer to the first used byte in data
    // data grows from end to beginning
    usedPtr = 1000;
    // slotCnt is the the number of slots in use
    slotCnt = 0;
    // set the first slot to default values
    // length is 0 bc there are no values stored
    slot[1].length = 0;
    // set the offset to -1 to indicate that the slot is empty 
    slot[1].offset = 1002;
    
    // freeSpace on the data array is goint to be
    // MAX_SPACE - DP_FIXED
    // MAX_SPACE = 1024 -- default size of a page
    // DPFIXED = size of one slot + size of page_id + 4 bytes:
    // slotCnt = 1 byte
    // usedPtr = 1 byte
    // freeSpace = 1 byte
    // type = 1 byte -- an arbitrary value used by subclasses as needed
    freeSpace = DPFIXED + sizeof(slot_t) * (1 - slotCnt);
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

    for (i = 0; i < slotCnt; i++)
    {
        cout << "slot[" << i << "].offset=" << slot[i].offset
             << ", slot[" << i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    // fill in the body
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    // fill in the body
    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // fill in the body
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    // fill in the body
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    // fill in the body
    int space_available = available_space();
    bool slot_found = false;
    if (space_available >= (recLen))
    {
        //place the data into the usedPtr
        //data[usedPtr] = *recPtr;
        //data[usedPtr-recLen]=*recPtr;
        memcpy(&data[usedPtr - recLen], recPtr, recLen * sizeof(char));

        //place the page No in the rid
        rid.pageNo = curPage;
        //Check if any slot has empty offset indicating there is no record
        for (int i = 0; i < slotCnt; i++)
        {
            //find an empty slot with -1 offset
            if (slot[i].offset == -1)
            {
                //empty slot found... place the offset into this position that is usedPtrt -(bytes taken from usedPtr by the slots)
                slot[i].offset = usedPtr - (sizeof(slot_t)) * i;
                slot[i].length = recLen;
                //slot founc
                slot_found = true;
                //insert the slot No in the rid
                rid.slotNo = i;
            }
        }
        if (slot_found == false)
        {
            //we need a new slot
            //enter the new slot number into the rid
            rid.slotNo = slotCnt;
            slot[slotCnt].offset = usedPtr - sizeof(slot_t) * slotCnt;
            slot[slotCnt].length = recLen;
            slotCnt++;
        }

        //decrease the usedptr pointer value
        usedPtr = usedPtr - recLen;
        return OK;
    }
    else
        return DONE;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    // fill in the body
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
  if(slotToBeDeleted == (slotCnt-1)) {
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
      if(i<1){
        break;
      }
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
  slot[slotToBeDeleted].length = (short) EMPTY_SLOT;
  slot[slotToBeDeleted].offset = (short) INVALID_SLOT;
 
  return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    // fill in the body
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
    // fill in the body
    // sanity check
    if((curRid.slotNo < 0) | (curRid.pageNo != curPage) | empty() ) {
          return FAIL;
    }

    int i;
    // note that we start from the next record after the current one
    for(i=curRid.slotNo + 1; i < slotCnt; i++){
        if(slot[i].length != EMPTY_SLOT){
            nextRid.slotNo = (short) i;
            nextRid.pageNo = (short) curPage;
            return OK;
        } 
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // fill in the body
    /* checks
       if slotNo is out of bondaries
       trying to get a record from another page
    */
    if ((rid.slotNo < 0) | (rid.slotNo >= slotCnt) | (rid.pageNo != curPage)) {
        return FAIL;
    } 
    // write reclen (passed by reference)j
    recLen = slot[rid.slotNo].length;
    // this is safe bc conditions were checked before
    // copy data[offset] -> recPtr of size recLen
    memcpy(recPtr, data + slot[rid.slotNo].offset, recLen);
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    // fill in the body
    // it should FAIL if the slotNo of the current rid is bigger than the slotCnt - something not mismatched
    // if slotNo of that rid is not a valid one 
    // if the PageNo is not at the currPage then we cannot retrieive them at all
    if ((rid.slotNo > slotCnt) | (rid.slotNo < 0) | (rid.pageNo != curPage)) {
        return FAIL;
    }
    recLen = slot[rid.slotNo].length;
    // else just simply return the refernce to the update data slot for returning the Recods
    recPtr = &data[slot[rid.slotNo].offset + sizeof(slot_t) * rid.slotNo - recLen];
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    // free space is equal to what we have left minus the less than one slotCnt with the size of each slot
    freeSpace = usedPtr - (slotCnt - 1) * (sizeof(slot_t));
    for (int i = 0; i < slotCnt; i++) {
        if (slot[i].offset == -1)
        {
            return freeSpace;
        }
    }
    return freeSpace - sizeof(slot_t);
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    int i = 0;
    bool result = true;
    while (i <= slotCnt - 1) {
        if (slot[i].offset != -1) {
            result = false;
        }
        i++;
    }
    return result;
}



