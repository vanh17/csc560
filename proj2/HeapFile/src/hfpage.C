#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"

// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo) {
    // set the first slot to default values
    // length is 0 bc there are no values stored
    slot[1].length = 0;
    // set the offset to -1 to indicate that the slot is empty 
    slot[1].offset = 1002;
    // set current page to pageNo 
    curPage = pageNo;
    // usedPtr is the pointer to the first used byte in data
    // data grows from end to beginning
    usedPtr = 1000;
    // initiallize prev and next page with the constant recommended in the project description 
    prevPage = -1;
    nextPage = -1;
    // slotCnt is the the number of slots in use
    slotCnt = 0;
    
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
void HFPage::dumpPage() {
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
Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid)
{
    // fill in the body
    // initiall set no slot to free, if later we can find a free slot this
    // set to true
    bool available_slot = false;
    // check if there is enough space store the record
    if(recLen > available_space()){
        return DONE;
    }
        /// copy recPtr to data[offSet]
        memcpy(&data[usedPtr - recLen], recPtr, recLen * sizeof(char));

        // rid was passed by reference
        rid.pageNo = curPage;
        int i = 0;
        while (i <= slotCnt - 1){
            //find an empty slot with -1 offset
            if (slot[i].offset == -1)
            {
                // calculate where to put the record
                // Remark: we have to know that it populates starting from the end
                slot[i].length = recLen;
                slot[i].offset = usedPtr - (sizeof(slot_t)) * i;
                
                //insert the slot No in the rid
                rid.slotNo = i;
                //there is place to store what we need
                available_slot = true;
            }
            i++;
        }
        if (available_slot == false)
        {
            // fill rid and slot info
            // rid was passed by reference 
            rid.slotNo = slotCnt;
            // slotCnt is equivalent to currentSlot + 1
            // slot info
            // using slotId here is safe bc in the worst case we already created
            // another slot previously
            slot[slotCnt].offset = usedPtr - sizeof(slot_t) * slotCnt;
            slot[slotCnt].length = recLen;
            // increment the number of slots
            slotCnt++;
        }
        usedPtr = usedPtr - recLen;
        return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID &rid)
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
        // if all of these conditional fail simply return FAIL nothing to check here
    if ((rid.slotNo < 0) | (rid.pageNo != curPage)) {
        return FAIL;
    } 
        int length = slot[rid.slotNo].length;
        // if the record being deleted corresponds to the last slot
        // compact the slot
        int slotID = rid.slotNo;
        // created additional variable to hold the current offset before deleting the records
        int additional_memory = slot[rid.slotNo].offset;
        if (rid.slotNo < (slotCnt - 1)) {

            slot[rid.slotNo].offset = -1;
            slot[rid.slotNo].length = 0;
            /// if the record being deleted corresponds to the last slot
            // compact the slot
            for (int i = rid.slotNo + 1; i < (slotCnt); i++)
            {
                if (slot[i].offset != -1)
                {
                    slot[i].offset = slot[i].offset + length;
                }
            }
            //created additonal variable to specify where we should to return the deleted record
            int final_address = additional_memory + (slotID) * sizeof(slot_t);
            //address where the deleteing file will be copy from
            int origin = final_address - length;
            //size of the file to be copied
            int size = origin - usedPtr;
            // free space
            memmove(&data[usedPtr + length], &data[usedPtr], size * sizeof(char));
            // reposition usedPtr
            usedPtr = usedPtr + length;
            return OK;
        }
        else if (rid.slotNo == (slotCnt - 1)) {
            //delete the slot
            slotCnt = slotCnt - 1;
            usedPtr = usedPtr + length;

            // scan the slots from end to beginning and delete the ones marked as empty
            while (slot[(slotCnt - 1)].offset == EMPTY_SLOT)
            {
                slotCnt = slotCnt - 1;
            }

            return OK;
        }
        else {
            return FAIL;
        }
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID &firstRid) {
    // check if the firstRecod is at our current page
    Status status = DONE;
    if ((firstRid.pageNo != curPage)) {
        status = FAIL;
    }
    else {
        int i;
        while (i <= slotCnt - 1){
            if(slot[i].offset != -1){
                firstRid.slotNo = i;
                firstRid.pageNo = curPage;
                // if we find the slot and that is not empty
                // return OK
                status = OK;
            }
            i++;
        }
    }      
    return status;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord(RID curRid, RID &nextRid)
{
    // fill in the body

    if ((curRid.slotNo < 0) | (curRid.slotNo > slotCnt) | (curRid.pageNo != curPage))
    {
        return FAIL;
    }
    for (int i = curRid.slotNo + 1; i < slotCnt; i++)
    {
        if (slot[i].offset != -1)
        {
            nextRid.slotNo = i;
            nextRid.pageNo = curPage;
            return OK;
        }
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char *recPtr, int &recLen)
{
    // fill in the body
       if ((rid.slotNo < 0) | (rid.slotNo > slotCnt) | (rid.pageNo != curPage))
    {
        return FAIL;
    }
    recLen = slot[rid.slotNo].length;
    memcpy(&recPtr, &data[usedPtr - recLen], recLen * sizeof(char));
    //  int offset_ptr=(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo);
    //  *recPtr=data[offset_ptr-slot[rid.slotNo].length];
    //  *recPtr = data[(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo)];
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char *&recPtr, int &recLen)
{
    // fill in the body
       if ((rid.slotNo < 0) | (rid.slotNo > slotCnt) | (rid.pageNo != curPage))
    {
        return FAIL;
    }

    recLen = slot[rid.slotNo].length;
    recPtr = &data[slot[rid.slotNo].offset + sizeof(slot_t) * rid.slotNo - recLen];
    //   memcpy( recPtr,&data[usedPtr-recLen], slot[rid.slotNo].length * sizeof(char));
    // recPtr = &data[(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo)];

    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    //check the Number of empty slots
    freeSpace = usedPtr + sizeof(slot_t) * (1 - slotCnt);
    for (int i = 0; i < slotCnt; i++)
    {
        if (slot[i].offset == -1)
        {
            //if there is empty space than
            return freeSpace;
        }
    }

    return (freeSpace - (sizeof(slot_t)));
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
        for (int i = 0; i < slotCnt; i++)
    {
        if (slot[i].offset != -1)
            return false;
    }
    return true;
}