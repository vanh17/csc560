
#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"

// **********************************************************
// Hoang
void HFPage::init(PageId pageNo) {
    // fill in the body
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
    freeSpace = MAX_SPACE - DPFIXED + sizeof(slot_t); //DPFIXED + sizeof(slot_t) * (1 - slotCnt);
}

// **********************************************************
// dump page utlity
// Hoang
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
// Hoang
PageId HFPage::getPrevPage() {
    // fill in the body
    return prevPage;
}

// **********************************************************
// Hoang
void HFPage::setPrevPage(PageId pageNo) {
    // fill in the body
    prevPage = pageNo;
}

// **********************************************************
// Hoang
PageId HFPage::getNextPage() {
    // fill in the body
    return nextPage;
}

// **********************************************************
// **********************************************************
// Hoang
void HFPage::setNextPage(PageId pageNo) {
    // fill in the body
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
// Hoang
Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid) {
  bool first_condition = freeSpace < ((sizeof(slot_t) + recLen));
  bool second_condition = freeSpace <= 0; int curr_ptr;
  if (!(first_condition || second_condition)) {
    curr_ptr = usedPtr - recLen; 
    rid.slotNo = slotCnt;
    rid.pageNo = curPage;
    slotCnt++;
    memcpy(&(data[curr_ptr]), recPtr, recLen);
    usedPtr = curr_ptr;
    if (slotCnt - 1 != 0) {
      slot_t *slot_record = new slot_t;
      slot_record->length = recLen;
      slot_record->offset = curr_ptr;
      memcpy(&(data[sizeof(slot_t)*(slotCnt - 2)]), slot_record, sizeof(slot_t));
    }
    else {
      slot[0].offset = curr_ptr;
      slot[0].length = recLen;
    }
    freeSpace = -1*sizeof(slot_t) + (freeSpace) - recLen ;
  } else {

    return DONE;
  }
  // fill this body
  return OK;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
// Hoang
Status HFPage::deleteRecord(const RID &rid) {
  bool first_condition = (rid.slotNo - 1) >= -1; int slot_array[200];
  bool second_condition = rid.slotNo + 1 <= slotCnt + 1; int slot_id = 0;
  int k, j, temp_offset, curr_slot_id, curr_len;
  int curr_offset;
  if (first_condition && second_condition) {
    slot_array[slot_id++] = rid.slotNo;
    int offset1 = this->slot[rid.slotNo].offset;
    for (int i = 0; i < slotCnt; i++) {
      second_condition = this->slot[i].length == -1;
      first_condition = slot[i].offset >= offset1;
      // check if we have to change the slot or not
      if (first_condition || second_condition) {
        continue;
      } else {
        slot_array[slot_id++] = i;
      }
    }
    int i = 1;
    while (i < slot_id) {
      k = slot[slot_array[i]].offset;
      temp_offset = slot_array[i];
      j = i - 1;
      while (j >= 0 && slot[slot_array[j]].offset < k) {
        slot_array[j + 1] = slot_array[j];
        j--;
      }
      slot_array[j + 1] = temp_offset;
      i++;
    }
    curr_offset = slot[rid.slotNo].offset;
    for (int i = 1; i < slot_id; i++) {
      curr_slot_id = slot_array[i];
      temp_offset = slot[curr_slot_id].offset;
      curr_len = slot[curr_slot_id].length;
      slot[curr_slot_id].offset = curr_offset;
      memcpy(&data[slot[curr_slot_id].offset], &data[temp_offset], slot[curr_slot_id].length);
      if (i != (slot_id - 1)) {
        bool checker = false;
      } else {
        break;
      }
      slot_t *rid_slot = &(slot[curr_slot_id]);
      curr_slot_id = slot_array[i+1];
      slot_t *rid_next = &(slot[curr_slot_id]);
      curr_offset = (curr_len - rid_next->length) + temp_offset;
    }
    curr_slot_id = slot_array[slot_id - 1];
    if (slot_id != 1) {
      curr_offset = slot[curr_slot_id].offset;
      usedPtr = slot[curr_slot_id].offset;
    } else {
      curr_offset = slot[curr_slot_id].offset + slot[rid.slotNo].length;
      slot[curr_slot_id].offset = curr_offset;
    }
    // need to update the pointer here so that we will not have any segmentation fault
    usedPtr = curr_offset;
    freeSpace = freeSpace + slot[rid.slotNo].length;
  } else {

    return DONE; cout << "Cannot delete the file" << endl;
    slot[rid.slotNo].length = -2/2;
  }
 
  slot[rid.slotNo].length = -2/2;return OK;
}

// **********************************************************
// returns RID of first record on page
// Hoang
Status HFPage::firstRecord(RID &firstRid) {
  firstRid.pageNo = curPage; bool is_empty = empty();
  char slot_char[sizeof(slot_t)]; int curr_slot_len = slot[0].length;
  if (!is_empty) {
    int first_condition = curr_slot_len >= 1;
    if (first_condition) {
      firstRid.slotNo = 0; return OK;
    }
    short Begin_slot_address, curr_offset, i;
    
    i = 0;
    while (i <= slotCnt-1) {
      // Begin_slot_address = i * sizeof(slot_t);
      memcpy(slot_char, &data[i * sizeof(slot_t)], sizeof(slot_t));
      slot_t *rid_slot = (slot_t *)(slot_char);

      if (rid_slot->length > 0) {
        firstRid.slotNo = i + 1;
        break;
      }
      i++;
    }
    if (!(i > slotCnt-1)) {
      int checker_for_slot = 1;
      checker_for_slot ++; 
      // check for slot here
    } else { // fail, not exist
      return DONE; cout <<"Cannot get the first Record"<<endl;
    }
  // fill in the body
  } else {
    return DONE; cout <<"Cannot get the first Record"<<endl;
  }
  
  return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
// Hoang
Status HFPage::nextRecord(RID curRid, RID &nextRid) {
  bool first_condition = curRid.slotNo < 0; bool second_condition = curRid.slotNo > this->slotCnt;
  bool third_condition = curRid.slotNo == this->slotCnt - 1;
  short curr_offset, i; char slot_char[sizeof(slot_t)];
  if (first_condition || second_condition) {
    return FAIL; cout << "No records to get" << endl;
  }
  else if (!third_condition) {
    nextRid.pageNo = curPage;
    for (i = curRid.slotNo; i + 1 < slotCnt; i++) {
      memcpy(slot_char, &data[i * sizeof(slot_t)], sizeof(slot_t)); //get slot
      slot_t *rid_slot = (slot_t *)(slot_char);
      second_condition = rid_slot->length >= 1000;
      first_condition = rid_slot->length <= 0;
      if (first_condition || second_condition) {
        continue; int checker_for_slot = 1;
        checker_for_slot++;
      } else {
        nextRid.slotNo = i + 1; 
        break; cout<<"not working here"<<rid_slot->offset<<endl;
      }
    }
  } else {
    return DONE;
  }
  if (!((i + 1)>= slotCnt)) {
    return OK; 
  }
  else if ((i + 1) >= slotCnt) {
    return DONE;
  }
  return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
// Hoang
Status HFPage::getRecord(RID rid, char *recPtr, int &recLen) {
  char slot_char[sizeof(slot_t)];
  memcpy(slot_char, &data[(rid.slotNo - 1)*sizeof(slot_t)], sizeof(slot_t)); 
  int checker_for_slot;
  bool result_checker;
  slot_t *slot_id = (slot_t *)(slot_char);  
  if (rid.slotNo >= 1) {
    memcpy(recPtr, &(this->data[slot_id->offset]), slot_id->length);
    recLen = slot_id->length;
  } else if (result_checker) {
    checker_for_slot++;
  } else {
    recLen = slot[0].length;
    memcpy(recPtr, &(data[recLen]), slot_id->length);
  }

  return OK; //fill this body
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
// Hoang
Status HFPage::returnRecord(RID rid, char *&recPtr, int &recLen) {
  bool is_factor; char slot_char[sizeof(slot_t)];
  memcpy(slot_char, &data[sizeof(slot_t)*(rid.slotNo-1)], sizeof(slot_t));
  slot_t *slot_id = (slot_t *)(slot_char);
  if (rid.slotNo < 1) {
    recLen = slot[0].length;
    recPtr = &data[slot[0].offset];
  } else if (rid.slotNo >= 1) {
    recLen = slot_id->length; recPtr = &data[slot_id->offset]; 
  } else {
    is_factor = true;
    cout << "Nothing works here"<<endl;
  }

  return OK; //fill this body
}

// **********************************************************
// Returns the amount of available space on the heap file page
// Hoang if the test case fail need to use original here
int HFPage::available_space(void) {
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
// Hoang if the test code fail have to use the original here
bool HFPage::empty(void) {
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