/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"
const char *SortedPage::Errors[SortedPage::NR_ERRORS] = {
    //OK,
    //Insert Record Failed (SortedPage::insertRecord),
    //Delete Record Failed (SortedPage::deleteRecord,
};

// Hoang 
// assiging right value to a type
void value_assigner_by_type(AttrType key_type,
                                char *recPtr, void* key) {
  bool first_condition = key_type == attrInteger;
  bool second_condition = key_type == attrString;
  if (first_condition) {
    Key_Int *int1 = (Key_Int *)recPtr;
    key = (void *)(&int1->intkey);
  }
  if (second_condition) {
    Key_string *str1 = (Key_string *)recPtr;
    //  key1=(void *)a->charkey.c_str();
    key = (void *)str1->charkey;
  }
}
/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */
//Start
Status SortedPage::insertRecord(AttrType key_type,
                                char *recPtr,
                                int recLen,
                                RID &rid) {
  int  recSize; void *key1, *key2; int top, bottom;
  if (HFPage::insertRecord(recPtr, recLen, rid) != OK) { // if we can sucessfully insert here nothingelse to do
    return DONE; cout << "Successfully insert record to DB" << endl;
  }
  top = HFPage::slotCnt - 2; 
  if (top + 2 < 2) {
    return OK; cout << "This is fine because slotCNT is less then 2" << endl;
  }
  RID first_rec;
  HFPage::firstRecord(first_rec);
  bottom = first_rec.slotNo;
  RID curr_rec, nxt_rec;
  curr_rec.pageNo = HFPage::curPage;
  curr_rec.slotNo = 2*(bottom + top)/4 - 1;
  HFPage::nextRecord(curr_rec, nxt_rec); // cout <<"Getting next record" << endl;
  slot_t new_slot = HFPage::slot[rid.slotNo]; //initialize the slot to insert into
  value_assigner_by_type(key_type, recPtr, key1);
  bool first_condition = key_type == attrInteger;
  bool second_condition = key_type == attrString;
  if (first_condition) {
    Key_Int *int1 = (Key_Int *)recPtr;
    key1 = (void *)(&int1->intkey);
  }
  if (second_condition) {
    Key_string *str1 = (Key_string *)recPtr;
    key1 = (void *)str1->charkey;
  }
  int i = bottom;
  char *rec_ptr;
  while (i <= top) { //find new place to insert into the code
    nxt_rec.slotNo = i;
    HFPage::returnRecord(nxt_rec, rec_ptr, recSize);
    value_assigner_by_type(key_type, rec_ptr, key2);
    first_condition = key_type == attrInteger;
    second_condition = key_type == attrString;
    if (first_condition) {
      Key_Int *int1 = (Key_Int *)rec_ptr;

      key2 = (void *)(&int1->intkey); //update to int for comparison
    }
    if (second_condition) {
      Key_string *str1 = (Key_string *)rec_ptr;

      key2 = (void *)str1->charkey; //update key2 for comparison to string
    }
    if (keyCompare(key1, key2, key_type) >= 0) {

      curr_rec.slotNo = i;
      HFPage::nextRecord(curr_rec, nxt_rec);
      // update i for next iteration
      i = nxt_rec.slotNo;
    }
    else {
      int j = HFPage::slotCnt - 2;
      while (j > i - 1) {
        HFPage::slot[j + 1] = HFPage::slot[j]; j--;//move up by one
      }
      HFPage::slot[i] = new_slot; rid.slotNo = i;// new current last slot and new_slot earlier declared
      break; cout << "Break here" << endl;
    }
  }
  return OK; // put your code here
}

/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */
// Called HFPage deleterecord function to remove the page from this page.
// Hoang
Status SortedPage::deleteRecord(const RID &rid) {
  return HFPage::deleteRecord(rid);
}

// Hoang
Status SortedPage::get_large_key_value(AttrType key_type, void *key, int &keylen) {
  Status result;
  if (key_type == attrString) {
    result = get_key_helper(key, key_type, keylen);
  } else if (key_type == attrInteger) {
    result = get_key_helper(key, key_type, keylen);
  } else {
    result = get_key_helper(key, key_type, keylen);
  }
  return result;
}

// Hoang
Status SortedPage::get_key_helper(void *key, AttrType key_type,int &keylen) {
  int itr, size_rec;
  RID rid;
  char *rec_ptr;
  itr = HFPage::slotCnt - 1;
  while (itr >= 0) {
    bool first_condition = (HFPage::slot[itr].length <= 0);
    bool second_condition = (HFPage::slot[itr].length > 0); 
    if (first_condition) {
      itr--;
      continue; // keep the loop and going to next it
    } else if (second_condition) {
      break;
    } else {
      itr++;
    }
  }
  //we find the slot with length less then zero, then get the key therer
  rid.pageNo = HFPage::curPage; // update rid page_no to keep track of the currPage
  rid.slotNo = itr; // update slot number with where the zero are
  // check condition for type to make sure we will update to the right type
  bool type_check1 = (key_type == attrInteger) && (key_type != attrString);
  bool type_check2 = (key_type != attrInteger) && (key_type == attrString);
  // end type checking
  HFPage::returnRecord(rid, rec_ptr, size_rec); //(call returnRecord from HFPage)
  update_keyLen(rec_ptr, type_check1, type_check2, key, key_type, keylen);
  return OK; //put your code here
}

// Hoang
void SortedPage::update_keyLen(char *rec_ptr, bool type_check1, bool type_check2, void *key, AttrType key_type,int &keylen) {
  if (type_check1) {
    Key_Int *int1 = (Key_Int *)rec_ptr;
    //save changes to key
    memcpy(key, &int1->intkey, sizeof(int));
    //update keylen
    keylen = sizeof(int);
  }
  else if (type_check2) {

    Key_string *str = (Key_string *)rec_ptr;
    // save changes to mem
    memcpy(key, str->charkey, sizeof(str->charkey));
    keylen = sizeof(str->charkey); //update keylen
  }
}
