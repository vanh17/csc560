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
  if (HFPage::insertRecord(recPtr, recLen, rid) != OK) { // if we can sucessfully insert here nothingelse to do
    return DONE; cout << "Successfully insert record to DB" << endl;
  }
  if (HFPage::slotCnt < 2) {
    return OK; cout << "This is fine because slotCNT is less then 2" << endl;
  }
  RID first_rec;
  HFPage::firstRecord(first_rec);
  RID curr_rec, nxt_rec;
  curr_rec.pageNo = HFPage::curPage;
  curr_rec.slotNo = 2*(first_rec.slotNo + HFPage::slotCnt - 2)/4 - 1;
  HFPage::nextRecord(curr_rec, nxt_rec); // cout <<"Getting next record" << endl;
  slot_t last = HFPage::slot[rid.slotNo]; // find position for newest insert slot
  int entry_len, rec_Len;
  void *key1, *key2;
  if (key_type == attrInteger) {
    entry_len = 4;
    Key_Int *a = (Key_Int *)recPtr;
    key1 = (void *)(&a->intkey);
  }
  else if (key_type == attrString)
  {
    entry_len = 8;
    Key_string *a = (Key_string *)recPtr;
    //  key1=(void *)a->charkey.c_str();
    key1 = (void *)a->charkey;
  }
  // scan whole , find a place to insert
  char *recPtr_comp;
  int i = first_rec.slotNo;
  while (i <= HFPage::slotCnt - 2) {
    nxt_rec.slotNo = i;
    HFPage::returnRecord(nxt_rec, recPtr_comp, rec_Len);
    if (key_type == attrInteger)
    {
      entry_len = 4;
      Key_Int *a = (Key_Int *)recPtr_comp;
      key2 = (void *)(&a->intkey);
    }
    else if (key_type == attrString)
    {
      entry_len = 8;
      Key_string *a = (Key_string *)recPtr_comp;
      //  key2=(void *)a->charkey.c_str();
      key2 = (void *)a->charkey;
    }
    if (keyCompare(key1, key2, key_type) < 0)
    {
      // find the place , and insert.
      for (int j = HFPage::slotCnt - 2; j > i - 1; j--)
      {
        //all slot position after j move back 1
        HFPage::slot[j + 1] = HFPage::slot[j];
      }
      // insert
      HFPage::slot[i] = last;
      rid.slotNo = i;
      break;
    }
    else
    {
      curr_rec.slotNo = i;
      HFPage::nextRecord(curr_rec, nxt_rec);
      i = nxt_rec.slotNo;
    }
  }

  // put your code here
  return OK;
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
