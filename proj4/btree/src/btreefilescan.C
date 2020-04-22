/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */
// Hoang
BTreeFileScan::~BTreeFileScan() {
	// set head and tail to the same thing.
	begin = 0; end = 0;
	is_initialized = false;// put your code here
}

Status BTreeFileScan::get_next(RID &rid, void *keyptr) {
	if (begin != -1) {
		RID curr;
		char *recChar; //declare pointer to char of record
		int recordSize; // initialize record size
		PageId page_id; Page *currPage = new Page(); //initialize the beginning and currPage to keep track of
		bool checker_for_slot;
		bool first_condition, second_condition, third_condition;
		if (is_initialized == false) {
			page_id = begin;
			MINIBASE_DB->read_page(page_id, currPage); // read from DB
			memcpy(leaf_page, currPage, sizeof(Page));
			leaf_page->firstRecord(head_ptr);
			first_condition = head_ptr.slotNo < R_Start.slotNo;
			while (first_condition) {
				leaf_page->nextRecord(head_ptr, nxt_ptr);
				head_ptr = nxt_ptr;
				first_condition = head_ptr.slotNo < R_Start.slotNo;
			}
			leaf_page->HFPage::returnRecord(head_ptr, recChar, recordSize);
			second_condition = keytype != attrString;
			if (second_condition) {
				Key_Int *integer = (Key_Int *)recChar;
				memcpy(keyptr, &(integer->intkey), sizeof(int));
				rid = integer->data.rid;
			}
			else if (!second_condition) {
				Key_string *str = (Key_string *)recChar;
				memcpy(keyptr, str->charkey, sizeof(str->charkey));
				rid = str->data.rid;
			} else {
				checker_for_slot = true;
			}

			is_initialized = true;
		} else {
			if (leaf_page->nextRecord(head_ptr, nxt_ptr) == OK) {
				if (get_next_helper(rid, keyptr, recChar, recordSize)==2) {
					return DONE;
				}
			} else {
				if (get_next_not_initial(rid, keyptr, currPage, recChar, recordSize) == 2) {
					return DONE;
				}
			}
		}
		delete currPage; //delete here to avoid overflow error.
		// put your code here
		
	} else { 
		return DONE; cout <<" Done here nothing else to do" << endl;
	}
	return OK;
}

/*************************** Hepers Implementation *********************************************************************/
// Hoang
int BTreeFileScan::get_next_helper(RID &rid, void* keyptr, char* recChar, int recordSize) {
	bool first_condition, second_condition, third_condition;
	first_condition = nxt_ptr.pageNo == end;
	second_condition = nxt_ptr.slotNo > R_End.slotNo
	if (first_condition && second_condition) {
		return 2;
	}
	leaf_page->HFPage::returnRecord(nxt_ptr, recChar, recordSize);
	first_condition = keytype != attrString;
	if (first_condition) {
		Key_Int *integer = (Key_Int *)recChar;
		memcpy(keyptr, &(integer->intkey), sizeof(int));
		rid = integer->data.rid;
	}
	else if (!first_condition) {
		Key_string *str = (Key_string *)recChar;
		memcpy(keyptr, str->charkey, sizeof(str->charkey));
		rid = str->data.rid;
	}
	head_ptr = nxt_ptr;return 1;
}
// Hoang
int BTreeFileScan::get_next_not_initial(RID &rid, void* keyptr, Page* currPage, char* recChar, int recordSize) {
	PageId nxt_page = leaf_page->getPrevPage();
	if (nxt_page <= -1) {
		return 2;
	}
	MINIBASE_DB->read_page(nxt_page, currPage);
	leaf_page = (BTLeafPage *)currPage;
	if (leaf_page->empty() == false) {
		
		leaf_page->firstRecord(head_ptr);
	// reach final page and reach high key rid , stop
	if (nxt_page == this->end && head_ptr.slotNo >= this->R_End.slotNo) {
		return 2;
	}
	leaf_page->HFPage::returnRecord(head_ptr, recChar, recordSize);
	if (this->keytype == attrInteger) {
		Key_Int *integer = (Key_Int *)recChar;
		memcpy(keyptr, &(integer->intkey), sizeof(int));

		rid = integer->data.rid;
	}
	else if (this->keytype == attrString) {
		Key_string *str = (Key_string *)recChar;
		memcpy(keyptr, str->charkey, sizeof(str->charkey));
		rid = str->data.rid;
	}
	} else {
		return 2;
	}
	
	return 1;
}

// Hoang
Status BTreeFileScan::delete_current() {
	Status delete_status = leaf_page->HFPage::deleteRecord(head_ptr);
	if (delete_status != OK) {
		return delete_status; // put your code here
	} else {
		return OK;
	}
}

//Hoang
int BTreeFileScan::keysize() {
	bool first_condition = (keytype == attrString);
	bool second_condition = keytype == attrInteger;
	if (first_condition) {
		return 220;
		
	}
	else if (second_condition) {
		return sizeof(int);
		// return size of int for the size of key
	} else {
		return OK; // put your code here
	}
	return -1; // it is fine to return this to catch error
}
