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
	flag_init = false;// put your code here
}

Status BTreeFileScan::get_next(RID &rid, void *keyptr) {
	if (begin != -1) {
		RID curr;
		char *recChar; //declare pointer to char of record
		int recordSize; // initialize record size
		PageId page_id; Page *currPage = new Page(); //initialize the beginning and currPage to keep track of
		// the current page
		if (flag_init == false) {
			page_id = begin;
			MINIBASE_DB->read_page(page_id, currPage); // read from DB
			memcpy(leaf_page, currPage, sizeof(Page));
			// find the first record  of a leaf page
			leaf_page->firstRecord(head_ptr);
			while (head_ptr.slotNo < this->R_Start.slotNo) {
				leaf_page->nextRecord(head_ptr, nxt_ptr);
				head_ptr = nxt_ptr;
			}

			// get the first record of a leaf page
			leaf_page->HFPage::returnRecord(head_ptr, recChar, recordSize);
			if (this->keytype != attrString) {
				Key_Int *a = (Key_Int *)recChar;

				memcpy(keyptr, &(a->intkey), sizeof(int));
				rid = a->data.rid;
			}
			else if (this->keytype == attrString) {
				Key_string *a = (Key_string *)recChar;

				memcpy(keyptr, a->charkey, sizeof(a->charkey));

				rid = a->data.rid;
			}

			flag_init = true;
		} else {
			if (leaf_page->nextRecord(head_ptr, nxt_ptr) == OK) {
				// reach the end (the high key), stop
				if (nxt_ptr.pageNo == this->end && nxt_ptr.slotNo > this->R_End.slotNo)
					return DONE;
				leaf_page->HFPage::returnRecord(nxt_ptr, recChar, recordSize);

				if (this->keytype != attrString) {
					Key_Int *a = (Key_Int *)recChar;

					memcpy(keyptr, &(a->intkey), sizeof(int));

					rid = a->data.rid;
				}
				else if (this->keytype == attrString) {
					Key_string *a = (Key_string *)recChar;

					memcpy(keyptr, a->charkey, sizeof(a->charkey));

					rid = a->data.rid;
				}
				head_ptr = nxt_ptr;
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

int BTreeFileScan::get_next_not_initial(RID &rid, void* keyptr, Page* currPage, char* recChar, int recordSize) {
	PageId nxt_page = leaf_page->getPrevPage();
	if (nxt_page < 0) {
		return 2; // final page , return
	}

	MINIBASE_DB->read_page(nxt_page, currPage); // read the page from DB

	leaf_page = (BTLeafPage *)currPage;
	if (leaf_page->empty()) {
		return 2; // empty page . return DONE
	}
	leaf_page->firstRecord(head_ptr);
	// reach final page and reach high key rid , stop
	if (nxt_page == this->end && head_ptr.slotNo >= this->R_End.slotNo) {
		return 2;
	}
	leaf_page->HFPage::returnRecord(head_ptr, recChar, recordSize);
	if (this->keytype == attrInteger) {
		Key_Int *a = (Key_Int *)recChar;
		memcpy(keyptr, &(a->intkey), sizeof(int));

		rid = a->data.rid;
	}
	else if (this->keytype == attrString) {
		Key_string *a = (Key_string *)recChar;

		memcpy(keyptr, a->charkey, sizeof(a->charkey));

		rid = a->data.rid;
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
