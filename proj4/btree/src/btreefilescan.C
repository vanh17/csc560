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
BTreeFileScan::~BTreeFileScan() {
	// set head and tail to the same thing.
	begin = 0; end = 0;
	flag_init = false;// put your code here
}

Status BTreeFileScan::get_next(RID &rid, void *keyptr) {
	if (begin != -1) {
		int rec_Len;
		Status leaf_Read;
		char *recPtr_comp;
	
		PageId pageNo;
		Page *leaf_read = new Page();
		RID curr;
		if (flag_init == false) {
			// scan first element
			pageNo = this->begin;
			leaf_Read = MINIBASE_DB->read_page(pageNo, leaf_read);
			memcpy(leaf_page, leaf_read, sizeof(Page));
			// find the first record  of a leaf page
			leaf_page->firstRecord(head_ptr);
			while (head_ptr.slotNo < this->R_Start.slotNo) {
				leaf_page->nextRecord(head_ptr, nxt_ptr);
				head_ptr = nxt_ptr;
			}

			// get the first record of a leaf page
			leaf_page->HFPage::returnRecord(head_ptr, recPtr_comp, rec_Len);
			if (this->keytype != attrString) {
				Key_Int *a = (Key_Int *)recPtr_comp;

				memcpy(keyptr, &(a->intkey), sizeof(int));
				rid = a->data.rid;
			}
			else if (this->keytype == attrString) {
				Key_string *a = (Key_string *)recPtr_comp;

				memcpy(keyptr, a->charkey, sizeof(a->charkey));

				rid = a->data.rid;
			}

			flag_init = true;
		} else {
			if (leaf_page->nextRecord(head_ptr, nxt_ptr) == OK) {
				// reach the end (the high key), stop
				if (nxt_ptr.pageNo == this->end && nxt_ptr.slotNo > this->R_End.slotNo)
					return DONE;
				leaf_page->HFPage::returnRecord(nxt_ptr, recPtr_comp, rec_Len);

				if (this->keytype != attrString) {
					Key_Int *a = (Key_Int *)recPtr_comp;

					memcpy(keyptr, &(a->intkey), sizeof(int));

					rid = a->data.rid;
				}
				else if (this->keytype == attrString) {
					Key_string *a = (Key_string *)recPtr_comp;

					memcpy(keyptr, a->charkey, sizeof(a->charkey));

					rid = a->data.rid;
				}
				head_ptr = nxt_ptr;
			} else {
				PageId nxt_page = leaf_page->getPrevPage();

				if (nxt_page < 0)
					return DONE; // final page , return

				leaf_Read = MINIBASE_DB->read_page(nxt_page, leaf_read);

				leaf_page = (BTLeafPage *)leaf_read;
				if (leaf_page->empty())
					return DONE; // empty page . return DONE
				leaf_page->firstRecord(head_ptr);
				// reach final page and reach high key rid , stop
				if (nxt_page == this->end && head_ptr.slotNo >= this->R_End.slotNo)
					return DONE;
				leaf_page->HFPage::returnRecord(head_ptr, recPtr_comp, rec_Len);
				if (this->keytype == attrInteger) {
					Key_Int *a = (Key_Int *)recPtr_comp;
					memcpy(keyptr, &(a->intkey), sizeof(int));

					rid = a->data.rid;
				}
				else if (this->keytype == attrString)
				{
					Key_string *a = (Key_string *)recPtr_comp;

					memcpy(keyptr, a->charkey, sizeof(a->charkey));

					rid = a->data.rid;
				}
			}
		}
		delete leaf_read;
		// put your code here
		
	} else { 
		return DONE; cout <<" Done here nothing else to do" << endl;
	}
	return OK;
}

Status BTreeFileScan::delete_current() {
	Status delete_status = leaf_page->HFPage::deleteRecord(head_ptr);
	if (delete_status != OK) {
		return delete_status; // put your code here
	} else {
		return OK;
	}
}

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
		return 0; // put your code here
	}
	return -1; // it is fine to return this to catch error
}
