/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */
BTLeafPage *scan_leaf = new BTLeafPage();
RID first, next;
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
			memcpy(scan_leaf, leaf_read, sizeof(Page));
			// find the first record  of a leaf page
			scan_leaf->firstRecord(first);
			while (first.slotNo < this->R_Start.slotNo)
			{
				scan_leaf->nextRecord(first, next);
				first = next;
			}

			// get the first record of a leaf page
			scan_leaf->HFPage::returnRecord(first, recPtr_comp, rec_Len);
			if (this->keytype != attrString)
			{
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

			flag_init = true;
		} else {
			if (scan_leaf->nextRecord(first, next) == OK) {
				// reach the end (the high key), stop
				if (next.pageNo == this->end && next.slotNo > this->R_End.slotNo)
					return DONE;
				scan_leaf->HFPage::returnRecord(next, recPtr_comp, rec_Len);

				if (this->keytype != attrString) {
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
				first = next;
			} else {
				PageId next_page = scan_leaf->getPrevPage();

				if (next_page < 0)
					return DONE; // final page , return

				leaf_Read = MINIBASE_DB->read_page(next_page, leaf_read);

				scan_leaf = (BTLeafPage *)leaf_read;
				if (scan_leaf->empty())
					return DONE; // empty page . return DONE
				scan_leaf->firstRecord(first);
				// reach final page and reach high key rid , stop
				if (next_page == this->end && first.slotNo >= this->R_End.slotNo)
					return DONE;
				scan_leaf->HFPage::returnRecord(first, recPtr_comp, rec_Len);
				if (this->keytype == attrInteger)
				{
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

Status BTreeFileScan::delete_current()
{

	// cout<<"page number "<<first.pageNo<<" slot number "<<first.slotNo<<endl;

	return scan_leaf->HFPage::deleteRecord(first);

	// put your code here
	// return OK;
}

int BTreeFileScan::keysize()
{
	if (this->keytype == attrInteger)
		return sizeof(int);
	else if (this->keytype == attrString)
		return 220;
	// put your code here
	return OK;
}
