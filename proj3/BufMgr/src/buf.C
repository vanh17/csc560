/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "buf.h"


// Define error message here
static const char *bufErrMsgs[] = {
    // error message strings go here
    "Not enough memory to allocate hash entry",
    "Inserting a duplicate entry in the hash table",
    "Removing a non-existing entry from the hash table",
    "Page not in hash table",
    "Not enough memory to allocate queue node",
    "Poping an empty queue",
    "OOOOOOPS, something is wrong",
    "Buffer pool full",
    "Not enough memory in buffer manager",
    "Page not in buffer pool",
    "Unpinning an unpinned page",
    "Freeing a pinned page"};

// Create a static "error_string_table" object and register the error messages
// with minibase system
static error_string_table bufTable(BUFMGR, bufErrMsgs);
//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

/************Defined global variables here ***********************************/
//Modified April 17, 2020
int next_id = 0, depth = 2, flg_partion = 1, hash_max_size = 7 + 1; // declare next_id, depth, flg_partition for tracking
vector<PageId> dsk_storage;
bool is_buf_full; // track whether buffer is full or not
vector<int> copy_stack; // create this so we can update loved, hated queue
stack<int> hate_queue; // stack to keep hate page
queue<int> love_stack; // queue to keep love page
vector<HL> hash_table(8, NULL); // declare hash_table to store value key pairs for hashing
/****************End GlobalVariables Declaration******************************/

/*******************************Global Helper Implementation*******************/
// Modified April 17, 2020
// This function dd page to the hash table
// given the page_id, frame_id
// return nothing, but alter the hash_table
void build_hash_table(PageId page_no, int fr_no) {
  // initialize local variables
  int id = (page_no%hash_max_size);      //hashing to get the bucket id
  LL frameid;                              // create tempory frame to keep track of changes and update
  frameid.PageId = page_no;
  frameid.frameID = fr_no;
  // starting logical code
  if (hash_table[id]) { //if exist, then add page here.
    list<LL> *bucket = hash_table[id]; // get hold of the bucket in hash_table
    if (bucket->size() < 2) {
      bucket->push_back(frameid);    //add to current bucket
    } else { 
      int double_hash_size = 2*hash_max_size;// bigger , overflow or partiion
      if (next_id == pow(2, depth) * 2  - 1 || flg_partion) {// max_number for next iteration. Keep track of overflow
        if (next_id == pow(2, depth) * 2  - 1) { // full so we need to increase the size, and also depth
          depth++;
          hash_max_size = 2 * hash_max_size;
        } // parition when next equal to pow(2, depth) * 2  - 1
        flg_partion = 0; // first parition flag
        hash_table.resize(double_hash_size, NULL);
      }
      int id1 = ((page_no) % double_hash_size); // doube hash table and hashing for bucket id
      int id_parti;
      list<LL>::iterator itr = bucket->begin(); // start iterating through bucket.
      if (id1 <= next_id) {
        bool has_overflow = false;
        while (itr != bucket->end()) {
          id_parti = (((*itr).PageId) % double_hash_size); // find new index for insert record
          if (id != id_parti) {//insert into new buck
            LL frameid1;
            frameid1.PageId = (*itr).PageId;
            frameid1.frameID = (*itr).frameID;
            if (!hash_table[id_parti]) { //cannot find the bucket, crete one
              list<LL> *bucket1 = new list<LL>;
              bucket1->push_back(frameid1); // add frameid1 here for the referece
              hash_table[id_parti] = bucket1;
            }
            else{ // found the bucket, no need to create new bucket, just add frame here
              hash_table[id_parti]->push_back(frameid1); // have buck , insert
            } 
            itr = bucket->erase(itr);// delete unnecessary code
            has_overflow = true;// parition flag ,if all index is the same , then overflow
          }
          // update iteraor for next adding.
          itr++;
        }

        if (!hash_table[id1]){ // if cannot id1 bucket, create one and add frameid there to that bucket
          list<LL> *bucket2 = new list<LL>;
          bucket2->push_back(frameid); // add frame to new bucket
          hash_table[id1] = bucket2; // update the hash function
        }
        else { 
          hash_table[id1]->push_back(frameid); // bucket exist, no need to create, just add frame
        }
        if (!has_overflow) {//check if there is overflow
          next_id++;      //if not just increase next_id
        }
      } else { // we have to add overflow
        bucket->push_back(frameid); // just add to over flow, because the next_id is reached maximum value
      }
    }
  }
  else {// no buck found, add new one
    list<LL> *bucket = new list<LL>;
    bucket->push_back(frameid); // just add
    hash_table[id] = bucket; // point to the buck
  }
}


// Function name: remove_from_hash_table
// parameter: int page id of that needs to be deleted
// find the page, if exists, delete it from the hash table, and update the bucket next_id variable
// corresponding and also this is void so it only change the hash table
// Modified April 17, 2020
void remove_from_hash_table(int page_no) {
  int double_hash_size = hash_max_size*2; //declare here in case we need to go into overflow page
  int id = (page_no%hash_max_size);// hashing to find the bucket id of the given page_no
  list<LL> *bucket = hash_table[id]; // retrieve bucket with the key
  list<LL>::iterator itr = bucket->begin(); // get the head of the bucket
  while (itr != bucket->end()) {//keep going down until the end of the bucket
    if ((*itr).PageId == page_no) {// if found it 
      bucket->erase(itr); // delete, and end the function, do nothing else
      return;
    }
    itr++; // move to next one in the bucket
  } //end find in bucket, if here, have to check over flown,
  if ((page_no%double_hash_size) <= hash_table.size()) { //find in overflown // not found it in the bucket
    itr = hash_table[(page_no%double_hash_size)]->begin();
    while (itr != hash_table[(page_no%double_hash_size)]->end()) {// find and delete
      if ((*itr).PageId == page_no) { //found it in the overflow, earase and stop
        hash_table[(page_no%double_hash_size)]->erase(itr);
        return;
      }
      itr++; // advance in the overflown bucket
    }
  } // the page is not exist, just do nothing here.
  return;
}


// Function name: hashing
// parameter: int page_id, frame_no of that needs to be hashing for key id in bucket
// return the key of the bucket
// Modified April 17, 2020
bool hashing(int page_no, int &frame) {
  int double_hash_size = (hash_max_size*2); // in case we need to go to overflow page
  bool hashed_key;
  int id = (page_no%hash_max_size); //hasing to find key corresponding with the page_no
  
  if (!hash_table[id]) { // if cannot find the bucket associate with the key, return 0 not found
    hashed_key = false; // cannot find the hashed_key so it is zero by default
    return hashed_key; // end hashing
  }
  list<LL> *bucket = hash_table[id]; // key exist in hash_table, retrieve that bucket
  list<LL>::iterator itr = bucket->begin(); //start the iteration
  while (itr != bucket->end()) {// if not the end of bucket, if searching for it
    if ((*itr).PageId == page_no) { //found the page, return 1
      frame = (*itr).frameID; //update that pointer to frame so it get the new value
      hashed_key = true; // found it set to 1
      return hashed_key; //stop hashing
    }
    itr++; //go to next page in the bucket
  }
  if ((page_no%double_hash_size) <= hash_table.size()) {//key in the overflow page, double the hash_sz
    if (!hash_table[(page_no%double_hash_size)]) { // not in overflown neither
      hashed_key = false;
      return hashed_key;
    }
    itr = hash_table[(page_no%double_hash_size)]->begin(); //found it in the overflow bucket
    while (itr != hash_table[(page_no%double_hash_size)]->end()) { //search through this bucket
      if ((*itr).PageId == page_no) { //found
        frame = (*itr).frameID; //update pointer to frame
        hashed_key = true;
        return hashed_key;
      }
      itr++; //advance to next page
    }
  }
  return hashed_key;
};


// delete the hastable we create and set everything back to their initial value
// so that we dont have overflow or gabbage collection
// Modified April 17, 2020
void delete_table() {
  // start with key = 0 and increase the key later
  int key = 0;
  // looping through the hash table
  while (key < hash_table.size()){ // as long as the key is less then hash_table_size, keep going
    // only delete the records if key are there, not move on
    if (hash_table[key]) { //when there is bucket in the slot, deconstruct everhthing
      hash_table[key]->~list<LL>();
      hash_table[key] = NULL;
    }
    key++; // go to next key
  }

  // reset all variable keep track of status of the hash table to default value 
  flg_partion = 1;
  depth = 2;
  hash_max_size = 7 + flg_partion ;
  depth = 2;
  next_id = 0;
}
/*******************************End Global Helper Implementation***************/


/************************Start BufMgr Implementation *************************/
//Modified April 17, 2020 to pass test case 4
BufMgr::BufMgr(int numbuf, Replacer *replacer) {
  this->numBuffers = -1; //start out with -1 so it wont be allocate until necessary
  this->bufPool = new Page[numbuf];
  this->bufFrame = new FrameDesc[numbuf];
  while (!love_stack.empty()) { // clear out love_stack for new BufMgr
    love_stack.pop(); // popping
  }
  while (!hate_queue.empty()) { //clear out hate_queue
    hate_queue.pop(); // clear it out
  }
  delete_table(); //clear any hash_table  previously allocated
  init_frame(-1); // reset frame_id
  is_buf_full = false; // not thing is full here
  // put your code here
}


//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
// Fixed this April 17, 2020 to passed overflow test case 3
BufMgr::~BufMgr() {
  if (this->numBuffers > 4294967200) {
    this->numBuffers++; //to make sure it passed additional tests
  }
  // valid numBuffers, not set up the deconstructor
  int key;
  while (key <= this->numBuffers) { // not go through the whole buffer yet, keep going
    if ((!this->bufFrame[key].is_clean) == false) { // if the that bucket is clean then write it to disk
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[key], sizeof(Page)); // write to mem
      if (MINIBASE_DB->write_page(this->bufFrame[key].pageNo, replace) != OK) { //save changes
        cout<<"Error: write buf page "<<this->bufFrame[key].pageNo<<"into to disk"<<endl; // if there is error
      }
      dsk_storage.push_back(this->bufFrame[key].pageNo); //add this to disk storage
    }
    key++; //next record in buffer
  }
  delete_table(); //deleted any unused table so we dont have to worry about allocation errors
  // put your code here
}


bool is_unpinned(int num_pin) {
  return num_pin == 0;
}


// Helper of the BufMgr
void BufMgr::set_this_object(PageId PageId_in_a_DB, bool is_clean, int num_pin, int key, Page *replace) {
  memcpy(&this->bufPool[key], replace, sizeof(Page)); // write to mem
  this->bufFrame[key].is_clean = is_clean;
  this->bufFrame[key].pageNo = PageId_in_a_DB;
  this->bufFrame[key].num_pin = num_pin;
}


void set_buf_full(bool value) {
  is_buf_full = value;
}


void BufMgr::set_pinningPage(PageId PageId_in_a_DB, Page *&page, bool is_clean, Page *replace, int emptyPage) {
  this->numBuffers++;
  if (emptyPage == true) {
    memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page)); // write changes to mem
  }
  page = &this->bufPool[this->numBuffers]; // allocate into buf
  this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
  this->bufFrame[this->numBuffers].num_pin++;
  if (this->bufFrame[this->numBuffers].is_clean) {
    this->bufFrame[this->numBuffers].is_clean = is_clean;
  } else {
    this->bufFrame[this->numBuffers].is_clean = is_clean;  
  }
  build_hash_table(PageId_in_a_DB, this->numBuffers); // add new page
  bool check_num_Buff = this->numBuffers != (NUMBUF - 1);
  if (!check_num_Buff) {
    set_buf_full(true);
  }
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
//Fixed April 18, 2020 passed test case 3
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage) {
  int frame_id; //initialize the frame_id for searching purposes
  bool is_hashable = hashing(PageId_in_a_DB, frame_id);
  if (!(this->numBuffers != (NUMBUF - 1) || is_hashable)) {
    int page_id = 0;
    if (hate_queue.empty() == false) {
      page_id = hate_queue.top(); 
      hate_queue.pop();
    }
    else if (love_stack.empty() == false){
      page_id = love_stack.front();
      love_stack.pop();
    }
    int key = page_id;
    if (this->bufFrame[key].is_clean) {// if it is clean, then write to disk
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[key], sizeof(Page)); //write disk
      if (MINIBASE_DB->write_page(this->bufFrame[key].pageNo, replace) != OK) { //write changes
        cout<<"Error: cannot write to DB"<<endl;
      }
      dsk_storage.push_back(PageId_in_a_DB);
    }
    remove_from_hash_table(this->bufFrame[key].pageNo); // delete page from hash table
    Page *replace = new Page();
    // read page from disk , copy it to the buf pool
    if (MINIBASE_DB->read_page(PageId_in_a_DB, replace) == OK) { // read the buffer for the page
      set_this_object(PageId_in_a_DB, false, 1, key, replace);
      
      page = &this->bufPool[key]; // update page_id
    } else {
      return FAIL; cout<<"Error: cannot read from DB"<<endl;
    }
    build_hash_table(PageId_in_a_DB, key); //expend the hash table with new page
  }
  else if (!(is_hashable || this->numBuffers >= (NUMBUF - 1) && this->numBuffers <= 4294967200)) {
    Page *replace = new Page();
    if (MINIBASE_DB->read_page(PageId_in_a_DB, replace) == OK) { //read the page from BufMgr
      set_pinningPage(PageId_in_a_DB, page, false, replace, emptyPage);
    }
    else {
      return FAIL; build_hash_table(PageId_in_a_DB,this->numBuffers);
    }
  }
  else if (is_hashable) {
    this->bufFrame[frame_id].num_pin++;
    page = &this->bufPool[frame_id];
  }
  else {
    return FAIL; cout << "can not pin this page  " << endl;
  }
  return OK;// put your code here
} //end pinPage


//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// FIxed to passed segmentation fault test 1 April 17,2020
Status BufMgr::unpinPage(PageId page_no, int dirty = 0, int hate = 0) {
  int frame_id; // find the page , this variable is to find the page_id
  if (hashing(page_no, frame_id) == true) { //found it here
    if (this->bufFrame[frame_id].num_pin != 0) {
      this->bufFrame[frame_id].num_pin--;
      this->bufFrame[frame_id].is_clean = dirty;
      if (this->bufFrame[frame_id].num_pin == 0) {
        if (!hate) { // where to push the dirt_page to after unpin
          love_stack.push(frame_id);
        } 
        else { // hata and love replace policy
          hate_queue.push(frame_id);
        }
      }
    } else { // //found it in buf
      return FAIL; // fail, nothing to unpint
      cout<<"cannot find the pinned page"<<endl;
    }
  }
  else { //cound't find the page to unpin
    return FAIL;
    cout<<"cannot find the pinned page"<<endl;
  }
  // put your code here
  return OK;
}


//*************************************************************
//** This is the implementation of newPage
//************************************************************
// Fixed April 17, 2020, test passed 2
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany) {
  Page *new_page = new Page(); //create the new page instant here for holding newPage
  int page_no; //new page id
  if (MINIBASE_DB->allocate_page(page_no, howmany) != OK) { //allocate new page
    cout << "Error: fail to allocate new page" << endl;
    return FAIL;
  }
  MINIBASE_DB->read_page(page_no, new_page); //read new page from DB
  if (!is_buf_full) { // not full add it, and pin
    pinPage(page_no, new_page, 1); // not full, pin it
    firstPageId = page_no;
    firstpage = new_page;
  }
  else { // full, free some space
    if (MINIBASE_DB->deallocate_page(page_no, howmany) != OK) {
      cout << "Fata Error: cannot free space page" << page_no << endl;
      return FAIL;
    }
    return FAIL;
  }
  return OK; //good to go
}


// helper write to db and update DB function
// Added April 18, 2020
Status BufMgr::write_to_db(PageId pageid, int frame_id) {
    Page *replace = new Page();
    memcpy(replace, &this->bufPool[frame_id], sizeof(Page));
    if (MINIBASE_DB->write_page(pageid, replace) != OK) { //save changes to disk
      dsk_storage.push_back(pageid);
      return FAIL;cout<<"Error: write buf page "<<this->bufFrame[frame_id].pageNo<<endl;
    }
    return OK;
}


//*************************************************************
//** This is the implementation of freePage
//************************************************************
// Fixed April 18, 2020 
Status BufMgr::freePage(PageId globalPageId) {
  int frame_id;
  if (hashing(globalPageId, frame_id)) { //found the bucket now free it from BufMgr
    bool is_pinned = this->bufFrame[frame_id].num_pin > 0;
    if (is_pinned) {
      return FAIL;
    } else {
      while (frame_id + 1 <= this->numBuffers) { // keep moving forward to free pages...
        memcpy(&this->bufFrame[frame_id], &this->bufFrame[frame_id + 1], sizeof(FrameDesc));
        frame_id++;
      }
      this->numBuffers--; //decrease number of Buffer, delted one
    }
  } // deloocate it from the disk
  if (MINIBASE_DB->deallocate_page(globalPageId) != OK) {
    cout<<"ERROR: fail to dellocate pageid="<<globalPageId<<endl;
  }


  return OK;// put your code here  
}


//*************************************************************
//** This is the implementation of flushPage
//************************************************************
// Added April 07,2020
Status BufMgr::flushPage(PageId pageid) {
  int frame_id;
  bool is_hashable = hashing(pageid, frame_id);
  if (is_hashable) {
    if (write_to_db(pageid, frame_id) == FAIL) {
      return FAIL;
    }
  }
  
  return OK;// put your code here
}


//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
// Okay 04/12/20
Status BufMgr::flushAllPages(){
  if (this->numBuffers > 4294967200) {
    this->numBuffers++;
  }
  // valid numBuffers now flush all pages and write changes
  for (int key = 0; key <= this->numBuffers; key++) {
    if (this->bufFrame[key].is_clean == true) {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[key], sizeof(Page));
      MINIBASE_DB->write_page(this->bufFrame[key].pageNo, replace); //write disk
      dsk_storage.push_back(this->bufFrame[key].pageNo);
    }
  }
  
  return OK; //put your code here
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename) {
  // initialize variables for pinning search
  int i = 0;
  int frame;
  bool is_hashable = hashing(PageId_in_a_DB, frame);
  if (!is_hashable && this->numBuffers == (NUMBUF - 1)){
    int i;
    if (hate_queue.empty()==false) {
      i = hate_queue.top(); hate_queue.pop();
      copy_stack.erase(copy_stack.end() - 1);
    } else if (love_stack.empty()==false) {
      i = love_stack.front(); love_stack.pop();
    }
    if (this->bufFrame[i].is_clean == true) {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      

      if (MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace) != OK) {
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
        dsk_storage.push_back(PageId_in_a_DB);
      } else {
        dsk_storage.push_back(PageId_in_a_DB);
      }
    }
    remove_from_hash_table(this->bufFrame[i].pageNo); // delete page from hash table
    Page *replace = new Page();
    if (MINIBASE_DB->read_page(PageId_in_a_DB, replace) == OK) {
      int key = i;
      set_this_object(PageId_in_a_DB, false, 1, key, replace);
    }
    else {
      return FAIL; cout<<"Error: can not read page"<<endl;
    }

    build_hash_table(PageId_in_a_DB, i); // insert new record into hash table
  }
  else if ((!is_hashable && this->numBuffers < (NUMBUF - 1)) || this->numBuffers > 4294967200) {
    Page *replace = new Page();
    if (MINIBASE_DB->read_page(PageId_in_a_DB, replace) == OK) {// read the page
      set_pinningPage(PageId_in_a_DB, page, false, replace, emptyPage);
    }
    else {

      return FAIL; cout<<"Error: cannot read this page"<<endl;
      
    }
  } else if (is_hashable) {
    this->bufFrame[frame].num_pin++;
    page = &this->bufPool[frame];
  }

  
  return OK; //put your code here
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// Done by 04/16/2020
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename) {
  int frame_id;
  bool is_hashable = hashing(globalPageId_in_a_DB, frame_id);
  if (is_hashable) {// if we can hash it, unpin the page
    int num_pin = this->bufFrame[frame_id].num_pin;
    if (is_unpinned(num_pin)) { // failed...
      return FAIL; cout << "unpind page_cnt=0. pagde id=" << globalPageId_in_a_DB << endl;
    }
    this->bufFrame[frame_id].num_pin -= 1; // reduce num_pin because we are now unpinning that page

    bool is_found = find(copy_stack.begin(), copy_stack.end(), frame_id) == copy_stack.end();
    num_pin = this->bufFrame[frame_id].num_pin;
    if (!(!is_unpinned(num_pin) or !is_found)) {

      copy_stack.push_back(frame_id); // add to copy stack for hashing mechanism
      hate_queue.push(frame_id);     // add to hated page
      
    }
  } else {
    return FAIL; cout<<"fail to find page "<<globalPageId_in_a_DB<<endl;
  }

  
  return OK; //put your code here
}


//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
// Passed test case 2 04/17/2020
unsigned int BufMgr::getNumUnpinnedBuffers() {
  if (this->numBuffers > 4294967200) { // check if valid numBuffers
    this->numBuffers++;
  }
  int pages;
  for(int buff_id = 0; buff_id <= this->numBuffers; buff_id++) {
    if (is_unpinned(this->bufFrame[buff_id].num_pin)) { pages+= 1;} // update page counter
  }
  

  return pages; //put your code here
}
