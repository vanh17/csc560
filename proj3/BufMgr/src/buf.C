/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/
#include "buf.h"

/***************Global Variables******************************/
unsigned int next_id = 0, depth = 2, partion_id = 1;
//Global Hash table to hold the key and pair value.
vector<HL> hash_table(8, NULL);
//size of a buffer hash table
unsigned int hash_size = HTSIZE + partion_id;
bool is_buf_full = false;
// Loved Frame holder
queue<int> loved_queue;
// disk holder
vector<PageId> storage;
// Hated Frame holder
stack<int> hated_stack;
// the copy 
vector<int> copy_stack;
/************************************************************/
// Define buffer manager error messages here
//enum bufErrCodes  {...};
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

static error_string_table bufTable(BUFMGR, bufErrMsgs);
/**************************Global Helpers Definition*****************************/
// add page to the hash table
// given the page_id, frame_id
// return nothing, but alter the hash_table
// Modified April 02, 2020 add page to hash_table
void add_page(PageId page_id, int frame_id) {
  LL frame_holder; //create the frame to hold the data of the hashed key
  // max_pages can be stored in this Frame
  int max_page = pow(2, depth) * 2 - 1; 
  int key = page_id % hash_size;
  frame_holder.frameID = frame_id;
  frame_holder.PageId = page_id;
  // create new slot in case we need it to hold new frame
  list<LL> *slot = new list<LL>;

  if (!hash_table[key]) { // if the key doesn't there yet, create new slot for it
    // add frame to the hash table
    slot->push_back(frame_holder);
    hash_table[key] = slot; // the hash function will point to the ptr point to the slot
  }
  else {// the key is already exist, now have to check if the slot is full or not
    slot = hash_table[key];
    if (slot->size() < 2) {
      slot->push_back(frame_holder);    // insert into the buck
    }
    else {
      if (partion_id || max_page == next_id) {
        if (max_page == next_id) {
          depth++;
          hash_size = 2 * hash_size;
          // full, double the size
        }
        hash_table.resize(2 * (hash_size), NULL);
        partion_id = 0;
      }
      // Added doubled_hashbuf April 2nd, 2020
      int doubled_hashbuf = hash_size * 2;
      int new_key = (page_id) % doubled_hashbuf; // since size is double, update key
      int partion_index;
      list<LL>::iterator ptr = slot->begin();
      if (new_key > next_id) {
        slot->push_back(frame_holder); // overflow
      }
      else{
        int overflow = 0;
        while (ptr != slot->end()) {
          partion_index = (*ptr).PageId;
          partion_index = (partion_index) % doubled_hashbuf; // update index
          if (key != partion_index) { // if they are not the same, we have to create new frame
            LL new_frame_holder;
            new_frame_holder.PageId = (*ptr).PageId;
            new_frame_holder.frameID = (*ptr).frameID;
            if (!hash_table[partion_index]) {
              list<LL> *new_slot = new list<LL>;
              new_slot->push_back(new_frame_holder);
              hash_table[partion_index] = new_slot;
            }
            else
              hash_table[partion_index]->push_back(new_frame_holder); // add new frame to hash table
              ptr = slot->erase(ptr); //delete the slot
              overflow = 1;                                   
          }
          ptr++;
        }

        if (!hash_table[new_key]){
          list<LL> *slot2 = new list<LL>;
          slot2->push_back(frame_holder);
          hash_table[new_key] = slot2;
        }
        else {
          hash_table[new_key]->push_back(frame_holder);
        }
        if (!overflow) {next_id++;} // good to go because there is no overflow
      }
    }
  }
}

// global function defnition
// remove page from hash table
// given page_id
// Modified April 02, 2020
void remove_page(int page_id) {
  int pos = page_id % hash_size;     //hash the key value
  list<LL> *slot = hash_table[pos]; // get hold of the slot with hashed key
  list<LL>::iterator ptr = slot->begin(); // starting from the head of the LL
  bool at_tail = (ptr != slot->end());
  bool removed_page = false;
  while (at_tail || removed_page) { // search through the whole slot
    if ((*ptr).PageId == page_id) {
      slot->erase(ptr);
      removed_page = true;
      break;
    }
    ptr++;
    at_tail = (ptr != slot->end());
  }
  // Added doubled_hashbuf April 2nd, 2020
  // if already found the page and removed it, do nothing here
  if (!removed_page) {
    unsigned int doubled_hashbuf = hash_size * 2;
    pos = page_id % doubled_hashbuf; //hashing the page_id to hashed key
    // if pos is not in the hash_table, then do nothing
    if (pos > hash_table.size()) {
      return;
    }
    if (pos <= hash_table.size()) {
      slot = hash_table[pos];
      ptr = slot->begin();
      at_tail = (ptr != slot->end());
      while (at_tail) { // iterate through LL and delete found page
        if ((*ptr).PageId == page_id) {
          slot->erase(ptr);
          break;
        }
        ptr++;
        at_tail = (ptr != slot->end());
      }
    }
  } else {
    return;
  }
}

// search within the hash table to find the page we need 
// this function will take frameID and pageID to better 
// find the doc we need
// Fixed April 02, 2020
bool search_frame(int page_id, int &frame_id) {
  // index is page_id mod hash_size, this is for hashing
  int pos = (page_id) % hash_size;
  if (!hash_table[pos]) {return 0;} // no key exist
  list<LL> *slot = hash_table[pos];
  list<LL>::iterator ptr = slot->begin();
  bool at_tail = (ptr != slot->end());
  while (at_tail) {
    // found the page_id, return 1 to say it is true
    if ((*ptr).PageId == page_id) {
      frame_id = (*ptr).frameID;
      return true;
    }
    ptr++;
    at_tail = (ptr != slot->end());
  }
  // Added doubled_hashbuf April 2nd, 2020
  int doubled_hashbuf = 2 * hash_size;
  pos = (page_id) % (doubled_hashbuf); //key has to be new because size is now double
  if (pos <= hash_table.size()) {
    if (!hash_table[pos]) {return false;}
    slot = hash_table[pos];
    ptr = slot->begin();
    at_tail = (ptr != slot->end());
    // do another loop at this new slot to make sure we dont miss the double size hashing
    while (at_tail) {
      if ((*ptr).PageId == page_id)
      {
        frame_id = (*ptr).frameID;
        return true;
      }
      ptr++;
      at_tail = (ptr != slot->end());
    }
  }
  return false;
};

// delete the hastable we create and set everything back to their initial value
// so that we dont have overflow or gabbage collection
// Modified April 02, 2020
void clear_hash_table() {
  int key = 0;
  while (key < hash_table.size()){
    // only delete the records if key are there, not move on
    if (hash_table[key]) {
      hash_table[key]->~list<LL>();
      hash_table[key] = NULL;
      // buck->~list<LL>();
    }
    key++;
  }
  // reset all variable keep track of status of the hash table to default value 
  partion_id = 1;
  hash_size = HTSIZE + partion_id;
  depth = 2;
  next_id = 0;
}
/************************************End Global Helpers Definition***********************/
// Modified April 02, 2020 to passed the allocated and deallocated space for constructor
BufMgr::BufMgr(int numbuf, Replacer *replacer){
  // create new buffer page and Frame for buffer page
  // set buffer pools of frame to this bufFrame
  this->bufFrame = new FrameDesc[numbuf];
  // set buffer pools of page to this bufPage
  this->bufPool = new Page[numbuf];
  // set this to wrong
  this->numBuffers = -1; 
  is_buf_full = false;
  // clear all values in hash table 
  clear_hash_table();
  while (loved_queue.empty() == false){
    loved_queue.pop(); // empty all the queue
  }
  while (hated_stack.empty() == false) {
    hated_stack.pop();
    // empty all the previous populated screen
  }
  init_frame(-1);
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
// remove all the unused space for not being gabagge overflow
// Modifed April 02, 2020 to make sure it works.
BufMgr::~BufMgr() {
  if (this->numBuffers > 4294967200) { this->numBuffers++; }
  for (int frame_id = 0; frame_id <= this->numBuffers; frame_id++) {
    if (this->bufFrame[frame_id].is_clean != true) {
      continue;
    } else {
      // write to memmory the current framId and then remove the rest
      Page *dirty_page = new Page();
      memcpy(dirty_page, &this->bufPool[frame_id], sizeof(Page));
      MINIBASE_DB->write_page(this->bufFrame[frame_id].pageNo, dirty_page); //write disk
      // push to disk story.
      storage.push_back(this->bufFrame[frame_id].pageNo);
    }
  }
  // clear everything
  clear_hash_table();
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
// Modified April 02, 2020 all test past for 6
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage) {
  int frame_holder = 0;
  // search the page to be pinned
  bool found = search_frame(PageId_in_a_DB, frame_holder);
  if (!found && this->numBuffers == (NUMBUF - 1)) {
    int key = 0;
    if (hated_stack.empty() == false) {
      key = hated_stack.top();
      hated_stack.pop();
    } else if (loved_queue.empty() == false) {
       // if hate is empty the ncheck if love is also empty
      key = loved_queue.front();
      loved_queue.pop();
    }
    if (this->bufFrame[key].is_clean == true) {// if it is dirty , write to disk
      Page *dirty_page = new Page();
      memcpy(dirty_page, &this->bufPool[key], sizeof(Page));
      storage.push_back(PageId_in_a_DB);
      // if cannot write to disk the dirty page, print out
      if (MINIBASE_DB->write_page(this->bufFrame[key].pageNo, dirty_page) != OK) {
        cout << "Error: cannot write to disk" << endl;
      }
    }
    // removed the pin page
    remove_page(this->bufFrame[key].pageNo);

    Page *dirty_page = new Page();
    // if page is in disk, read it and past it to buf pool
    if (MINIBASE_DB->read_page(PageId_in_a_DB, dirty_page) == OK) {
      // copy it to bufPool
      memcpy(&this->bufPool[key], dirty_page, sizeof(Page));
      this->bufFrame[key].is_clean = false; // Frame is now dirty
      page = &this->bufPool[key]; // update variable page to the current page in buf poo;
      this->bufFrame[key].num_pin = 1; // now we pin one document here
      this->bufFrame[key].pageNo = PageId_in_a_DB;
      add_page(PageId_in_a_DB, key);
    }
    else { return FAIL; }
  }
  else if (this->numBuffers < (NUMBUF - 1) && search_frame(PageId_in_a_DB, frame_holder) == false || this->numBuffers > 4294967200) {
    Page *dirty_page = new Page();
    // read this page to buf pool
    if (MINIBASE_DB->read_page(PageId_in_a_DB, dirty_page) == OK) {
      this->numBuffers = this->numBuffers + 1;
      int curr_buff = this->numBuffers;
      // write dirt page to disk
      memcpy(&this->bufPool[curr_buff], dirty_page, sizeof(Page));
      // update page variable to hold new buf poo;l
      page = &this->bufPool[curr_buff]; 
      curr_buff = this->numBuffers;
      this->bufFrame[curr_buff].num_pin++;
      this->bufFrame[curr_buff].pageNo = PageId_in_a_DB; // update buffer to curretn PageId in DB
      this->bufFrame[curr_buff].is_clean = false; // it is not clear anymroe
      add_page(this->bufFrame[curr_buff].pageNo, curr_buff); // insert new page record into hash table
      if (this->numBuffers < (NUMBUF - 1)) { is_buf_full = false;}
      else {is_buf_full = true;}
    }
    else {
      return FAIL;
      add_page(PageId_in_a_DB,this->numBuffers); 
      this->numBuffers++;
      page=&this->bufPool[this->numBuffers];
      int curr_frame_id;
      this->bufFrame[curr_frame_id].is_clean=false;
    }
  }
  else if (search_frame(PageId_in_a_DB, frame_holder) == true) {
    this->bufFrame[frame_holder].num_pin++; // accumulate pin counts
    page = &this->bufPool[frame_holder];
  }
  else {
    cout << "Errors: this page isn't pinned yet" << endl;
    return FAIL;
  }
  return OK;
} //end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// Modifed April 03, 2020
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE) {
  int frame_id; // frame_id to search for pinned page in the Frame
  if (!search_frame(page_num, frame_id)) { // to check if we can find the frame with that page_id the buf pool
    return FAIL; // stop the page is already unpinned or not exist
    cout<<"cannot find the pinned page"<<endl;
  }
  else {
    int num_pins_curr_frame = this->bufFrame[frame_id].num_pin;
    if (num_pins_curr_frame == 0) {
      return FAIL; cout << "nothing to unpin even found the pin"<<endl;
    } 
    else {// can not pin a page which num_pin=0
      this->bufFrame[frame_id].num_pin--;
      this->bufFrame[frame_id].is_clean = dirty;
      num_pins_curr_frame = this->bufFrame[frame_id].num_pin;
      if (num_pins_curr_frame == 0) { // check the rest 
        if (!hate) { // where to push the dirt_page to after unpin
          loved_queue.push(frame_id);
        } 
        else { // hata and love replace policy
          hated_stack.push(frame_id);
        }
      }
    }
    
    return OK;
  }
  return OK; // put your code here
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
// Modified APril 04, 2020, all test passed 
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany) {
  Page *new_page = new Page(); //setting new Page
  int page_id;
  if (MINIBASE_DB->allocate_page(page_id, howmany) != OK) {
    return FAIL; // if cannot allocate from DB
  }
  MINIBASE_DB->read_page(page_id, new_page); // read the page
  if (!is_buf_full) {// buf pool not full simply add the page here
    pinPage(page_id, new_page, 1);
    // set firstPage to this after add new page to DB
    firstpage = new_page;
    firstPageId = page_id;
  }
  else { //full buf pool 
    if (MINIBASE_DB->deallocate_page(page_id, howmany) != OK) { //delete space for new space so we dont 
      // have gabbage collector error
      return FAIL;
    }
    return FAIL;
    cout << "No space on DB for this" << endl;
  }
  return OK; //put your code here
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
// Modified this all test passed
Status BufMgr::freePage(PageId globalPageId) {
  int frame_id; //initialize frame_id to find right frame given pageID
  if (search_frame(globalPageId, frame_id)) { //find the frame and assign that to frame_id
    if (!this->bufFrame[frame_id].num_pin) {
      for (int temp = frame_id + 1; temp <= this->numBuffers; temp++) {// free the rest and update the next file


        memcpy(&this->bufFrame[frame_id], &this->bufFrame[temp], sizeof(FrameDesc));
        frame_id++; temp++; // update current frame and frame pointer for next itr
      }
      // reduce pinned page number
      this->numBuffers--;
    }
    else {
      return FAIL;
      cout << "Return FAIL because cannot free this page" <<endl;
    }
  }
  if (MINIBASE_DB->deallocate_page(globalPageId) != OK) {//delete from disk
    return FAIL;
    cout << "ERROR: can not free this page" << globalPageId << endl;
  }
  return OK;// put your code here
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
// Modified April 03, 2020 passed test case 5
Status BufMgr::flushPage(PageId pageid) {
  // set the frame_id here so that we can find the page
  int frame_id;
  if (!search_frame(pageid, frame_id)) { // step-by-step: searching for frame, flush all the content, and write changes
    cout << "Error: cannot flush page_id =" << pageid << endl;
    return FAIL;
  } // if we can find some frame flush them all here
  else {
    Page *dirty_page  = new Page();
    memcpy(dirty_page, &this->bufPool[frame_id], sizeof(Page));
    //write changes to disk
    if (MINIBASE_DB->write_page(pageid, dirty_page) != OK) {
      cout << "Error: cannot write to disk " << endl;
      storage.push_back(pageid);
      return FAIL;
    } 
    else {
      storage.push_back(pageid);
      // put your code here
      return OK;
    }
  }
  return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
// Modified April 03, 2020. Passed test 6
Status BufMgr::flushAllPages() {
  // first we have to check if the numBuffers from this BufMrgs are Valid Buffers
  if (this->numBuffers > INT_MAX) { this->numBuffers++; }
  for (int frame_id = 0; frame_id <= this->numBuffers; frame_id++){
    if (this->bufFrame[frame_id].is_clean != true) {
      continue; //cout<<"it is clean here, nothingwrite page to disk"<<endl;
    } 
    else {
      Page *dirty_page = new Page(); // temporary diry page for flushing
      memcpy(dirty_page, &this->bufPool[frame_id], sizeof(Page)); // write to memmp
      if (MINIBASE_DB->write_page(this->bufFrame[frame_id].pageNo, dirty_page) != OK) { //write to this
        cout << "Error: cannot flush some Page " << "into to disk" << endl;
        storage.push_back(this->bufFrame[frame_id].pageNo);
        return FAIL;
      }
      else {
        storage.push_back(this->bufFrame[frame_id].pageNo); //write to disk
      }
    }
  }
  return OK; //put your code here
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename) {
  int max_numBuffer = NUMBUF - 1; //declare maximum numBuffers to validate numBuffers
  int frame_id; // create new frame_id to find the frame with the pageid to pin
  bool found_frame = search_frame(PageId_in_a_DB, frame_id);
  int index;
  if (!found_frame && this->numBuffers == max_numBuffer) {
    if (!loved_queue.empty()) { // loving policy
      index = loved_queue.front();
      loved_queue.pop();
    
    }
    else if (!hated_stack.empty()) {// hate policiy
      index = hated_stack.top();
      hated_stack.pop(); // pop the frame out for searching in the hated stack
      copy_stack.erase(copy_stack.end() - 1); //search for the frame to pin
      
    }
    bool is_clean = this->bufFrame[index].is_clean;
    if (is_clean) {
      Page *dirty_page = new Page();
      memcpy(dirty_page, &this->bufPool[index], sizeof(Page));
      
      if (MINIBASE_DB->write_page(this->bufFrame[index].pageNo, dirty_page) != OK) {//write changes
        cout << "Cannot write buf page " << this->bufFrame[index].pageNo << endl;
        storage.push_back(PageId_in_a_DB);
      } else {
        storage.push_back(PageId_in_a_DB);
      }
    }


    remove_page(this->bufFrame[index].pageNo); // remove from hash table
    Page *dirty_page = new Page();
    if (MINIBASE_DB->read_page(PageId_in_a_DB, dirty_page) != OK) {
      return FAIL;
      
    }
    else {
      memcpy(&this->bufPool[index], dirty_page, sizeof(Page));
      page = &this->bufPool[index];
      this->bufFrame[index].is_clean = false; //set clean to dirty
      this->bufFrame[index].pageNo = PageId_in_a_DB;
      this->bufFrame[index].num_pin = 1; // added one to numpin
    }

    add_page(PageId_in_a_DB, index); // insert new record into hash table
  }
  else if (this->numBuffers > 4294967200 || (!found_frame && this->numBuffers < max_numBuffer)) {
    Page *dirty_page = new Page();
    if (MINIBASE_DB->read_page(PageId_in_a_DB, dirty_page) == OK) {
      this->numBuffers++;
      if (emptyPage) {
        memcpy(&this->bufPool[this->numBuffers], dirty_page, sizeof(Page));
      }
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufFrame[this->numBuffers].num_pin++;
      this->bufFrame[this->numBuffers].is_clean = false;
      add_page(PageId_in_a_DB, this->numBuffers); 
      if (max_numBuffer > this->numBuffers){
        is_buf_full = 0; // full now,
      } else {
        is_buf_full = 1; //full now
      }
    }
    // if cant read just return Fail
    else {
      return FAIL;
    }
  }
  else if(!found_frame) {
    Page *dirty_page = new Page();
    MINIBASE_DB->read_page(PageId_in_a_DB,dirty_page);
    cout << "max test" << "pageid="  << PageId_in_a_DB << endl; // now do the max test
    // update numBuffers
    this->numBuffers++;
    int curr_numBuffer = this->numBuffers;
    page=&this->bufPool[curr_numBuffer];
    memcpy(&this->bufPool[this->numBuffers], dirty_page, sizeof(Page)); //write to memory
    curr_numBuffer = this->numBuffers; //update current numBuffers
    this->bufFrame[curr_numBuffer].is_clean = false; //set to dirty page
    this->bufFrame[curr_numBuffer].pageNo = PageId_in_a_DB;
    this->bufFrame[curr_numBuffer].num_pin++;
    add_page(PageId_in_a_DB,this->numBuffers); //add page to the hash table 
  }
  else if (found_frame) {
    // if nothing else works 
    this->bufFrame[frame_id].num_pin++; //increase the num_pin
    page = &this->bufPool[frame_id];
  }
  else {
    page=&this->bufPool[frame];      // allocate into buf
    this->bufFrame[frame].num_pin++;
    this->bufFrame[frame].is_clean = false; //not clean Frame so it will notify delete function
    this->bufFrame[frame].pageNo=PageId_in_a_DB;
  }
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// Fixed to make sure there is no overflow when unpin April 04 2020
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename) {
  // search for the page to unpin
  //initialize point to a frameid so that we can find it
  int frame; 
  if (search_frame(globalPageId_in_a_DB, frame)) {
    // if cant find the frame with that numpin >= 0 not thing to unpin
    // just return fail
    int pins = this->bufFrame[frame].num_pin;
    if (pins == 0) {
      return FAIL;
    }
    // if find any, then start unpin one at a time
    this->bufFrame[frame].num_pin--;
    pins = this->bufFrame[frame].num_pin;
    if (pins == 0 && find(copy_stack.begin(), copy_stack.end(), frame) == copy_stack.end()) {
      // put it to hated and copy stack for search purpose
      hated_stack.push(frame);
      copy_stack.push_back(frame);
    }
    return OK;
  }
  // if cannot find the page we need
  return FAIL;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
// Modified April 02, 2020 for unpining page test cases
unsigned int BufMgr::getNumUnpinnedBuffers() {
  // initialized cnt to return avalue 
  unsigned int cnt = 0;
  if (this->numBuffers > INT_MAX) { this->numBuffers++; } // to make sure the numBuffer value is valid
  // loop through frames in the buffers, and add number of pins to the count
  // these will be used in order to clear the hashtable and deconstruct BufMgr
  for (unsigned int frame_id = 0; frame_id <= this->numBuffers; frame_id++) {
    int curr_frame_pins = this->bufFrame[frame_id].num_pin;
    if (!curr_frame_pins) { cnt++; } // there are unpin pages, add them to cnt
  }
  return cnt; //put your code here
}