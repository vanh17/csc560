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
vector<PageId> disk_page;
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
BufMgr::~BufMgr() {
  if (this->numBuffers > 4294967200) { this->numBuffers++; }
  for (int frame_id = 0, frame_id <= this->numBuffers, frame_id++) {
    if (this->bufFrame[frame_id].is_clean == true) {
      // write to memmory the current framId and then remove the rest
      memcpy(new Page();, &this->bufPool[frame_id], sizeof(Page));
      MINIBASE_DB->write_page(this->bufFrame[frame_id].pageNo, replace); //write disk
      disk_page.push_back(this->bufFrame[frame_id].pageNo);
    }
  }
  // clear everything
  clear_hash_table();
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage)
{
  int frame;
  //  cout<<"frame number  pin without filename"<<this->numBuffers<<"page number="<<PageId_in_a_DB<<endl;
  if (!search_frame(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1)) //page  not in the buf pool and buf pool full
  {
    int i = 0;
    if (!hated_stack.empty()) // love and hate replace policy
    {
      i = hated_stack.top(); //MRU.  use stack
      hated_stack.pop();
    }
    else if (!loved_queue.empty())
    {

      i = loved_queue.front(); // LRU   use queue
      loved_queue.pop();
    }
    if (this->bufFrame[i].is_clean == true) // if it is dirty , write to disk
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      disk_page.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }

    remove_page(this->bufFrame[i].pageNo); // remove from hash table

    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace); // read page from disk , copy it to the buf pool
    if (buf_read == OK)
    {
      memcpy(&this->bufPool[i], replace, sizeof(Page));
      page = &this->bufPool[i];
      this->bufFrame[i].pageNo = PageId_in_a_DB;
      this->bufFrame[i].num_pin = 1;
      this->bufFrame[i].is_clean = false;
    }
    else
    {
      //   cout<<"Fata error: can not read page in the disk"<<endl;      // disk do not have page , fata error
      return FAIL;
    }

    add_page(PageId_in_a_DB, i); // insert new page record into hash table
  }
  else if (!search_frame(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1) || this->numBuffers > 4294967200) {
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace); // read this page to buf pool
    if (buf_read == OK)
    {
      this->numBuffers++;
      memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufFrame[this->numBuffers].num_pin++;
      this->bufFrame[this->numBuffers].is_clean = false;
      add_page(PageId_in_a_DB, this->numBuffers); // insert new page record into hash table
      if (this->numBuffers == (NUMBUF - 1))
        is_buf_full = true; // buf pool full
    }
    else {
      return FAIL;
      /*   this->numBuffers++;
              page=&this->bufPool[this->numBuffers];      // allocate into buf
             this->bufFrame[this->numBuffers].pageNo=PageId_in_a_DB;
             this->bufFrame[this->numBuffers].num_pin++;
             this->bufFrame[this->numBuffers].is_clean=false;
              add_page(PageId_in_a_DB,this->numBuffers);   // insert
              */
    }
  }
  else if (search_frame(PageId_in_a_DB, frame)) // in the buf pool , pin ++
  {
    this->bufFrame[frame].num_pin++;
    page = &this->bufPool[frame];
  }
  else
    cout << "can not pin this page  " << endl;
  // put your code here
  return OK;
} //end pinPage
//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE)
{

  int frameid;
  if (search_frame(page_num, frameid)) // in the buf pool
  {
    if (this->bufFrame[frameid].num_pin == 0)
    {
      return FAIL;
    } // can not pin a page which num_pin=0
    this->bufFrame[frameid].num_pin--;
    this->bufFrame[frameid].is_clean = dirty;
    if (this->bufFrame[frameid].num_pin == 0)
    {
      if (hate == FALSE)
      {
        loved_queue.push(frameid);
      } // hata and love replace policy
      else
      {
        hated_stack.push(frameid);
      }
    }
    //   cout<<"unpin a page  pageid="<<page_num<<endl;
  }
  else
  {
    //   cout<<"can not find the page in the buf pool pageid="<<page_num<<endl;
    return FAIL;
  }
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany)
{
  int allocate_page;
  Page *new_page = new Page();
  Status allocte, get_new, deallocte, pin;
  howmany = 1;
  allocte = MINIBASE_DB->allocate_page(allocate_page, howmany); // allocate a page
  if (allocte != OK)
    cout << "Error: can not allocate a page from DB" << endl;
  get_new = MINIBASE_DB->read_page(allocate_page, new_page);
  //  if(this->numBuffers>=(NUMBUF-1))
  if (is_buf_full) // if buf pool is full , dellocate it
  {
    deallocte = MINIBASE_DB->deallocate_page(allocate_page, howmany);
    if (deallocte != OK)
      cout << "Fata Error: can not dellocate page  pageid" << allocate_page << endl;
    return FAIL;
  }
  else
  {
    pin = pinPage(allocate_page, new_page, 1); // not full, pin it
    firstPageId = allocate_page;
    firstpage = new_page;
  }
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId)
{
  int frame;
  if (search_frame(globalPageId, frame)) // find frame no and free it
  {
    if (this->bufFrame[frame].num_pin)
      return FAIL;
    else
    {
      int i = frame + 1;
      while (i <= this->numBuffers) // move forward to fill the gap
      {

        memcpy(&this->bufFrame[frame], &this->bufFrame[i], sizeof(FrameDesc));
        frame++;
        i++;
      }
      this->numBuffers--;
    }
  }
  Status deallocte = MINIBASE_DB->deallocate_page(globalPageId); // deloocate it from the disk
  if (deallocte != OK)
    cout << "ERROR: can not dellocate a page  pageid=" << globalPageId << endl;

  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid)
{

  int frameid;
  if (search_frame(pageid, frameid)) // find frame no , and flush it , write it to disk
  {
    Page *replace = new Page();
    memcpy(replace, &this->bufPool[frameid], sizeof(Page));
    Status buf_write = MINIBASE_DB->write_page(pageid, replace); //write disk
    disk_page.push_back(pageid);
    if (buf_write != OK)
    {
      cout << "Error: write buf page " << this->bufFrame[frameid].pageNo << "into to disk" << endl;
      return FAIL;
    }
  }
  else
    cout << "can not find the page in the buf pool pageid=" << pageid << endl;

  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages()
{

  //flush all  , write all to disk

  int i = 0;
  if (this->numBuffers > INT_MAX) { this->numBuffers++; }
  while (i <= this->numBuffers){
    if (this->bufFrame[i].is_clean == true) {
      //   cout<<"write page to disk"<<endl;
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      disk_page.push_back(this->bufFrame[i].pageNo);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }
    i++;
  }
  return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{

  int frame;
  if (!search_frame(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1))
  {

    int i;
    //    cout<<"size of hate "<<hated_stack.size()<<endl;
    if (!hated_stack.empty())
    {
      i = hated_stack.top(); // file pin and unpin all use Hate replace policy
                             //     cout<<"size of hate "<<hated_stack.size()<<" i="<<i<<endl;
      hated_stack.pop();
      copy_stack.erase(copy_stack.end() - 1); // use a copy stack for search frame no in order to not let same frame to push into stack
    }
    else if (!loved_queue.empty()) // miss love policy , no any paremeter indicate love may be in the future test case
    {

      i = loved_queue.front();
      loved_queue.pop();
    }
    // following code same as above pin function
    if (this->bufFrame[i].is_clean == true)
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      disk_page.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }

    remove_page(this->bufFrame[i].pageNo); // remove from hash table
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      memcpy(&this->bufPool[i], replace, sizeof(Page));
      page = &this->bufPool[i];
      this->bufFrame[i].pageNo = PageId_in_a_DB;
      this->bufFrame[i].num_pin = 1;
      this->bufFrame[i].is_clean = false;
    }
    else {
      return FAIL;
    }

    add_page(PageId_in_a_DB, i); // insert new record into hash table
  }
  else if ((!search_frame(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1)) || this->numBuffers > 4294967200) {
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK) {
      this->numBuffers++;
      if (emptyPage) {
        memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      }
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufFrame[this->numBuffers].num_pin++;
      this->bufFrame[this->numBuffers].is_clean = false;
      add_page(PageId_in_a_DB, this->numBuffers); // insert into hash table
                                                    // cout<<"page "<<PageId_in_a_DB<<" num_pin "<<this->bufFrame[this->numBuffers].num_pin<<endl;
      if (this->numBuffers == (NUMBUF - 1)){
        is_buf_full = 1;
      }
    }
    // if cant read just return Fail
    else {
      return FAIL;
    }
  }
  else if (search_frame(PageId_in_a_DB, frame)) {
    this->bufFrame[frame].num_pin++;
    page = &this->bufPool[frame];
  }
  else if(!search_frame(PageId_in_a_DB,frame)) {
    cout<<"max test"<<"pageid="<<PageId_in_a_DB<<endl;
    Page *replace=new Page();
    Status buf_read=MINIBASE_DB->read_page(PageId_in_a_DB,replace);

    this->numBuffers++;
    page=&this->bufPool[this->numBuffers];
    memcpy(&this->bufPool[this->numBuffers],replace,sizeof(Page));
    this->bufFrame[this->numBuffers].pageNo=PageId_in_a_DB;
    this->bufFrame[this->numBuffers].num_pin++;
    this->bufFrame[this->numBuffers].is_clean = false;
    add_page(PageId_in_a_DB,this->numBuffers); 
  }
  else {
    page=&this->bufPool[frame];      // allocate into buf
    this->bufFrame[frame].pageNo=PageId_in_a_DB;
    // add_page(PageId_in_a_DB,this->numBuffers);   // insert into hash table
    this->bufFrame[frame].num_pin++;
    //  this->numBuffers++;
    this->bufFrame[frame].is_clean = false;
    
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
    if (this->bufFrame[frame].num_pin == 0) {
      return FAIL;
    }
    // if find any, then start unpin one at a time
    this->bufFrame[frame].num_pin--;
    if (this->bufFrame[frame].num_pin == 0 && find(copy_stack.begin(), copy_stack.end(), frame) == copy_stack.end()) {
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
unsigned int BufMgr::getNumUnpinnedBuffers()
{

  int i = 0;
  int count = 0;
  if (this->numBuffers > INT_MAX)
    this->numBuffers++;
  while (i <= this->numBuffers)
  {
    if (!this->bufFrame[i].num_pin) // cout total unpin page
      count++;
    i++;
  }
  //put your code here
  return count;
}