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
//Modified April 10, 2020
// int a = 1, b = 0;
int next_id = 0, depth = 2, flg_partion = 1, hash_max_size = HTSIZE + 1; // declare next_id, depth, flg_partition for tracking
vector<PageId> dsk_storage;
bool is_buf_full; // track whether buffer is full or not
vector<int> copy_stack; // create this so we can update loved, hated queue
stack<int> hate_queue; // stack to keep hate page
queue<int> love_stack; // queue to keep love page
vector<HL> hash_table(8, NULL); // declare hash_table to store value key pairs for hashing
/****************End GlobalVariables Declaration******************************/

/*******************************Global Helper Implementation*******************/
//Start Modifying April 10, 2020
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
    } else  { 
      int double_hash_size = 2*hash_max_size;// bigger , overflow or partiion
      if (next_id == pow(2, depth) * 2  - 1 || flg_partion) {// max_number for next iteration. Keep track of overflow
        if (next_id == pow(2, depth) * 2  - 1) { // full so we need to increase the size, and also depth
          depth++;
          hash_max_size = 2 * hash_max_size;
        } // parition when next equal to pow(2, depth) * 2  - 1
        flg_partion = 0; // first parition flag
        hash_table.resize(double_hash_size, NULL);
      }
      list<LL>::iterator itr = bucket->begin(); // start iterating through bucket.
      int id1 = ((page_no) % double_hash_size); // doube hash table and hashing for bucket id
      int id_parti;
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
      }
      else {
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
/*
Function: remove a pair from hash table
Paremeter: page number
Author: xing yuan
Return: void
*/
void remove_from_hash_table(int page)
{
  int index = (page) % hash_max_size;     //key    find in the no partion page
  list<LL> *buck = hash_table[index]; // get the buck
  list<LL>::iterator it = buck->begin();
  while (it != buck->end()) // find the element and remove it
  {
    if ((*it).PageId == page)
    {
      buck->erase(it);
      return;
    }
    it++;
  }

  index = (page) % (2 * hash_max_size); //key , find in the parition pages or overflow pages
  if (index <= hash_table.size())
  {
    buck = hash_table[index];
    it = buck->begin();
    while (it != buck->end()) // find and delete
    {
      if ((*it).PageId == page)
      {
        buck->erase(it);
        break;
      }
      it++;
    }
  }
}
/*
Function: find a frame number from hash table
Paremeter: pageid  page number ,  frameNo output vaules - frame number
Author: xing yuan
Data: 2017-10-19
Return: 1 success find  0 do not exit in the hash table
*/
int hashing(int pageID, int &frameNo)
{
  int index = (pageID) % hash_max_size; //key  find in the no partion page
  if (!hash_table[index])
    return 0;
  list<LL> *buck = hash_table[index];
  list<LL>::iterator it = buck->begin();
  while (it != buck->end())
  {
    if ((*it).PageId == pageID)
    {
      frameNo = (*it).frameID;
      return 1;
    }
    it++;
  }
  index = (pageID) % (2 * hash_max_size); //key
  if (index <= hash_table.size())           //key , find in the parition pages or overflow pages
  {
    if (!hash_table[index])
      return 0;
    buck = hash_table[index];
    it = buck->begin();
    while (it != buck->end())
    {
      if ((*it).PageId == pageID)
      {
        frameNo = (*it).frameID;
        return 1;
      }
      it++;
    }
  }
  return 0;
};
/*
Functiomn: clear hash table , destory buck
*/
void delete_pair() {
#if 1
  for (int index = 0; index < hash_table.size(); index++)
  {
    if (!hash_table[index])
      continue;
    list<LL> *buck = hash_table[index];
    hash_table[index] = NULL;
    buck->~list<LL>();
  }
  next_id = 0;
  depth = 2;
  flg_partion = 1;
  hash_max_size = HTSIZE + 1;
#endif

  // hash_table(7,NULL);
}
/*******************************End Global Helper Implementation***************/

/************************Start BufMgr Implementation *************************/
BufMgr::BufMgr(int numbuf, Replacer *replacer) {

  Page *BufPage = new Page[numbuf];
  FrameDesc *BufDesript = new FrameDesc[numbuf];
  this->bufPool = BufPage;
  this->bufFrame = BufDesript;
  this->numBuffers = -1; // init 0   it from biggest number ++ become 0
                         //   cout<<"bufmgr "<<this->numBuffers<<endl;
  while (!hate_queue.empty())
    hate_queue.pop();
  while (!love_stack.empty())
    love_stack.pop();
  delete_pair();
  init_frame(-1);
  is_buf_full = false;

  //    cout<<"bufmgr "<<this->numBuffers<<endl;
  // put your code here
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr()
{

  if (this->numBuffers > 4294967200)
    this->numBuffers++; // avoid numBuffers init value which always biggest int number from my test

  // cout<<"number of frame="<<this->numBuffers<<endl;
  int i = 0;
  while (i <= this->numBuffers)
  {
    if (this->bufFrame[i].is_clean == true)
    {
      //   cout<<"write page to disk"<<endl;
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      dsk_storage.push_back(this->bufFrame[i].pageNo);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }
    i++;
  }
  delete_pair();
  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage) {
  int frame;
  //  cout<<"frame number  pin without filename"<<this->numBuffers<<"page number="<<PageId_in_a_DB<<endl;
  if (!hashing(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1)) //page  not in the buf pool and buf pool full
  {
    int i = 0;
    if (!hate_queue.empty()) // love and hate replace policy
    {
      i = hate_queue.top(); //MRU.  use stack
      hate_queue.pop();
    }
    else if (!love_stack.empty())
    {

      i = love_stack.front(); // LRU   use queue
      love_stack.pop();
    }
    if (this->bufFrame[i].is_clean == true) // if it is dirty , write to disk
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      dsk_storage.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }

    remove_from_hash_table(this->bufFrame[i].pageNo); // remove from hash table

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

    build_hash_table(PageId_in_a_DB, i); // insert new page record into hash table
  }
  else if (!hashing(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1) || this->numBuffers > 4294967200) // page not in the buf pool, not full
  {
    //  if(this->numBuffers>4294967200) cout<<"biggerst number enter " <<endl;
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace); // read this page to buf pool
    if (buf_read == OK)
    {
      this->numBuffers++;
      if (emptyPage)
        memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufFrame[this->numBuffers].num_pin++;
      this->bufFrame[this->numBuffers].is_clean = false;
      build_hash_table(PageId_in_a_DB, this->numBuffers); // insert new page record into hash table
      if (this->numBuffers == (NUMBUF - 1))
        is_buf_full = true; // buf pool full
    }
    else
    {
      //detect a error code
      // cout<<"Error: can not read page from disk"<<endl;
      return FAIL;
      /*   this->numBuffers++;
              page=&this->bufPool[this->numBuffers];      // allocate into buf
             this->bufFrame[this->numBuffers].pageNo=PageId_in_a_DB;
             this->bufFrame[this->numBuffers].num_pin++;
             this->bufFrame[this->numBuffers].is_clean=false;
              build_hash_table(PageId_in_a_DB,this->numBuffers);   // insert
              */
    }
  }
  else if (hashing(PageId_in_a_DB, frame)) // in the buf pool , pin ++
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
  if (hashing(page_num, frameid)) // in the buf pool
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
        love_stack.push(frameid);
      } // hata and love replace policy
      else
      {
        hate_queue.push(frameid);
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
  //   firstPageId=allocate_page;
  //   firstpage=new_page;
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId)
{
  int frame;
  if (hashing(globalPageId, frame)) // find frame no and free it
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
  if (hashing(pageid, frameid)) // find frame no , and flush it , write it to disk
  {
    Page *replace = new Page();
    memcpy(replace, &this->bufPool[frameid], sizeof(Page));
    Status buf_write = MINIBASE_DB->write_page(pageid, replace); //write disk
    dsk_storage.push_back(pageid);
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
  if (this->numBuffers > 4294967200)
    this->numBuffers++; // avoid numBuffers init value which always biggest int number from my test
  while (i <= this->numBuffers)
  {
    if (this->bufFrame[i].is_clean == true)
    {
      //   cout<<"write page to disk"<<endl;
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      dsk_storage.push_back(this->bufFrame[i].pageNo);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }
    i++;
  }

  //put your code here
  return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{

  int frame;
  //  cout<<"file pin pade id"<<PageId_in_a_DB<<" frame "<<this->numBuffers<<endl;

  if (!hashing(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1))
  {

    int i;
    if (!hate_queue.empty())
    {
      i = hate_queue.top(); // file pin and unpin all use Hate replace policy
                             //     cout<<"size of hate "<<hate_queue.size()<<" i="<<i<<endl;
      hate_queue.pop();
      copy_stack.erase(copy_stack.end() - 1); // use a copy stack for search frame no in order to not let same frame to push into stack
    }
    else if (!love_stack.empty()) // miss love policy , no any paremeter indicate love may be in the future test case
    {

      i = love_stack.front();
      love_stack.pop();
    }
    // following code same as above pin function
    if (this->bufFrame[i].is_clean == true)
    {
      Page *replace = new Page();
      memcpy(replace, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufFrame[i].pageNo, replace); //write disk
      dsk_storage.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        cout << "Error: write buf page " << this->bufFrame[i].pageNo << "into to disk" << endl;
    }

    remove_from_hash_table(this->bufFrame[i].pageNo); // remove from hash table
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
    else
    {
      cout << "Fata error: can not read page in the disk" << endl;
      return FAIL;
    }

    build_hash_table(PageId_in_a_DB, i); // insert new record into hash table
  }

#if 1
  else if ((!hashing(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1)) || this->numBuffers > 4294967200)
  {
    //  if(this->numBuffers>4294967200) cout<<"biggerst number enter  pageid= " <<PageId_in_a_DB<<endl;
    Page *replace = new Page();
    Status buf_read = MINIBASE_DB->read_page(PageId_in_a_DB, replace);
    if (buf_read == OK)
    {
      this->numBuffers++;
      if (emptyPage) // if it is a empty page , do not need to copy it from the disk
        memcpy(&this->bufPool[this->numBuffers], replace, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; // allocate into buf
      this->bufFrame[this->numBuffers].pageNo = PageId_in_a_DB;
      this->bufFrame[this->numBuffers].num_pin++;
      this->bufFrame[this->numBuffers].is_clean = false;
      build_hash_table(PageId_in_a_DB, this->numBuffers); // insert into hash table
                                                    // cout<<"page "<<PageId_in_a_DB<<" num_pin "<<this->bufFrame[this->numBuffers].num_pin<<endl;
      if (this->numBuffers == (NUMBUF - 1))
        is_buf_full = true;
    }
    else
    {
      //detect a error code
      cout << "Error: can not read page from disk" << endl;
      return FAIL;
    }
  }
  else if (hashing(PageId_in_a_DB, frame))
  {
    this->bufFrame[frame].num_pin++;
    page = &this->bufPool[frame];
  }
  else
    cout << "can not pin this page  " << endl;

#endif

#if 0
         else if(!hashing(PageId_in_a_DB,frame))
          {
             cout<<"max test"<<"pageid="<<PageId_in_a_DB<<endl;
               Page *replace=new Page();
              Status buf_read=MINIBASE_DB->read_page(PageId_in_a_DB,replace);

            this->numBuffers++;
            page=&this->bufPool[this->numBuffers];
            memcpy(&this->bufPool[this->numBuffers],replace,sizeof(Page));
            this->bufFrame[this->numBuffers].pageNo=PageId_in_a_DB;
            this->bufFrame[this->numBuffers].num_pin++;
            this->bufFrame[this->numBuffers].is_clean=false;
            build_hash_table(PageId_in_a_DB,this->numBuffers); 
          }
         else 
         {
          //  this->numBuffers++;
             page=&this->bufPool[frame];      // allocate into buf
            this->bufFrame[frame].pageNo=PageId_in_a_DB;
            this->bufFrame[frame].num_pin++;
            this->bufFrame[frame].is_clean=false;
           // build_hash_table(PageId_in_a_DB,this->numBuffers);   // insert into hash table
         }
#endif

  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename)
{

  int frameid;
  if (hashing(globalPageId_in_a_DB, frameid)) // find page frame id and unpin it
  {

    if (this->bufFrame[frameid].num_pin == 0)
    {
      cout << "unpind page_cnt=0. pagde id=" << globalPageId_in_a_DB << endl;
      return FAIL;
    }
    this->bufFrame[frameid].num_pin--;
    if (this->bufFrame[frameid].num_pin == 0 && find(copy_stack.begin(), copy_stack.end(), frameid) == copy_stack.end())
    {
      hate_queue.push(frameid);     // Hate policy
      copy_stack.push_back(frameid); // copy a stack for seach
    }
    //    cout<<"unpin file "<<globalPageId_in_a_DB<<" num_pin"<<this->bufFrame[frameid].num_pin<<endl;
  }
  else
  {
    // cout<<"can not find the page in the buf pool pageid="<<globalPageId_in_a_DB<<endl;
    return FAIL;
  }

  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers()
{

  int i = 0;
  int count = 0;
  if (this->numBuffers > 4294967200)
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
