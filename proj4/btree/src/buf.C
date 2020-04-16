#include "buf.h"
#include <list>
#include <vector>
#include <algorithm>
#include <stack>
#include <deque>
#include <queue>
#include <math.h>
void build_current_hash_table(PageId PageNo, int frameNo);
void remove_current_hash(int page);
int find_hash_from_table(int pageID, int &frameNo);


typedef struct hashNodeList
{
  int PageId;
  int frameID;
} * List;

typedef list<hashNodeList> *linkedlistHash;
#define INT_MAX 4294967100
#define muss_bu_ckSize 2
vector<linkedlistHash> hash_table(8, NULL);

int Next = 0, level = 2;



vector<int> stack_reserved;
int bit_for_part = 1;
int hashbuf = HTSIZE + 1;

vector<PageId> page_in_disk;
stack<int> fram_dirty_hate;
queue<int> fram_pure_love;

int bit_for_indicate_buffer_full;

int a = 1, b = 0;
static const char *bufErrMsgs[] = {
    
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
//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr(int numbuf, Replacer  *replacer)
{

  Page *muss_buff_page = new Page[numbuf];
  FrameDesc *muss_buff_descr = new FrameDesc[numbuf];
  this->bufPool = muss_buff_page;
  this->bufDescr = muss_buff_descr;
  this->numBuffers = -1; 
  while (!fram_dirty_hate.empty())
    fram_dirty_hate.pop();
  while (!fram_pure_love.empty())
    fram_pure_love.pop();
  //delete_table();
    for (int index = 0; index < hash_table.size(); index++)
  {
    if (!hash_table[index])
      continue;
    list<hashNodeList> *muss_bu_ck = hash_table[index];
    hash_table[index] = NULL;
    muss_bu_ck->~list<hashNodeList>();
  }
  Next = 0;
  level = 2;
  bit_for_part = 1;
  hashbuf = HTSIZE + 1;


  init_frame(-1);
  bit_for_indicate_buffer_full = 0;

}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr()
{

  if (this->numBuffers > INT_MAX)
    this->numBuffers++; 
  
  int i = 0;
  while (i <= this->numBuffers)
  {
    if (this->bufDescr[i].dirtybit == true)
    {
  
      Page *page_replaced = new Page();
      memcpy(page_replaced, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, page_replaced); 
      page_in_disk.push_back(this->bufDescr[i].pageNo);
      if (buf_write != OK)
        {}
    }
    i++;
  }
  //delete_table();
    for (int index = 0; index < hash_table.size(); index++)
  {
    if (!hash_table[index])
      continue;
    list<hashNodeList> *muss_bu_ck = hash_table[index];
    hash_table[index] = NULL;
    muss_bu_ck->~list<hashNodeList>();
  }
  Next = 0;
  level = 2;
  bit_for_part = 1;
  hashbuf = HTSIZE + 1;
  // put your code here
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage)
{
  int frame;
    if (!find_hash_from_table(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1)) 
  {
    int i = 0;
    if (!fram_dirty_hate.empty())
    {
      i = fram_dirty_hate.top(); 
      fram_dirty_hate.pop();
    }
    else if (!fram_pure_love.empty())
    {

      i = fram_pure_love.front(); 
      fram_pure_love.pop();
    }
    if (this->bufDescr[i].dirtybit == true)
    {
      Page *page_replaced = new Page();
      memcpy(page_replaced, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, page_replaced); 
      page_in_disk.push_back(PageId_in_a_DB);
      if (buf_write != OK)
        {}
    }

    remove_current_hash(this->bufDescr[i].pageNo); 

    Page *page_replaced = new Page();
    Status func_read_buffer_status = MINIBASE_DB->read_page(PageId_in_a_DB, page_replaced); 
    if (func_read_buffer_status == OK)
    {
      memcpy(&this->bufPool[i], page_replaced, sizeof(Page));
      page = &this->bufPool[i];
      this->bufDescr[i].pageNo = PageId_in_a_DB;
      this->bufDescr[i].numberofPins = 1;
      this->bufDescr[i].dirtybit = false;
    }
    else
    {
            return FAIL;
    }

    build_current_hash_table(PageId_in_a_DB, i); 
  }
  else if (!find_hash_from_table(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1) || this->numBuffers > INT_MAX) 
  {
       Page *page_replaced = new Page();
    Status func_read_buffer_status = MINIBASE_DB->read_page(PageId_in_a_DB, page_replaced); 
    if (func_read_buffer_status == OK)
    {
      this->numBuffers++;
      memcpy(&this->bufPool[this->numBuffers], page_replaced, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; 
      this->bufDescr[this->numBuffers].numberofPins++;
      this->bufDescr[this->numBuffers].pageNo = PageId_in_a_DB;
      
      this->bufDescr[this->numBuffers].dirtybit = false;
      build_current_hash_table(PageId_in_a_DB, this->numBuffers);
      if (this->numBuffers == (NUMBUF - 1))
        bit_for_indicate_buffer_full = 1; 
    }
    else
    {

      return FAIL;
   
    }
  }
  else if (find_hash_from_table(PageId_in_a_DB, frame)) 
  {
    this->bufDescr[frame].numberofPins++;
    page = &this->bufPool[frame];
  }
 

  return OK;
} 


int find_hash_from_table(int pageID, int &frameNo)
{
  int index = (a * pageID + b) % hashbuf; 
  if (!hash_table[index])
    return 0;
  list<hashNodeList> *muss_bu_ck = hash_table[index];
  list<hashNodeList>::iterator it = muss_bu_ck->begin();
  while (it != muss_bu_ck->end())
  {
    if ((*it).PageId == pageID)
    {
      frameNo = (*it).frameID;
      return 1;
    }
    it++;
  }
  index = (a * pageID + b) % (2 * hashbuf); 
  if (index <= hash_table.size())          
  {
    if (!hash_table[index])
      return 0;
    muss_bu_ck = hash_table[index];
    it = muss_bu_ck->begin();
    while (it != muss_bu_ck->end())
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


void build_current_hash_table(PageId PageNo, int frameNo)
{

  int Max_next = muss_bu_ckSize * pow(2, level) - 1; 
  int index = (a * PageNo + b) % hashbuf;      
  hashNodeList frame;  
  frame.frameID = frameNo;                            
  frame.PageId = PageNo;
  
  if (frame.PageId == 1)
  {
    int save = frame.PageId;
    frame.PageId = 2;
    frame.PageId = save;
  }

  if (!hash_table[index]) 
  {
    list<hashNodeList> *muss_bu_ck = new list<hashNodeList>;
    muss_bu_ck->push_back(frame);
    hash_table[index] = muss_bu_ck; 
  }
  else 
  {

    list<hashNodeList> *muss_bu_ck = hash_table[index];
    if (muss_bu_ck->size() < muss_bu_ckSize) 
      muss_bu_ck->push_back(frame);    
    else                        
    {
      if (bit_for_part || Max_next == Next)
      {
        if (Max_next == Next)
        {
          level++;
          hashbuf = 2 * hashbuf;
        } 
        hash_table.resize(2 * (hashbuf), NULL);
        bit_for_part = 0; 
      }
      int hash_size = (hashbuf)*2;              
      int index1 = (a * PageNo + b) % hash_size; 
      int partion_index;
      list<hashNodeList>::iterator it = muss_bu_ck->begin();
      if (index1 <= Next) 
      {
        int overflow = 0;
        while (it != muss_bu_ck->end())
        {
        
          partion_index = (*it).PageId;
          partion_index = (a * partion_index + b) % hash_size; 
          if (index != partion_index)                         
          {
            hashNodeList frame1;
            frame1.frameID = (*it).frameID;
            frame1.PageId = (*it).PageId;
            
            if (!hash_table[partion_index]) 
            {
              list<hashNodeList> *muss_bu_ck1 = new list<hashNodeList>;
              muss_bu_ck1->push_back(frame1);
              hash_table[partion_index] = muss_bu_ck1;
            }
            else
              hash_table[partion_index]->push_back(frame1); 
            it = muss_bu_ck->erase(it);                          
            overflow = 1;                                   
          }

          it++;
        }

        if (!hash_table[index1]) 
        {
          list<hashNodeList> *muss_bu_ck2 = new list<hashNodeList>;
          muss_bu_ck2->push_back(frame);
          hash_table[index1] = muss_bu_ck2;
        }
        else
          hash_table[index1]->push_back(frame);
        if (!overflow) 
          Next++;    
      }
      else
      {
        muss_bu_ck->push_back(frame); 
      }
    }
  }
}



void remove_current_hash(int page)
{
  int index = (a * page + b) % hashbuf;     
  list<hashNodeList> *muss_bu_ck = hash_table[index]; 
  list<hashNodeList>::iterator it = muss_bu_ck->begin();
  while (it != muss_bu_ck->end()) 
  {
    if ((*it).PageId == page)
    {
      muss_bu_ck->erase(it);
      return;
    }
    it++;
  }

  index = (a * page + b) % (2 * hashbuf); 
  if (index <= hash_table.size())
  {
    muss_bu_ck = hash_table[index];
    it = muss_bu_ck->begin();
    while (it != muss_bu_ck->end()) 
    {
      if ((*it).PageId == page)
      {
        muss_bu_ck->erase(it);
        break;
      }
      it++;
    }
  }
}



//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE)
{

  int frameid;
  if (find_hash_from_table(page_num, frameid)) 
  {
    if (this->bufDescr[frameid].numberofPins == 0)
    {
      return FAIL;
    }
    
    this->bufDescr[frameid].dirtybit = dirty;

    this->bufDescr[frameid].numberofPins--;


    if (this->bufDescr[frameid].numberofPins == 0)
    {
      if (hate == FALSE)
      {
        fram_pure_love.push(frameid);
      } 
      else
      {
        fram_dirty_hate.push(frameid);
      }
    }
   
  }
  else
  {
   
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
  Status bit_allocate_page, get_new, bit_de_allocate_page, pin;
  howmany = 1;
  bit_allocate_page = MINIBASE_DB->allocate_page(allocate_page, howmany); 

  get_new = MINIBASE_DB->read_page(allocate_page, new_page);
  
  if (bit_for_indicate_buffer_full) 
  {
    bit_de_allocate_page = MINIBASE_DB->deallocate_page(allocate_page, howmany);
    
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
  if (find_hash_from_table(globalPageId, frame)) 
  {
    if (this->bufDescr[frame].numberofPins)
      return FAIL;
    else
    {
      int i = frame + 1;
      while (i <= this->numBuffers) 
      {

        memcpy(&this->bufDescr[frame], &this->bufDescr[i], sizeof(FrameDesc));
        frame++;
        i++;
      }
      this->numBuffers--;
    }
  }
  Status bit_de_allocate_page = MINIBASE_DB->deallocate_page(globalPageId); // deloocate it from the disk
  
  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid)
{

  int frameid;
  if (find_hash_from_table(pageid, frameid)) 
  {
    Page *page_replaced = new Page();
    memcpy(page_replaced, &this->bufPool[frameid], sizeof(Page));
    Status buf_write = MINIBASE_DB->write_page(pageid, page_replaced); 
    page_in_disk.push_back(pageid);
    if (buf_write != OK)
    {
      
      return FAIL;
    }
  }
 

  // put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages()
{


  int i = 0;
  if (this->numBuffers > INT_MAX)
    this->numBuffers++; 
  while (i <= this->numBuffers)
  {
    if (this->bufDescr[i].dirtybit == true)
    {
      //   cout<<"write page to disk"<<endl;
      Page *page_replaced = new Page();
      memcpy(page_replaced, &this->bufPool[i], sizeof(Page));//write disk
      page_in_disk.push_back(this->bufDescr[i].pageNo);
    }
    i++;
  }

  //put your code here
  return OK;
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{

  int frame;
 
  if (!find_hash_from_table(PageId_in_a_DB, frame) && this->numBuffers == (NUMBUF - 1))
  {

    int i;
  
    if (!fram_dirty_hate.empty())
    {
      i = fram_dirty_hate.top(); 
                           
      fram_dirty_hate.pop();
      stack_reserved.erase(stack_reserved.end() - 1); 
    }
    else if (!fram_pure_love.empty())
    {

      i = fram_pure_love.front();
      fram_pure_love.pop();
    }

    if (this->bufDescr[i].dirtybit == true)
    {
      Page *page_replaced = new Page();
      memcpy(page_replaced, &this->bufPool[i], sizeof(Page));
      Status buf_write = MINIBASE_DB->write_page(this->bufDescr[i].pageNo, page_replaced); //write disk
      page_in_disk.push_back(PageId_in_a_DB);
     
    }

    remove_current_hash(this->bufDescr[i].pageNo); 
    Page *page_replaced = new Page();
    Status func_read_buffer_status = MINIBASE_DB->read_page(PageId_in_a_DB, page_replaced);
    if (func_read_buffer_status == OK)
    {
      memcpy(&this->bufPool[i], page_replaced, sizeof(Page));
      page = &this->bufPool[i];
      this->bufDescr[i].numberofPins = 1;
      this->bufDescr[i].pageNo = PageId_in_a_DB;
      
      this->bufDescr[i].dirtybit = false;
    }
    else
    {
      
      return FAIL;
    }

    build_current_hash_table(PageId_in_a_DB, i); 
  }

#if 1
  else if ((!find_hash_from_table(PageId_in_a_DB, frame) && this->numBuffers < (NUMBUF - 1)) || this->numBuffers > INT_MAX)
  {
    Page *page_replaced = new Page();
    Status func_read_buffer_status = MINIBASE_DB->read_page(PageId_in_a_DB, page_replaced);
    if (func_read_buffer_status == OK)
    {
      this->numBuffers++;
      if (emptyPage)
        memcpy(&this->bufPool[this->numBuffers], page_replaced, sizeof(Page));
      page = &this->bufPool[this->numBuffers]; 
      int mussBuff = this->numBuffers;

      this->bufDescr[mussBuff].pageNo = PageId_in_a_DB;
      this->bufDescr[mussBuff].numberofPins++;
      this->bufDescr[mussBuff].dirtybit = false;
      build_current_hash_table(PageId_in_a_DB, mussBuff);
                                                    
      if (this->numBuffers == (NUMBUF - 1))
        bit_for_indicate_buffer_full = 1;
    }
    else
    {
    
      return FAIL;
    }
  }
  else if (find_hash_from_table(PageId_in_a_DB, frame))
  {
    this->bufDescr[frame].numberofPins++;
    page = &this->bufPool[frame];
  }
  
    //   print_hash();

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
  if (find_hash_from_table(globalPageId_in_a_DB, frameid)) // find page frame id and unpin it
  {
    int mussPins = this->bufDescr[frameid].numberofPins;
    if ( mussPins== 0)
    {
      
      return FAIL;
    }
    this->bufDescr[frameid].numberofPins--;
    if (this->bufDescr[frameid].numberofPins == 0 && find(stack_reserved.begin(), stack_reserved.end(), frameid) == stack_reserved.end())
    {
      fram_dirty_hate.push(frameid);     
      stack_reserved.push_back(frameid); 
    }
    
  }
  else
  {
  
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
  if (this->numBuffers > INT_MAX)
    this->numBuffers++;
  while (i <= this->numBuffers)
  {
    if (!this->bufDescr[i].numberofPins) 
      count++;
    i++;
  }
  //put your code here
  return count;
}