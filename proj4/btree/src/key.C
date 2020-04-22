/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
// Hoang
int keyCompare(const void *key1, const void *key2, AttrType t) {
    int result;
    bool first_condition = ((t == attrInteger) && (t != attrString));
    
    bool second_condition = ((t != attrInteger) && (t == attrString));
    
    if (first_condition) {  
        int * int1= (int *)key1; int *int2 = (int *)key2;

        if (*int1 < *int2) {
            result =  -1;
        }
        if (*int1 > *int2) {
            result =  1;
        }
        return result;
    }
    else if (second_condition) {
        char *char1 = (char *)key1; char *char2 = (char *)key2;

        if (strcmp(char1, char2) > 0) {
            result = 1;
        }
        else if (strcmp(char1, char2) < 0)
            result = -1;
        return result;
    } else { // not string, or integer, just return a value to track the error
        return -100;
    }
    return -2000;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
// Hoang
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len) {
    bool first_condition = ((key_type == attrInteger) && (key_type != attrString));
     int checker_for_entry;
    bool second_condition = ((key_type != attrInteger) && (key_type == attrString));
    if (first_condition) {
        int *int1 = (int *)key;
        (*target).key.intkey = *int1;
        first_condition = ndtype != INDEX;
        if (first_condition) {
            *pentry_len = sizeof(int) + sizeof(RID); 
                // update the rid in the target pointer
                (*target).data.rid = data.rid;
        }
        else {
            if (!checker_for_entry) {
                int set_pentry_len = 4*sizeof(key) + 4*sizeof(RID);
                *pentry_len = set_pentry_len/4;

            (*target).data.pageNo = data.pageNo; //set new entry to the database
            
            }
            
        }
    }
    if (second_condition) {
        
        second_condition = ndtype == INDEX;
        char *str1 = (char *)key;
        strcpy((*target).key.charkey, str1);
        
        if (second_condition) {
            int set_pentry_len = 4*sizeof(key) + 4*sizeof(RID);
            *pentry_len = set_pentry_len/4;

            (*target).data.pageNo = data.pageNo;     
        }
        if (!second_condition) {
            int set_pentry_len = 4*sizeof(key) + 4*sizeof(RID);

            *pentry_len = set_pentry_len/4;

            (*target).data.rid = data.rid;
            
        }
    }
    // put your code here
    return;
}

/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void *get_key_data(void *targetkey, Datatype *targetdata,
                   KeyDataEntry *psource, int entry_len, nodetype ndtype)
{

    if (ndtype == INDEX)
    {

        targetdata->pageNo = (*psource).data.pageNo;
    }
    else
    {
        (*targetdata).rid = (*psource).data.rid;
    }

    if (entry_len == 4)
    {
        //    *all=((*psource).key.intkey);
        return &((*psource).key.intkey);
    }
    else
    {
        targetkey = &((*psource).key.charkey);
        return &((*psource).key.charkey);
    }

    return NULL;
    // put your code here
    // return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
    if (key_type == attrInteger)
        return sizeof(int);
    else if (key_type == attrString)
    {
        char *a = (char *)key;
        string len = a;
        return len.size();
    }
    // put your code here
    return 0;
    // put your code here
    return 0;
}

/*
 * get_key_data_length: return (key+data) length in given key_type
 */
int get_key_data_length(const void *key, const AttrType key_type,
                        const nodetype ndtype)
{

    if (key_type == attrInteger)
    {
        if (ndtype == INDEX)
            return sizeof(int) + sizeof(PageId);
        else
            return sizeof(int) + sizeof(RID);
    }
    else if (key_type == attrString)
    {
        char *a = (char *)key;
        string len = a;
        //    cout<<len.c_str()<<" key data length "<<len.size()<<endl;
        if (ndtype == INDEX)
            return len.size() + sizeof(PageId);
        else
            return len.size() + sizeof(RID);
    }
    // put your code here
    return 0;
}
