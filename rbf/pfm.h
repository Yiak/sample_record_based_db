#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <fstream>
#include <sys/stat.h>
#include <iostream>
#include <cstring>

using namespace std;

typedef struct{
    unsigned readCounter;
    unsigned writeCounter;
    unsigned appendCounter;
    unsigned tableCounter;
} counterInfo;

enum rc {
    SUCCESS=0,
    FILE_EXISTS=1,
    FILE_CREATE_FAILS=2,
    FILE_NOT_EXISTS=3,
    FILE_DESTROY_FAILS=4,
    PAGE_NUMBER_OVERFLOW=5,
    SLOT_NUMBER_ERROR=6,
    TABLE_EXISTS_ERROR=7,
    RID_EOF=8,
};


class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance
    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
private:
    FILE* _file;
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    unsigned tableCounter;

    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    RC initFile( FILE* fileptr);
    FILE* getFilePtr();
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
};

#endif