#include "pfm.h"


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
    if(_pf_manager) {
        delete _pf_manager;
        _pf_manager = 0;
    }
}

bool ifFileExists(const string& file) {
    struct stat buf;
    return (stat(file.c_str(), &buf) == 0);
}

RC PagedFileManager::createFile(const string &fileName)
{
    if(ifFileExists(fileName))
        return rc::FILE_EXISTS;

    ofstream file;
    file.open(fileName);
    if (!file.is_open())
        return rc::FILE_CREATE_FAILS;

    FileHandle fileHandle;
    FILE* fileptr=fopen(fileName.c_str(),"rb+");
    fileHandle.initFile(fileptr);
    void* page=malloc(PAGE_SIZE);
    counterInfo* ci=(counterInfo*) page;
    ci->readCounter=(unsigned) 0;
    ci->writeCounter=(unsigned) 0;
    ci->appendCounter=(unsigned) 0;
    ci->tableCounter=(unsigned) 2; //This is only for table of table, since there are two tables when init, so it is 2.
    fileHandle.appendPage(page);

    free(page);
    file.close();
    return rc::SUCCESS;

}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(!ifFileExists(fileName))
        return rc::FILE_NOT_EXISTS;
    int result=remove(fileName.c_str());
    if (result!=0)
        return rc::FILE_DESTROY_FAILS;
    return rc::SUCCESS;

}



RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(!ifFileExists(fileName))
        return FILE_NOT_EXISTS;
    FILE* fileptr=fopen(fileName.c_str(),"rb+");
    fileHandle.initFile(fileptr);



    void* page=malloc(PAGE_SIZE);
    fileHandle.readPage(-1,page);
    counterInfo* ci=(counterInfo*) page;

//    cout<<"In openFile, print table counter for test:"<<ci->tableCounter<<endl;

//    cout<<"in open,ci rwa:"<<ci->readCounter<<" "<<ci->writeCounter<<" "<<ci->appendCounter<<endl;

    fileHandle.readPageCounter=ci->readCounter;
    fileHandle.writePageCounter=ci->writeCounter;
    fileHandle.appendPageCounter=ci->appendCounter;
    fileHandle.tableCounter=ci->tableCounter;

    free(page);
//    cout<<"in open, fh rwa:"<<fileHandle.readPageCounter<<" "<<fileHandle.writePageCounter<<" "<<fileHandle.appendPageCounter<<endl;

    return rc::SUCCESS;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    void* page=malloc(PAGE_SIZE);
    fileHandle.readPage(-1,page);
    fileHandle.readPageCounter-=1;
    counterInfo* ci=(counterInfo*) page;
//    cout<<"in close, fh rwa:"<<fileHandle.readPageCounter<<" "<<fileHandle.writePageCounter<<" "<<fileHandle.appendPageCounter<<endl;
//    cout<<"In closeFile, print table counter for test:"<<ci->tableCounter<<endl;

    ci->readCounter=fileHandle.readPageCounter;
    ci->writeCounter=fileHandle.writePageCounter;
    ci->appendCounter=fileHandle.appendPageCounter;
    ci->tableCounter=fileHandle.tableCounter;
    fileHandle.writePage(-1,page);
//    cout<<"in close, ci rwa:"<<ci->readCounter<<" "<<ci->writeCounter<<" "<<ci->appendCounter<<endl;

    free(page);
    fclose(fileHandle.getFilePtr());
    return rc::SUCCESS;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    unsigned realNum=pageNum+1;
//    cout<<"In fh readPage, realNum is:"<<realNum<<" pageNum is:"<<pageNum<<" getNumberofPages is:"<<getNumberOfPages()<<endl;

    if(pageNum>= getNumberOfPages()&&pageNum!=-1){
//        cout<<"Should not see this check sign."<<endl;
        return rc::PAGE_NUMBER_OVERFLOW;
    }

    fflush(_file);
    fseek(_file, realNum * PAGE_SIZE, SEEK_SET);
    fread(data, PAGE_SIZE, 1, _file);


    readPageCounter++;
    return rc::SUCCESS;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    unsigned realNum=pageNum+1;
//    cout<<"In fh writePage, realNum is:"<<realNum<<endl;
    if(pageNum >= getNumberOfPages()&&pageNum!=-1)
        return rc::PAGE_NUMBER_OVERFLOW;
    fflush(_file);
    fseek(_file, realNum * PAGE_SIZE, SEEK_SET);
    fwrite(data, PAGE_SIZE, 1, _file);

    writePageCounter++;
    return rc::SUCCESS;
}


RC FileHandle::appendPage(const void *data)
{
    fflush(_file);
    fseek(_file, 0, SEEK_END);
    fwrite(data, PAGE_SIZE, 1, _file);


    appendPageCounter++;
    return rc:: SUCCESS;
}

RC FileHandle::initFile(FILE *fileptr) {
    this->_file=fileptr;
    return -1;
}

FILE* FileHandle::getFilePtr() {
    return this->_file;
}

unsigned FileHandle::getNumberOfPages()
{
    fflush(_file);
    fseek(_file, 0, SEEK_END);
    return (ftell(_file)/PAGE_SIZE)-1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return rc::SUCCESS;
}