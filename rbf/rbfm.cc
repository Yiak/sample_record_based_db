#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    if(_rbf_manager){
        delete _rbf_manager;
        _rbf_manager = 0;
    }
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}


RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {

    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

int RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) {
    int total_size = 0;
    int attr_size=recordDescriptor.size();
    int data_size=0;
    char* current = (char*)data;
    static unsigned char mask[] = {128, 64, 32, 16, 8, 4, 2, 1};
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    for(int i=0;i<nullFieldsIndicatorActualSize;++i){
        nullsIndicator[i]=*current;
        current++;
    }
    int counter=0;
    for(vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {
        Attribute attr = *itr;
        switch(attr.type)
        {
            case TypeInt:
                if ((nullsIndicator[counter/8] & mask[counter%8]) == 0){
                    current += 4;
                    data_size+=4;
                }
                break;
            case TypeReal:
                if ((nullsIndicator[counter/8] & mask[counter%8]) == 0){
                    current += 4;
                    data_size+=4;
                }
                break;
            case TypeVarChar:
                if ((nullsIndicator[counter/8] & mask[counter%8]) == 0){
                    int* size = (int*)current;
                    current += 4;
                    if(* size==0){
                        data_size++;
                    }
                    for(int i = 0; i < *size; i++)
                    {
                        current++;
                        data_size++;
                    }
                }
                break;
        }
        counter++;
    }
    free(nullsIndicator) ;
    total_size=1+2*attr_size+data_size;
    return total_size;

}

RC RecordBasedFileManager::formatRecord(const vector<Attribute> &recordDescriptor, const void *data,void *record) {
//    cout<<"--------In formatRecord-------"<<endl;
//    cout<<"Now print the input data"<<endl;
//    printRecord(recordDescriptor,data);
//
//
//    cout<<"in format: the null indicater is:"<<(int)*(char*)data<<(int)*((char*)data+1)<<(int)*((char*)data+2)<<(int)*((char*)data+3)<<endl;

    //writer is the ptr which format the record one byte by one byte.
    char* writer= (char*) record;
    //current is the ptr which parse the raw data one byte by one byte.
    char* current = (char*)data;
    //data_index is the index of each data block in this record.
    short data_index=1+2*recordDescriptor.size();
    //The first byte in the record indicates it is a pointer(when ==0) or it is the attr version (default is 1)
    *writer=1;
    writer++;
    //The next two bytes indicate the number of attributes in this record.





    static unsigned char mask[] = {128, 64, 32, 16, 8, 4, 2, 1};
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    for(int i=0;i<nullFieldsIndicatorActualSize;++i){
        nullsIndicator[i]=*current;
        current++;
    }

    //the following variable is a counter which counts how many attr we have already read.
    int counter=0;

    for(vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {

        Attribute attr = *itr;
        switch(attr.type)
        {
            case TypeInt:

                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0){
                    short * ptr=(short*)writer;
                    if(counter==0){
                        *ptr=data_index;
                    }else{
                        short * pre_ptr=(short*)(((char*) record)+1+2*(counter-1));
                        *ptr=*pre_ptr;
                        //cout<<"in format record, int data is: "<<*ptr<<endl;
                    }
                }

                else{
                    short* ptr=(short*)writer;
                    int* pointer=(int*)(((char*) record)+data_index);
                    *pointer=*(int*)current;
                    data_index+=4;
                    *ptr=data_index;
                    current += 4;
                    //cout<<"in format record, int data is: "<<*pointer<<endl;
                    //cout<<"record ptr is "<<*ptr<<endl;
                    //cout<<"data index is:"<<data_index<<endl;
                }
                writer+=2;
                break;

            case TypeReal:
                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0){
                    short * ptr=(short*)writer;
                    if(counter==0){
                        *ptr=data_index;
                    }else{
                        short * pre_ptr=(short*)(((char*) record)+1+2*(counter-1));
                        *ptr=*pre_ptr;
                        //cout<<"in format record, real data is: "<<*ptr<<endl;

                    }
                }

                else{
                    short* ptr=(short*)writer;
                    float* pointer=(float*)(((char*) record)+data_index);
                    *pointer=*(float*)current;
                    data_index+=4;
                    *ptr=data_index;
                    current += 4;
                    //cout<<"in format record, real data is: "<<*pointer<<endl;
                    //cout<<"counter is:"<<counter<<"record ptr is "<<*ptr<<endl;
                }
                writer+=2;
                break;
            case TypeVarChar:

                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0){
                    if(counter==0){
                        short * ptr=(short*)writer;
                        *ptr=data_index;
                    }else{
                        short * ptr=(short*)writer;
                        short * pre_ptr=(short*)(((char*) record)+1+2*(counter-1));
                        *ptr=*pre_ptr;
                    }
                }

                else{

                    short* ptr=(short*)writer;
//                    cout<<"in varchar type, data_index is:"<<data_index<<endl;
                    char* pointer=((char*) record)+data_index;

                    int* size = (int*)current;
                    current += 4;
                    if(*size==0){
                        *ptr=data_index+1;
                        *pointer = 0;
                        data_index++;
                        writer+=2;
                        break;
                    }

                    //data ptr is equal to the previous data ptr means it is a varchar type and length is 0.
                    *ptr=data_index + *size;

                    for(int i = 0; i < *size; i++)
                    {
                        *pointer = *current;
                        current++;
                        pointer++;
                        data_index++;
                    }
                }
                writer+=2;
                break;
        }
        counter++;
    }
    free(nullsIndicator);
//
//    cout<<"data index is "<<data_index<<endl;
//    cout<<"raw data size is:"<<current-(char*)data<<endl;
//    cout<<"--------Quit formatRecord-------"<<endl;

    return rc::SUCCESS;
}

RC RecordBasedFileManager::newPage(FileHandle &fileHandle) {
    void* page = malloc(PAGE_SIZE);
    char* ptr = (char*)page;
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;
    ph->spaceLeft=(PAGE_SIZE-sizeof(pageHeader));
    ph->slotsNumber=0;
    ph->nextFreeSpace=0;
    ph->nextSlotAvailable=PAGE_SIZE;
    fileHandle.appendPage(page);
    free(page);
    return rc::SUCCESS;
}

RC RecordBasedFileManager::getNextAvailableSlot(char *page) {
    pageHeader* ph=(pageHeader*)page+PAGE_SIZE-sizeof(pageHeader);
    ph->nextSlotAvailable=PAGE_SIZE;
    char * ptr=(char*)ph;
    for(int i=1;i<=ph->slotsNumber;i++){
        slotDirectory* sd=(slotDirectory*)(ptr-i* sizeof(slotDirectory));
        if(sd->offset==PAGE_SIZE){
            ph->nextSlotAvailable=i;
            return rc::SUCCESS;
        }
    }
}

int RecordBasedFileManager::getNextFreePage(FileHandle &fileHandle, int size) {
    int pageNumber=fileHandle.getNumberOfPages();
    for (int i=pageNumber-1;i>=0;--i){
        void* page = malloc(PAGE_SIZE);
        fileHandle.readPage(i,page);
        char* ptr = (char*)page;
        ptr += (PAGE_SIZE- sizeof(pageHeader));
        pageHeader* ph=(pageHeader*)ptr;
        if(ph->nextSlotAvailable!=PAGE_SIZE){
            if(ph->spaceLeft>=size){
                free(page);
                return i;
            }
        }else{
            if(ph->spaceLeft>=(size+sizeof(slotDirectory))){
                free(page);
                return i;
            }
        }

        free(page);
    }

    newPage(fileHandle);
    return fileHandle.getNumberOfPages()-1;
}

RC RecordBasedFileManager::writeRecord(FileHandle &fileHandle, int pageNumber, void *record, int recordSize,RID &rid, const RID &w_rid,bool useRid) {
//    cout<<"-----------------In writeRecord-------------"<<endl;
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNumber,page);
    char* ptr = (char*)page;
    //The following find the position of the pageHeader (meta data) in this page.
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;



    if(!useRid) {

        if (ph->nextSlotAvailable != PAGE_SIZE) {
            cout << "slot available is:" << ph->nextSlotAvailable << endl;
            ptr -= ph->nextSlotAvailable * sizeof(slotDirectory);
            slotDirectory *new_slot = (slotDirectory *) ptr;
            new_slot->length = recordSize;
            new_slot->offset = ph->nextFreeSpace;

            ph->spaceLeft -= recordSize;
            ptr = (char *) page;
            ptr += ph->nextFreeSpace;
            memcpy(ptr, record, recordSize);

            ph->nextFreeSpace += recordSize;

            rid.pageNum = pageNumber;
            rid.slotNum = ph->nextSlotAvailable;
            getNextAvailableSlot((char *) page);


        } else {
            ptr -= ((ph->slotsNumber + 1) *
                    sizeof(slotDirectory)); // +1 because we need to move the ptr to the position which can write new slot.
            slotDirectory *new_slot = (slotDirectory *) ptr;
            new_slot->length = recordSize;
            new_slot->offset = ph->nextFreeSpace;

            ph->spaceLeft -= (recordSize + sizeof(slotDirectory));
            ph->slotsNumber++;

            ptr = (char *) page;
            ptr += ph->nextFreeSpace;
            memcpy(ptr, record, recordSize);

            ph->nextFreeSpace += recordSize;

            rid.pageNum = pageNumber;
            //Note: slot number starts from 1.
            rid.slotNum = ph->slotsNumber;
        }

    }else{
        ptr -= (w_rid.slotNum * sizeof(slotDirectory));
        slotDirectory *new_slot = (slotDirectory *) ptr;
        new_slot->length = recordSize;
        new_slot->offset = ph->nextFreeSpace;

        ph->spaceLeft -= recordSize ;
        ptr = (char *) page;
        ptr += ph->nextFreeSpace;
        memcpy(ptr, record, recordSize);
        ph->nextFreeSpace += recordSize;

    }

    fileHandle.writePage(pageNumber, page);
    free(page);
//    cout<<"--------Quit writeRecord-------"<<endl;


}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
//    cout<<"--------In insertRecord-------"<<endl;

    int recordSize=getRecordSize(recordDescriptor,data);
//    cout<<"my format record size is "<<recordSize<<endl;
    int pageToInsert=getNextFreePage(fileHandle,recordSize);
//    cout<<"page#ToInsert is:"<<pageToInsert<<endl;
    void *new_record= malloc(recordSize);
    formatRecord(recordDescriptor,data,new_record);
    RID w_rid;
    writeRecord(fileHandle,pageToInsert,new_record,recordSize,rid,w_rid,false);
    free(new_record);
//    cout<<"--------Quit insertRecord-------"<<endl;
    return rc::SUCCESS;
}




RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
//    cout<<"------------------------In readRecord--------------"<<endl;
    void* buf=malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum,buf);
    char * ptr=(char*)buf;
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;
//
//    cout<<"print the rid.pageNum:"<<rid.pageNum<<endl;
//    cout<<"print the rid.slotNum:"<<rid.slotNum<<endl;
//    cout<<"print the number of slot:"<<ph->slotsNumber<<endl;
//    cout<<"print the spaceLeft:"<<ph->spaceLeft<<endl;
//    cout<<"print the nextFreeSpace:"<<ph->nextFreeSpace<<endl;
//

    //Note: slots start from #1, so we don't need to minus another sizeof(slotDirectory).
    ptr-=rid.slotNum*sizeof(slotDirectory);
    slotDirectory* sd=(slotDirectory*)ptr;

    if(sd->offset==PAGE_SIZE){
        return rc::SLOT_NUMBER_ERROR;
    }
//
//    cout<<"sd offset is:"<<sd->offset<<endl;
//    cout<<"sd length is:"<<sd->length<<endl;


    char* record_ptr=(char*)buf+sd->offset;
    char* current_ptr_head=record_ptr;

//    cout<<"-------test format record----------"<<endl;
//    char* test_ptr=record_ptr;
//    test_ptr+=25;
//    cout<<"The first var is:"<<*test_ptr<<*(test_ptr+1)<<*(test_ptr+2)<<*(test_ptr+3)<<endl;
//    test_ptr+=4;
//    cout<<"real is:"<<*(int*)test_ptr<<endl;
//    test_ptr+=23;
//    cout<<"The next float is:"<<*(float*)test_ptr<<endl;
    if (*record_ptr==0){
        unsigned * pageN=(unsigned *)(record_ptr+1);
        RID real_rid;
        real_rid.pageNum=*pageN;
        real_rid.slotNum=(unsigned)sd->length;
        readRecord(fileHandle,recordDescriptor,real_rid,data);
        return rc::SUCCESS;
    }

    record_ptr+=1;
    int data_index=1+2*recordDescriptor.size();
    char* current = (char*)data;
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    current+=nullFieldsIndicatorActualSize;

    int counter=0;
    for(vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {
        Attribute attr = *itr;
//        cout<<"in attri iterator, the counter now is:"<<counter<<endl;
        switch(attr.type)
        {
            case TypeInt:
//                cout<<"in int type"<<endl;
                if(counter==0){
                    short* data_ptr=(short*)record_ptr;
                    if(*data_ptr>data_index){
                        int* i=(int*)current;
                        *i=*(int*)(current_ptr_head+*data_ptr-4);
//                        cout<<"int data is "<<*i<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                    }
                    record_ptr+=2;
                }else{
                    short* data_ptr=(short*)record_ptr;
                    short* pre_data_ptr=(short*)(record_ptr-2);
//                    cout<<"data_ptr is:"<<* data_ptr<<endl;
//                    cout<<"pre_data_ptr is:"<<* pre_data_ptr<<endl;
                    if(*data_ptr>*pre_data_ptr){
                        int* i=(int*)current;
                        *i=*(int*)(current_ptr_head+*data_ptr-4);
//                        cout<<"int data is "<<*i<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                    }
                    record_ptr+=2;
                }
                break;
            case TypeReal:
//                cout<<"in real type"<<endl;
                if(counter==0){
                    short* data_ptr=(short*)record_ptr;
                    if(*data_ptr>data_index){
                        float* f=(float*)current;
                        *f=*(float*)(current_ptr_head+*data_ptr-4);
//                        cout<<"float data is "<<*f<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                    }
                    record_ptr+=2;
                }else{
                    short* data_ptr=(short*)record_ptr;
                    short* pre_data_ptr=(short*)(record_ptr-2);
//                    cout<<"data_ptr is:"<<* data_ptr<<endl;
//                    cout<<"pre_data_ptr is:"<<* pre_data_ptr<<endl;
                    if(*data_ptr>*pre_data_ptr){
                        float* f=(float*)current;
                        *f=*(float*)(current_ptr_head+*data_ptr-4);
//                        cout<<"float data is "<<*f<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                    }
                    record_ptr+=2;
                }
                break;
            case TypeVarChar:
//                cout<<"-----in var type------"<<endl;
                if(counter==0){
                    short* data_ptr=(short*)record_ptr;
                    int* length=(int*)current;
//                    cout<<"data_ptr is:"<<*data_ptr<<endl;
                    if(*data_ptr>data_index){
                        short var_length=*data_ptr-data_index;
//                        cout<<"---var_length is:"<<var_length<<endl;
                        if(var_length!=1){
                            *length=*data_ptr-data_index;

                        }else{
                            char * var_data=(current_ptr_head+*data_ptr-var_length);
                            if(*var_data==0){
//                                cout<<"var_data is '/0'"<<endl;
                                *length=0;
                            }else{
                                *length=*data_ptr-data_index;
                            }
                        }
//                        cout<<"length is:"<<* length<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                        record_ptr+=2;
                        break;
                    }
                    char * var_data=(current_ptr_head+*data_ptr-*length);
                    for (int i=0;i<*length;++i){
                        *current=*var_data;
//                        cout<<"var data is "<<*var_data<<endl;
                        current++;
                        var_data++;
                    }
                    record_ptr+=2;


                }else{
                    short* data_ptr=(short*)record_ptr;
                    short* pre_data_ptr=(short*)(record_ptr-2);
//                    cout<<"data_ptr is:"<<* data_ptr<<endl;
//                    cout<<"pre_data_ptr is:"<<* pre_data_ptr<<endl;
                    int* length=(int*)current;
                    if(*data_ptr>*pre_data_ptr){
                        short var_length=*data_ptr-*pre_data_ptr;
//                        cout<<"---var_length is:"<<var_length<<endl;
                        if(var_length!=1){
                            *length=*data_ptr-*pre_data_ptr;

                        }else{
                            char * var_data=(current_ptr_head+*data_ptr-var_length);
                            if(*var_data==0){
//                                cout<<"var_data is '/0'"<<endl;
                                *length=0;
                            }else{
                                *length=*data_ptr-*pre_data_ptr;
                            }
                        }
//                        cout<<"length is:"<<* length<<endl;
                        current+=4;
                    }else{
                        nullsIndicator[counter/8] |=1UL<<(7-counter%8);
                        record_ptr+=2;
                        break;
                    }
                    char * var_data=(current_ptr_head+*data_ptr-*length);
//                    cout<<"the counter is:"<<counter<<" the length is:"<<*length<<endl;
                    for (int i=0;i<*length;++i){
                        *current=*var_data;
//                        cout<<"var data is "<<*var_data<<endl;
                        current++;
                        var_data++;
                    }
                    record_ptr+=2;
                }
                break;
        }
        counter++;
    }
    memcpy(data,nullsIndicator,nullFieldsIndicatorActualSize);

//    cout<<"-----check output data----------"<<endl;
//    test_ptr=(char*)data;
//    test_ptr+=4;
//    cout<<"The first var length is:"<<*(int*)test_ptr<<endl;
//    test_ptr+=4;
//    cout<<"The first var is:"<<*test_ptr<<*(test_ptr+1)<<*(test_ptr+2)<<endl;
//    test_ptr+=7;
//    cout<<"real is:"<<*(float*)test_ptr<<endl;
//    test_ptr+=4;
//    cout<<"The next var length is:"<<*(int*)test_ptr<<endl;
//
//
//
//
//
//    cout<<"my output data size is:"<<current-(char*)data<<endl;
//    cout<<"-------quit read record-------"<<endl;
    free(nullsIndicator);
    free(buf);
    return rc::SUCCESS;


}




RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {


    char* current = (char*)data;
    static unsigned char mask[] = {128, 64, 32, 16, 8, 4, 2, 1};
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    for(int i=0;i<nullFieldsIndicatorActualSize;++i){
        nullsIndicator[i]=*current;
        current++;
    }
    int counter=0;
    for(vector<Attribute>::const_iterator itr = recordDescriptor.begin(); itr != recordDescriptor.end(); itr++)
    {
        Attribute attr = *itr;
        switch(attr.type)
        {
            case TypeInt:
                cout<<attr.name<<": ";
                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0)
                    cout<<"NULL ";
                else{
                    cout<<*(int*)current<<" ";
                    current += 4;
                }
                break;
            case TypeReal:
                cout<<attr.name<<": ";
                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0)
                    cout<<"NULL ";
                else{
                    cout<<*(float*)current<<" ";
                    current += 4;
                }
                break;
            case TypeVarChar:
                cout<<attr.name<<": ";
                if ((nullsIndicator[counter/8] & mask[counter%8]) != 0)
                    cout<<"NULL ";
                else{
                    int* size = (int*)current;
                    current += 4;
                    for(int i = 0; i < *size; i++)
                    {
                        char value = *current;
                        cout<<value;
                        current++;
                    }
                    cout<<" ";
                }
                break;
        }
        counter++;
    }
    cout<<endl;
    free(nullsIndicator);
    return rc::SUCCESS;

}

RC RecordBasedFileManager::writeRecordPointer(FileHandle &fileHandle, const RID &rid, RID &pointer_rid) {
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum,page);
    char* ptr = (char*)page;
    //The following find the position of the pageHeader (meta data) in this page.
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;

    ptr -= (rid.slotNum * sizeof(slotDirectory));
    slotDirectory *new_slot = (slotDirectory *) ptr;
    new_slot->length = (short)pointer_rid.slotNum;
    new_slot->offset = ph->nextFreeSpace;

    ph->spaceLeft -= sizeof(rid.pageNum) ;
    ptr = (char *) page;
    ptr += ph->nextFreeSpace;
    void* pointer_record=malloc(1+sizeof(rid.pageNum));
    char* ptr_record=(char*)pointer_record;
    //Assign the first byte to zero means this record is a pointer.
    *ptr_record=0;
    ptr_record++;
    *(unsigned *)ptr_record=pointer_rid.pageNum;

    memcpy(ptr, pointer_record, 1+sizeof(rid.pageNum));
    ph->nextFreeSpace += (1+sizeof(rid.pageNum));
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    free(pointer_record);

    return rc::SUCCESS;

}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {

    void* buf=malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum,buf);
    int newRecordSize=getRecordSize(recordDescriptor,data);
    short oldRecordSize=internalDeleteRecord((char*)buf,rid);
    if(newRecordSize<=oldRecordSize){
        void* new_record=malloc(newRecordSize);
        formatRecord(recordDescriptor,data,new_record);
        RID _rid;
        writeRecord(fileHandle,rid.pageNum,new_record,newRecordSize,_rid,rid,true);
    }else{
        RID new_rid;
        insertRecord(fileHandle,recordDescriptor,data,new_rid);
        writeRecordPointer(fileHandle,rid,new_rid);
    }


    free(buf);
    return rc::SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                         const RID &rid, const string &attributeName, void *data) {
    void* page=malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum,page);
    char * ptr=(char*)page;
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;
    slotDirectory* sd=(slotDirectory*)(ptr-rid.slotNum* sizeof(slotDirectory));

    char* record_ptr=(char*)page+sd->offset;
    char* record_head=record_ptr;

    unsigned index=0;
    for( ;index<recordDescriptor.size();index++){
        if(recordDescriptor[index].name == attributeName)
            break;
    }

    bool isNull=false;
    short* offset= (short *)(record_ptr+1+index*sizeof(short));
    if(index==0){
        if(*offset==1+2*recordDescriptor.size())
            isNull=true;
    }else{
        if(*offset==*(short *)(record_ptr+1+(index-1)*sizeof(short))){
            isNull=true;
        }
    }


    if(isNull){
        free(page);
        *(char*)data=128;
        return rc::SUCCESS;
    }else{
        *(char*)data=0;
    }

    if(recordDescriptor[index].type!=TypeVarChar){
        memcpy((char*)data+1,record_head+*offset-4,sizeof(int));
        free(page);
        return rc::SUCCESS;
    }else{
        if(index==0){
            short data_ptr=1+2*recordDescriptor.size();
            int length=*offset- data_ptr;
            if(length==1){
                if(*(record_head+data_ptr)==0){
                    int l=0;
                    memcpy((char*)data+1,&l,sizeof(int));
                }else{
                    int l=1;
                    memcpy((char*)data+1,&l,sizeof(int));
                    memcpy((char*)data+5,record_head+data_ptr,l);
                }
                free(page);
                return rc::SUCCESS;
            }else{
                memcpy((char*)data+1,&length, sizeof(int));
                memcpy((char*)data+5,record_head+data_ptr,length);
                free(page);
                return rc::SUCCESS;
            }

        }else{

            int length=(*offset- *(short *)(record_ptr+1+(index-1)*sizeof(short)));
            if(length==1){
                if(*(record_head+*offset-length)==0){
                    int l=0;
                    memcpy((char*)data+1,&l,sizeof(int));
                }else{
                    int l=1;
                    memcpy((char*)data+1,&l,sizeof(int));
                    memcpy((char*)data+5,record_head+*offset-length,l);
                }
                free(page);
                return rc::SUCCESS;
            }else{

                cout<<"In readAttribute, find where causes the seg"<<endl;
                memcpy((char*)data+1,&length, sizeof(int));
                memcpy((char*)data+5,record_head+*offset-length,length);
                free(page);
                return rc::SUCCESS;
            }

        }
    }

}

short RecordBasedFileManager::internalDeleteRecord(char *page,const RID &rid) {
    char * ptr=page;
    ptr += (PAGE_SIZE- sizeof(pageHeader));
    pageHeader* ph=(pageHeader*)ptr;
    slotDirectory* sd=(slotDirectory*)(ptr-rid.slotNum* sizeof(slotDirectory));
    for(int i=1;i<=ph->slotsNumber;++i){
        slotDirectory* sd_itr=(slotDirectory*)(ptr-i*sizeof(slotDirectory));
        if((sd_itr->offset>sd->offset)&&sd_itr->offset!=PAGE_SIZE){
            memmove((page+(sd_itr->offset)-sd->length),page+(sd_itr->offset),sd_itr->length);
            sd_itr->offset-=sd->length;
        }
    }
    sd->offset=PAGE_SIZE;
    short deleteSize=sd->length;
    sd->length=PAGE_SIZE;
    ph->spaceLeft+=deleteSize;
    if(rid.slotNum<ph->nextSlotAvailable){
        ph->nextSlotAvailable=rid.slotNum;
    }
    return deleteSize;

}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void* buf=malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum,buf);
    internalDeleteRecord((char*)buf,rid);
    fileHandle.writePage(rid.pageNum,buf);
    free(buf);
    return rc::SUCCESS;
}





RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, const void *value,
                                const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator._fh = fileHandle;
    rbfm_ScanIterator._descriptor = recordDescriptor;
    rbfm_ScanIterator._condAttr = conditionAttribute;
    rbfm_ScanIterator._op = compOp;
    rbfm_ScanIterator._value = (char*)value;
    rbfm_ScanIterator._attrName = attributeNames;
    rbfm_ScanIterator._rid.slotNum = 0;
    rbfm_ScanIterator._rid.pageNum = 0;
    return rc::SUCCESS;
}


RBFM_ScanIterator::RBFM_ScanIterator() {
    this->_record_page= malloc(PAGE_SIZE);
}

RBFM_ScanIterator::~RBFM_ScanIterator() {
    free(this->_record_page);
}


RC RBFM_ScanIterator::getNextRid(FileHandle fileHandle, RID& rid) {
    cout<<"In rbfm iterator, rid is s,p :"<<rid.slotNum<<" "<<rid.pageNum<<endl;
    if(rid.slotNum==0){
        fileHandle.readPage(rid.pageNum,this->_record_page);
    }
    char* page=(char*)this->_record_page;
    pageHeader* ph=(pageHeader*)(page+PAGE_SIZE-sizeof(pageHeader));
    if(rid.slotNum==ph->slotsNumber){
        rid.slotNum=0;
        rid.pageNum+=1;
        if (rid.pageNum>=fileHandle.getNumberOfPages()){
            rid.pageNum=0;
            rid.slotNum=0;
            return rc::RID_EOF;
        }
        return getNextRid(fileHandle,rid);
    }else{
        rid.slotNum+=1;
        slotDirectory* sd=(slotDirectory*)(page+PAGE_SIZE-sizeof(pageHeader)-rid.slotNum*sizeof(slotDirectory));
        if(sd->length==PAGE_SIZE&&sd->offset==PAGE_SIZE){
            return getNextRid(fileHandle,rid);
        }else{
            return rc::SUCCESS;
        }
    }
}

template <class type>
bool compValue(type a, type b, CompOp compOp)
{
    if(compOp == EQ_OP)
        return a == b;

    if(compOp == LT_OP)
        return a < b;

    if(compOp == GT_OP)
        return a > b;

    if(compOp == LE_OP)
        return a <= b;

    if(compOp == GE_OP)
        return a >= b;

    if(compOp == NE_OP)
        return a != b;

    return false;
}

bool compStr(const char *keyData, const char *compData, CompOp compOp)
{
    if(compOp == EQ_OP)
        return (strcmp(keyData, compData) == 0);

    if(compOp == LT_OP)
        return (strcmp(keyData, compData) < 0);

    if(compOp == GT_OP)
        return (strcmp(keyData, compData) > 0);

    if(compOp == LE_OP)
        return (strcmp(keyData, compData) <= 0);

    if(compOp == GE_OP)
        return (strcmp(keyData, compData) >= 0);

    if(compOp == NE_OP)
        return (strcmp(keyData, compData) != 0);

    return false;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    if(getNextRid(this->_fh,this->_rid)==RID_EOF){
        return RBFM_EOF;
    }
    cout<<"In rbfm get next record, next rid is: s,p:"<<rid.slotNum<<" "<<rid.pageNum<<endl;

    unsigned index=0;

    char* attr= (char*)malloc(PAGE_SIZE);
//    cout<<"in getNextRecord, ready to get into readAttribute"<<endl;
    RecordBasedFileManager::instance()->readAttribute(this->_fh,this->_descriptor,this->_rid,this->_condAttr,attr);
//    cout<<"in getNextRecord, just quit readAttribute"<<endl;
    for( ;index<this->_descriptor.size();index++){
        if(this->_descriptor[index].name == this->_condAttr)
            break;
    }
    if(this->_op==CompOp::NO_OP){
        cout<<"In no_op mode!"<<endl;
        RecordBasedFileManager::instance()->readRecord(this->_fh,this->_descriptor,this->_rid,data);
        rid=this->_rid;
        free(attr);
        return rc::SUCCESS;
    }else{

    }


    return rc::SUCCESS;


}