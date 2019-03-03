
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
    _rbfm = RecordBasedFileManager::instance();

}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    FileHandle fileHandle;
    if(_rbfm->openFile(TABLE_FILE,fileHandle)==rc::SUCCESS ||_rbfm->openFile(COLUMN_FILE,fileHandle)==rc::SUCCESS){
        return rc::TABLE_EXISTS_ERROR;
    }

    initTable(fileHandle);

    initColumn(fileHandle);

    return rc::SUCCESS;
}

RC RelationManager::initTable(FileHandle & fileHandle) {
    _rbfm->createFile(TABLE_FILE);
    _rbfm->openFile(TABLE_FILE,fileHandle);
    vector<Attribute> table_attr;
    createTableDescription(table_attr);
    RID rid;
    void* data=malloc(TABLE_RECORD_LEN);
    string t_name="Tables";

    prepareTableData(1,t_name,t_name,data);


    _rbfm->insertRecord(fileHandle,table_attr,data,rid);
    t_name="Columns";
    cout<<"In initTable, ready to prepare data."<<endl;
    prepareTableData(2,t_name,t_name,data);
    cout<<"In initTable, finish prepare data."<<endl;
    _rbfm->insertRecord(fileHandle,table_attr,data,rid);

    free(data);
    _rbfm->closeFile(fileHandle);
    return rc::SUCCESS;
}



RC RelationManager::initColumn(FileHandle &fileHandle) {
    _rbfm->createFile(COLUMN_FILE);
    _rbfm->openFile(COLUMN_FILE,fileHandle);
    vector<Attribute> column_attr;
    createColumnDescription(column_attr);
    RID rid;
    void* data=malloc(COLUMN_RECORD_LEN);
    string c_name="table-id";
    cout<<"In initcolumn, ready to prepare data."<<endl;

    prepareColumnData(1,c_name,TypeInt,4,1,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="table-name";
    prepareColumnData(1,c_name,TypeVarChar,50,2,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="file-name";
    prepareColumnData(1,c_name,TypeVarChar,50,3,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="table-id";
    prepareColumnData(2,c_name,TypeInt,4,1,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="column-name";
    prepareColumnData(2,c_name,TypeVarChar,50,2,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="column-type";
    prepareColumnData(2,c_name,TypeInt,4,3,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="column-length";
    prepareColumnData(2,c_name,TypeInt,4,4,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    c_name="column-position";
    prepareColumnData(2,c_name,TypeInt,4,5,data);
    _rbfm->insertRecord(fileHandle,column_attr,data,rid);
    free(data);
    cout<<"In initcolumn, finish prepare data."<<endl;

    _rbfm->closeFile(fileHandle);
    return rc::SUCCESS;
}


RC RelationManager::prepareTableData(int t_id, const string &t_name, const string &f_name, void *data) {
    char* ptr=(char*)data;
    *ptr=0;
    ptr++;
    int* table_id=(int*)ptr;
    *table_id=t_id;
    ptr+=4;
    int* length=(int*)ptr;
    *length=t_name.size();
    ptr+=4;
    char* s_data=ptr;
    for(int i =0;i<t_name.size();++i){
        *s_data=t_name[i];
        s_data++;
    }
    ptr+=t_name.size();
    length=(int*)ptr;
    *length=f_name.size();
    ptr+=4;
    s_data=ptr;
    for(int i =0;i<t_name.size();++i){
        *s_data=f_name[i];
        s_data++;
    }
    return rc::SUCCESS;
}

RC RelationManager::prepareColumnData(int t_id, const string &c_name, int c_type, int c_length, int c_pos, void *data) {
    char* ptr=(char*)data;
    *ptr=0;
    ptr++;
    int* table_id=(int*)ptr;
    *table_id=t_id;
    ptr+=4;
    int* length=(int*)ptr;
    *length=c_name.size();
    ptr+=4;
    char* s_data=ptr;
    for(int i =0;i<c_name.size();++i){
        *s_data=c_name[i];
        s_data++;
    }
    ptr+=c_name.size();
    int* column_type=(int*)ptr;
    *column_type=c_type;
    ptr+=4;
    int* column_length=(int*)ptr;
    *column_length=c_length;
    ptr+=4;
    int* column_pos=(int*)ptr;
    *column_length=c_pos;
    return rc::SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    return rc::SUCCESS;
}

RC RelationManager::createTableDescription(vector<Attribute> &attrs) {

    Attribute tableId, tableName, fileName;

    tableId.name = "table-id";
    tableId.type = TypeInt;
    tableId.length = sizeof(int);
    attrs.push_back(tableId);
    tableName.name = "table-name";
    tableName.type = TypeVarChar;
    tableName.length = 50;
    attrs.push_back(tableName);
    fileName.name = "file-name";
    fileName.type = TypeVarChar;
    fileName.length = 50;
    attrs.push_back(fileName);

    return rc::SUCCESS;
}

RC RelationManager::createColumnDescription(vector<Attribute> &attrs) {

    Attribute tableId, columnName, columnType, columnLength, columnPosition ;

    tableId.name = "table-id";
    tableId.type = TypeInt;
    tableId.length = sizeof(int);
    attrs.push_back(tableId);
    columnName.name = "column-name";
    columnName.type = TypeVarChar;
    columnName.length = 50;
    attrs.push_back(columnName);
    columnType.name = "column-type";
    columnType.type = TypeInt;
    columnType.length = sizeof(int);
    attrs.push_back(columnType);
    columnLength.name = "column-length";
    columnLength.type = TypeInt;
    columnLength.length = sizeof(int);
    attrs.push_back(columnLength);
    columnPosition.name = "column-position";
    columnPosition.type = TypeInt;
    columnPosition.length = sizeof(int);
    attrs.push_back(columnPosition);
    return rc::SUCCESS;
}



RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RID rid;


    //Check if the table file exists.
    FileHandle fileHandle;
    if(_rbfm->openFile(tableName,fileHandle)==rc::SUCCESS ){
        return rc::TABLE_EXISTS_ERROR;
    }
    //Update the table of tables file.
    _rbfm->openFile(TABLE_FILE,fileHandle);
    cout<<"In create table, the table counter now is:"<<fileHandle.tableCounter<<endl;
    vector<Attribute> table_attr;
    createTableDescription(table_attr);
    fileHandle.tableCounter+=1;
    unsigned tableNum=fileHandle.tableCounter;
    void* table_record=malloc(TABLE_RECORD_LEN);
    prepareTableData(tableNum,tableName,tableName,table_record);
    _rbfm->insertRecord(fileHandle,table_attr,table_record,rid);
    _rbfm->closeFile(fileHandle);
    free(table_record);

    //Update the table of columns file.
    _rbfm->openFile(COLUMN_FILE,fileHandle);
    vector<Attribute> column_attr;
    createColumnDescription(column_attr);
    void* column_record=malloc(COLUMN_RECORD_LEN);
    for(unsigned i=0; i<attrs.size();++i){
        prepareColumnData(tableNum,attrs[i].name,attrs[i].type,attrs[i].length,i+1,column_record);
        _rbfm->insertRecord(fileHandle,column_attr,column_record,rid);
    }
    _rbfm->closeFile(fileHandle);
    free(column_record);


    //Create the table file.
    _rbfm->createFile(tableName);


    return rc::SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{

    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
//    if(tableName == TABLE_FILE)
//    {
//        return createTableDescription(attrs);
//    }else if(tableName == COLUMN_FILE)
//    {
//        return createColumnDescription(attrs);
//    }else{
        string table_name="table-name";
        void * data=malloc(PAGE_SIZE);
        RM_ScanIterator rms;
        vector<string> test;
        vector<Attribute> table_attr;
        createTableDescription(table_attr);
        scan(tableName,table_name,NO_OP,tableName.c_str(),test,rms);
        rms.getNextTuple(rms._rbfm_ScanIterator._rid,data);
        _rbfm->printRecord(table_attr,data);

//    }
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator){

    FileHandle fileHandle;
    vector<Attribute> attr;
    createTableDescription(attr);

    _rbfm->openFile(TABLE_FILE,fileHandle);
    string table_name="table-name";
    _rbfm->scan(fileHandle,attr,table_name,NO_OP,value,attributeNames,rm_ScanIterator._rbfm_ScanIterator);

    return rc::SUCCESS;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


RM_ScanIterator::RM_ScanIterator() {
}

RM_ScanIterator::~RM_ScanIterator() {
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    this->_rbfm_ScanIterator.getNextRecord(rid,data);
}