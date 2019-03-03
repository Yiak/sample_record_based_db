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
    delete nullsIndicator;
    return rc::SUCCESS;
}
