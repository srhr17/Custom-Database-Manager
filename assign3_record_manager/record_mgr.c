#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <limits.h>
#include <float.h>
#define abytes 16

RM_TableData* table;

typedef struct ScanData
{
	RID rid;
	Expr* cond;
} ScanData;

ScanData* sc;

extern RC initRecordManager (void *mgmtData){
	initStorageManager();
	table=(RM_TableData*)calloc(1,sizeof(RM_TableData));

	return RC_OK;
}
extern RC createTable (char *name, Schema *schema){
	SM_FileHandle file;
	createPageFile(name);
	table->name=name;
	table->schema=schema;

	table->tc=0;

	openPageFile(name,&file);
	char *data = calloc(1,PAGE_SIZE);
	char temp[10];
	sprintf(temp, "%i", schema->keySize);
	strncat(data, temp, 10);
	strcat(data, "#");
	memset(temp,0,10);
	for (int i = 0; i < schema->keySize; i++) {
		sprintf(temp, "%i", schema->keyAttrs[i]);
		strncat(data, temp, 10);
		strcat(data, "#");
		memset(temp,0,10);
	}
	sprintf(temp, "%i", schema->numAttr);
	strncat(data, temp, 10);
	strcat(data, "#");
	memset(temp,0,10);
	for (int i = 0; i < schema->numAttr; i++) {
		strncat(data, schema->attrNames[i], 20);
		strcat(data, "#");
		sprintf(temp, "%i", schema->dataTypes[i]);
		strncat(data, temp, 10);
		strcat(data, "#");
		memset(temp,0,10);
		sprintf(temp, "%i", schema->typeLength[i]);
		strncat(data, temp, 10);
		strcat(data, "#");
		memset(temp,0,10);
	}
	strncat(data, "\0", 1);
	writeBlock(0,&file,data);
	closePageFile(&file);
	return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){
	
	BM_BufferPool *bp=MAKE_POOL();
	initBufferPool(bp,name,4,RS_FIFO,NULL);
	rel->name=name;
	rel->mgmtData=bp;
	rel->schema=table->schema;

	rel->tc=table->tc;

	return RC_OK;
}


extern RC shutdownRecordManager (){
	free(table);

	// table = NULL;

	return RC_OK;
}

extern RC closeTable (RM_TableData *rel){
	shutdownBufferPool(rel->mgmtData);
	free(rel->mgmtData);
	return RC_OK;
}

extern RC deleteTable (char *name){
	destroyPageFile(name);
	return RC_OK;
}

extern int getNumTuples (RM_TableData *rel){

	printf("%i",rel->tc);
	return rel->tc;

}

extern RC createRecord (Record **record, Schema *schema)
 {
     Record *myrecord = (Record *) calloc (1,sizeof(Record)+10);
     myrecord->data = (char *) calloc (1,getRecordSize(schema)+10);
	 memset(myrecord->data,0,sizeof(Record));
     RID myrecordID = myrecord->id;
     myrecordID.page = -1;
     myrecordID.slot = -1;
     *record = myrecord;
     return RC_OK;

 }

extern int getRecordSize(Schema *schema) {
	int size = 0;
	for (int i=0; i < schema->numAttr; i++) {
		switch(schema->dataTypes[i]){
			case DT_BOOL:
				size += sizeof(bool);
				break;
			case DT_INT:
				size += sizeof(int);
				break;
			case DT_FLOAT:
				size += sizeof(float);
				break;
			case DT_STRING:
				size += schema->typeLength[i];
				break;
			default:
				break;
		}
	}
	return size;
}

//inserts record into the table
extern RC insertRecord(RM_TableData* rel, Record* record)
{
	int pageNumber=1,slotNumber=0,pageLength;	
	int recordLength = getRecordSize(rel->schema);

	BM_BufferPool *bp = (BM_BufferPool *)rel->mgmtData;
	BM_PageHandle *bph = MAKE_PAGE_HANDLE();
	SM_FileHandle *sfh = (SM_FileHandle *)bp->mgmtData;
	while(pageNumber < sfh->totalNumPages)
	{

		
			pinPage(bp,bph,pageNumber);
			pageLength = strlen(bph->data);

			if(PAGE_SIZE-pageLength > recordLength)
			{
				slotNumber = pageLength/recordLength;
				unpinPage(bp,bph);
				break;
			}
			unpinPage(bp,bph);

			pageNumber++;
	}
	// 	if(slotNumber == 0)
	// {
	// 	pinPage(bp,bph,pageNumber + 1);
	// 	unpinPage(bp,bph);
	// }
	pinPage(bp,bph,pageNumber);
	char *str =NULL;
	str = bph->data + strlen(bph->data);
	strcat(str,record->data);
	bph->data=str;
	// printf("%s",bph->data);
	markDirty(bp,bph);
	unpinPage(bp,bph);
	RID rid;
	rid.page = pageNumber;
	rid.slot = slotNumber;
	record->id = rid;
	table->tc++;
	free(bph);
	return RC_OK;
}

//deletes record from the table
extern RC deleteRecord(RM_TableData* rel, RID id) {
	
	int pageNumber=id.page;
	BM_BufferPool *bp = (BM_BufferPool *)rel->mgmtData;
	BM_PageHandle *bph = MAKE_PAGE_HANDLE();
	pinPage(bp,bph,pageNumber);
	markDirty(bp,bph);
	unpinPage(bp,bph);
	free(bph);
	return RC_OK;
}

//Update the record having ID in the RID
extern RC updateRecord(RM_TableData* rel, Record* record) {
	int slotNumber=record->id.slot;
	int pageNumber=record->id.page;
	BM_BufferPool *bp = (BM_BufferPool *)rel->mgmtData;
  	BM_PageHandle *bph = MAKE_PAGE_HANDLE();
	pinPage(bp,bph,pageNumber);
	record->data=bph->data+getRecordSize(rel->schema)+slotNumber;
	free(bph);
	return RC_OK;
}

//Return record from memory
extern RC getRecord(RM_TableData* rel, RID id, Record* record) {
	BM_BufferPool *bp = (BM_BufferPool *)rel->mgmtData;
	BM_PageHandle *bph = MAKE_PAGE_HANDLE();
	int pageNumber=id.page;
	int slotNumber=id.slot;
	int recordLength = getRecordSize(rel->schema);
	pinPage(bp,bph,pageNumber);
	char *str = bph->data + recordLength * slotNumber;
	strncpy(record->data,str,recordLength);
	unpinPage(bp,bph);
	return RC_OK;
}

//get the next record that matches scan condition
extern RC next(RM_ScanHandle* scan, Record* record) {
	
	//accocate space to store the value
	Value* result = calloc(1,sizeof(Value));
	
	ScanData* scanData = scan->mgmtData;
	
	RM_TableData* tb = scan->rel->mgmtData;
	
	//get the expression condition and check if the condition is NULL
	Expr *cond = (Expr *)scan->mgmtData;
	if(cond == NULL){
		return RC_ERROR;
	}
	
	//get the schema
	Schema *schema = scan->rel->schema;
	
	//get the size of record
    	int recordSize = getRecordSize(schema);
	
	//get total number of records for the page
	int totalRecordsPerPage = PAGE_SIZE / recordSize;
	
	while (1) {
		if (scanData->rid.page > table->tc) {
			break;
		}

		if (scanData->rid.slot == totalRecordsPerPage) {
			scanData->rid.slot = 0;
			scanData->rid.page = scanData->rid.page + 1;
		}

		getRecord(scan->rel, scanData->rid, record);
		if (record->data == NULL) {
			scanData->rid.slot = scanData->rid.slot + 1;
			continue;
		}
		BM_PageHandle *bph = MAKE_PAGE_HANDLE();
		//pin the page which has data
		pinPage(table->mgmtData, bph, scanData->rid.page);
		
		//check if the record satisfies the expression
		evalExpr(record, schema, scanData->cond, &result);
		
		if (result->v.boolV == TRUE) {
			//unpin the page from buffer
			unpinPage(table->mgmtData, bph);
			scanData->rid.slot = scanData->rid.slot + 1;
			return RC_OK;
		}
	}
	return RC_RM_NO_MORE_TUPLES;
}


extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
 {
	sc =(ScanData*)calloc(1,sizeof(ScanData));
	sc->rid.page = 1;
	sc->rid.slot = 0;
	sc->cond = cond;
	scan->mgmtData = sc;
	scan->rel = rel;
	return RC_OK;
 }
 extern RC closeScan (RM_ScanHandle *scan)
 {
     free(scan->mgmtData);
     return RC_OK;
 }

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
 {
     Schema *mySchema = (Schema *)calloc(1,sizeof(Schema));
	mySchema->numAttr=numAttr;
	mySchema->attrNames=attrNames;
	mySchema->dataTypes=dataTypes;
	mySchema->typeLength=typeLength;
	mySchema->keySize=keySize;
	mySchema->keyAttrs=keys;
	return mySchema;
}
     
extern RC freeSchema (Schema *schema)
 {
     free(schema);
     return RC_OK;
 }

extern RC freeRecord (Record *record)
{
    free(record);
    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    Value *myval = (Value *)calloc(1,sizeof(Value)+10);
    myval->dt = schema->dataTypes[attrNum];
    int size;
    char *dataptr = record->data;
    int position = 0;
    int i;
    for (i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
        case DT_INT:
            position += sizeof(int);
            break;
        case DT_FLOAT:
            position += sizeof(float);
            break;
        case DT_STRING:
            position += schema->typeLength[i] + 1; 
            break;
        case DT_BOOL:
            position += sizeof(bool);
            break;
        }
    }
  dataptr += position;
  switch (schema->dataTypes[attrNum]) {
    case DT_INT:
      size = sizeof(int);
      memcpy(&myval->v.intV, dataptr, size);
      break;
    case DT_FLOAT:
      size = sizeof(float);
      memcpy(&myval->v.floatV, dataptr, size);
      break;
    case DT_STRING:
      size = schema->typeLength[attrNum];
      myval->v.stringV = (char*)calloc(1,schema->typeLength[attrNum] + 1);
      memcpy(&myval->v.stringV, dataptr, size); 
      break;
    case DT_BOOL:
      size = sizeof(bool);
      memcpy(&myval->v.boolV, dataptr, size);
      break;
  }
  *value = myval;
  return RC_OK;
}
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
  int position = 0;
  int i;
  int size;
  char *dataptr = record->data;
    for (i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
        case DT_INT:
            position += sizeof(int);
            break;
        case DT_FLOAT:
            position += sizeof(float);
            break;
        case DT_STRING:
            position += schema->typeLength[i] + 1; 
            break;
        case DT_BOOL:
            position += sizeof(bool);
            break;
        }
    }
  dataptr += position;
  switch(value->dt) {
    case DT_INT:
      if (value->v.intV > INT_MAX || value->v.intV < INT_MIN) {
        return RC_ERROR;
      }
      size = sizeof(int);
      memcpy(dataptr, &value->v.intV, size);
      break;
    case DT_FLOAT:
      if (value->v.floatV > FLT_MAX || value->v.floatV < FLT_MIN) {
        return RC_ERROR;
      }
      size = sizeof(float);
      memcpy(dataptr, &value->v.floatV, size);
      break;
    case DT_STRING:
      if (strlen(value->v.stringV) > schema->typeLength[attrNum]) {
        return RC_ERROR;
      }
      size = schema->typeLength[attrNum] + 1; 
      memcpy(dataptr, value->v.stringV, size);
      dataptr[size - 1] = '\0'; 
      break;
    case DT_BOOL:
      if (value->v.boolV != true && value->v.boolV != false) {
        return RC_ERROR;
      }
      size = sizeof(bool);
      memcpy(dataptr, &value->v.boolV, size);
      break;
    default :
      return RC_RM_UNKOWN_DATATYPE;
  }
  return RC_OK;
}
