#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "storage_mgr.h"
#include <string.h>
#include "dberror.h"
void initStorageManager (){
    int res;
    char* dirname = "Storage";
    struct stat str;
    // check if directory exists already or not
    if (stat(dirname, &str) == 0 && S_ISDIR(str.st_mode)) 
        {
            //directory already exists
        printf("Storage already initialized");
        } 
    else {
        
        res = mkdir(dirname,0777);
        // check if directory is created or not
        if (!res)
            printf("Storage initialized\n");
            
        else {
            //directory creation failed
            printf("Unable to initialize storage\n");
            exit(1);
            }
        }
    
    
  
}
// void main()
// {
// initStorageManager ();
// }

extern RC createPageFile(char *fileName){
	FILE *f = fopen(fileName, "w+b");
	if(f != NULL){
		//dynamically allocates memory
		SM_PageHandle newPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
		
		//Fills the page with \0 bytes
		memset(newPage, 0, PAGE_SIZE);
		
		//Writes the data from buffer page to the file
		fwrite(newPage, sizeof(char), PAGE_SIZE, f);
		fclose(f);
		
		//dynamically deallocates memory
		free(newPage);  
		
		return RC_OK;
	}
	else{
		printf("error occured during page creation");
		return RC_FILE_NOT_FOUND;
	}
}

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
	FILE *f = fopen(fileName, "r+");
	//check if file is accessible and open
	if(f == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		//assign value for File Name
		fHandle->fileName = fileName;
		
		//calculate total file size
		struct stat buf;
		stat(fileName, &buf);
		int size = buf.st_size;
		
		//assign total number of pages value
		if(size%PAGE_SIZE > 0){
			fHandle->totalNumPages = (size/PAGE_SIZE) + 1;
		}
		else{
			fHandle->totalNumPages = size/PAGE_SIZE;
		}
		
		//assign current page position and file information
		fHandle->curPagePos = 0;
		fHandle->mgmtInfo = f;
		return RC_OK;
	}
}

extern RC closePageFile (SM_FileHandle *fHandle) {
	//check if SM_FileHandle pointer is not null
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else{
		int ret = fclose(fHandle->mgmtInfo);
		
		//check if return value from fclose() is 0 or EOF
		if(ret == 0){
			return RC_OK;
		}
		else{
			return RC_FILE_NOT_FOUND;
		}
	}
}

extern RC destroyPageFile (char *fileName)
{
	//check if the file exits
    int accessFile=access( fileName, F_OK );
    if( accessFile== 0 ) 
    {
    // file exists
    int removeFile=remove(fileName);
    if (removeFile==0)
    {
        //file deleted successfully
        return RC_OK;
    }
    else
    {
        //file deletion failed
        return RC_FILE_DELETION_FAILED;
    }
    } else 
    {
    // file doesn't exist
        return RC_FILE_NOT_FOUND;
    }
    
}

extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	//check if File Handle is not null
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	//check if file is open and available; if not return error
	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_NOT_FOUND;
	}
	//check if given page number to be read is valid (i.e. should not be less than 0 or greater than total # of pages)
	if(pageNum < 0 || pageNum > fHandle->totalNumPages){
		return RC_READ_NON_EXISTING_PAGE;
	}
	
	//cursor is moved to start of block in pageNum page
	int start = fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);
	//if the fseek method does not return 0, throw error
	if(start != 0){
		return RC_ERROR;
	}
	
	//file is read and is stored in memPage pointer position
	int readResult = fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
	//fread method returns the total # of bytes read
	if(readResult != PAGE_SIZE){
		return RC_READ_FAILED;
	}
	
	//assign current page position value as given page number
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle)
{
    //checks if the file handle is initialized
    if (fHandle->mgmtInfo!=NULL)
    {
        //returns the current position of the file handle
        return fHandle->curPagePos;
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //checks if the file handle is initialized
    if (fHandle->mgmtInfo!=NULL)
    {
        //reads the block at first position
        return readBlock(0,fHandle,memPage);
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //checks if the file handle is initialized
    if (fHandle->mgmtInfo!=NULL)
    {
        //reads the block at previous position with respect to current position
        return readBlock(getBlockPos(fHandle)-1,fHandle,memPage);
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
     //checks if the file handle is initialized
    if (fHandle->mgmtInfo!=NULL)
    {
        //reads the block at current position
        return readBlock(getBlockPos(fHandle),fHandle,memPage);
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //checks if the file handle is initialized
    if (fHandle->mgmtInfo!=NULL)
    {
        //reads the block at next position with respect to current position
        return readBlock(getBlockPos(fHandle)+1,fHandle,memPage);
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //checks if the file handle is initialized
     if (fHandle->mgmtInfo!=NULL)
    {
        //reads the block at last position
        return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
    }
    else
    {
        //file handle is not initialized
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	ensureCapacity (pageNum, fHandle);		
	
	FILE *fp;
	RC rv;

	fp=fopen(fHandle->fileName,"rb+");
	if(fseek(fp,pageNum * PAGE_SIZE, SEEK_SET) != 0){
		rv = RC_READ_NON_EXISTING_PAGE;	
	} else if (fwrite(memPage, PAGE_SIZE, 1, fp) != 1){
		rv = RC_WRITE_FAILED; 
	} else {
		fHandle->curPagePos=pageNum;	
		rv = RC_OK;
	}

	fclose(fp);
	return rv;
}



RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle->curPagePos < 0){
		return RC_WRITE_FAILED;
	}

	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


RC appendEmptyBlock (SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 

	FILE *fp;
	char *allocData;
	RC rv;

	allocData = (char *)calloc(1, PAGE_SIZE);
	fp=fopen(fHandle->fileName,"ab+");

	if(fwrite(allocData, PAGE_SIZE, 1, fp) != 1)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages += 1;
		rv = RC_OK;		
	}

	free(allocData);
	fclose(fp);

	return  rv;
}


RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle -> totalNumPages >= numberOfPages){
		return RC_OK;
	}
	
	FILE *fp;
	long allocCapacity;
	char *allocData;
	RC rv;

	allocCapacity= (numberOfPages - fHandle -> totalNumPages) * PAGE_SIZE;
	allocData = (char *)calloc(1,allocCapacity);
	
	fp=fopen(fHandle->fileName,"ab+");
   
	if(fwrite(allocData, allocCapacity, 1, fp) == 0)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages = numberOfPages;	
		rv = RC_OK;
	}

	free(allocData);
	fclose(fp);

	return rv;
}

//End of File
