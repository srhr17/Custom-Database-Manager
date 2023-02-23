# Instructions to run it, execute the following in order
```
$ make
$ ./storage
```

# Functions Implemented : 

initStorageManager()
- This function checks if directory for storage exists at a pre-defined path. If it exists returns "Storage already initialized", 
else goes on to create a folder with the name. 
- If successfully created returns "Storage initialized" else throws an error.

createPageFile
- This function creates one new page in the name 'fileName' which is passed as parameter to this function by opening this file in 'w+b' mode
- Once the page is created, we intialize a page handler (SM_PageHandle) and allocate memory using calloc() method
- This function then fills this page with '/0' bytes using memset() method
- The data from buffer page is copied to the file using fwrite() method
- File is closed using fclose() and memory in the buffer is deallocated using free() method

openPageFile

- This function opens the file of 'fileName' in 'r+' mode
- Corresponding values for file handler (SM_FileHandle) are assigned
	- fileName is assigned from the parameter
	- curPagePos is set as 0
	- mgmtInfo is assigned with the opened file
	- totalNumPages is assigned with the total number of pages calculated using stat() method
- If the file is not opened, then RC_FILE_NOT_FOUND will be returned

closePageFile
- If the passed file handler parameter is NULL, then RC_FILE_HANDLE_NOT_INIT is returned
- Else the file stored in 'mgmtInfo' of the file handler is closed using fclose() method
- If the close method fails, RC_FILE_NOT_FOUND is returned

readBlock
- In this function if the file handler parameter is NULL, RC_FILE_HANDLE_NOT_INIT is returned
- If the file at 'mgmtInfo' of the file handler is NULL, RC_FILE_NOT_FOUND is returned
- If the page number is less than 0 or greater than total number of pages, RC_READ_NON_EXISTING_PAGE is returned
- Else, the cursor is moved to the start of the block in the given pageNum page using fseek() method
- If the fseek method does not return 0, RC_ERROR will be returned
- Then the file is read using fread() method and is stored in the memPage pointer poisition which is passed as a parameter
- If the fread() method does not return the total number of bytes, then RC_READ_FAILED is returned
- The pageNum value is assigned to the 'curPagePos' of the file handler

destoryPageFile()
- Checks if the page exits. If it exists then delete the page from the storage else throw an error.

getBlockPos()
- Checks if file handle is already initialized. If so then it return the position of current page.

readNextBlock()
- Reads the block at next position with respect to current position.

readCurrentBlock() 
- reads the block at current position.

readPreviousBlock()
- reads the block at previous position with respect to current position.

readFirstBlock()
- reads the block at 0th position.

readLastBlock()
- reads the block at last position (totalsize-1).

writeBlock()
- validate the page number and check the availability of the pointer to the page and with the help of pointer , use fseek() we navigate to the given location and once thats successful we write the data to the disk using fwrite() function 

writeCurrentBlock()
- Call writeBlock() with page number as the parameter

appendEmptyBlock()
- creates an empyt block and then navigates the file pointer to the end of the file and write the empty block
at the end of the file and update the page count(add 1 to the existing page count)

ensureCapacity()
- finds out how many number of pages have to be appended and creates the required amount of empty blocks using appendEmptyBlock() function


