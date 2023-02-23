Assignment 2 : Buffer Manager
Functions included:
_______________
- InitBufferPool() :

This method creates an empty buffer and initializes every frame to a state called unused state.

- shutdownBufferPool():

This method checks if there are any frames are in use and removes all the frames.

- forceFlushPool():

Writes the dirty and not-in-use page back to disk.

- markDirty():

This method marks certain pages as dirty

- pinPage() :

This method gets the frame position using hash table.  If the page exists, it adds 1 to the fix count and if its not found then it reads the page from the disk and then pins the given page number.

- unpinPage():

when a page is in use, this method unpins it.

- forcePage():

This method writes the page into the disk forcibly.

- getFrameContents():

This method fetches all the page numbers of the frames present in the pool.

- getDirtyFlags():

This method fetches every dirty page from the buffer pool.

- getFixCounts():

This method fetches all the fix count from every frame.

- getNumReadIO():

This method fetches all the page numbers that have been read from the disk to store it in MgmData.

- getNumWriteIO():

This method fetches all the page numbers that have been written to the disk to store it in MgmData.

- FIFO() :

This method replaces pages which are not in use when the buffer is full and writes the page back if its dirty

- LRU():

Using LRU technique, it replaces the frames that are not in use and assigns it a new timestamp

- CLOCK() :

Checks if each frame starts with 1 or 0 and based on what the binary number is 0 it’ll replace it with new page because the page is currently not in use.
