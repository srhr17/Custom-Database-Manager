#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "stdio.h"
#include "stdlib.h"
#include "unistd.h"

int totalFreq;

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
		{
			if( access( pageFileName, F_OK ) == 0 ) {
    				// file exists
					bm->numPages=numPages;
					bm->pageFile=pageFileName;
					bm->strategy=strategy;
					bm->totalReadCount=0;
					bm->totalWriteCount=0;
					totalFreq = 0;	
					BM_PageHandle *pageFrame = (BM_PageHandle *)calloc(numPages,sizeof(BM_PageHandle));
					for (int i=0;i<numPages;i++)
					{
						(i+pageFrame)->dirty=0;
						(i+pageFrame)->fixCount=0;
						(i+pageFrame)->pageNum=-1;
						(i+pageFrame)->frequency=0;
						(i+pageFrame)->data=(SM_PageHandle *)calloc(PAGE_SIZE,sizeof(char));
					}
					bm->mgmtData=pageFrame;
					bm->stratData=stratData;
					return RC_OK;
				} else {
    				// file doesn't exist
					 return RC_BUFFERPOOL_INIT_FAILED;
					}
		}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		if ((PageFrame->fixCount)==1)
		{
			//free(PageFrame);
			return RC_BUFFERPOOL_SHUTDOWN_FAILED;
		}
		else if ((PageFrame->dirty==1) && (PageFrame->fixCount==0))
		{
			//write Page to SM
			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			writeBlock(PageFrame->pageNum, &fHandle, PageFrame->data);
			PageFrame->dirty=0;
			closePageFile(&fHandle);
			bm->totalWriteCount++;
		}
		//free(PageFrame);
	}
		// free(bm->strategy);
		// free(bm->stratData);
		// free(bm->pageFile);
		// free(bm->totalReadCount);
		// free(bm->totalWriteCount);
	for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		//free(PageFrame->dirty);
		//free(PageFrame->fixCount);
		//free(PageFrame->pageNum);
		//free(PageFrame->data);

	}
		// free(bm->numPages);
		//free(bm->mgmtData);
		// free(bm);
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
		for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		if ((PageFrame->dirty==1) && (PageFrame->fixCount==0))
		{
			//write Page to SM
			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			writeBlock(PageFrame->pageNum, &fHandle, PageFrame->data);
			PageFrame->dirty=0;
			closePageFile(&fHandle);
			bm->totalWriteCount++;
		}
		//free(PageFrame);
	}
	return RC_OK;
}

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
		for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		if (PageFrame->pageNum == page->pageNum)
		{
			PageFrame->dirty=1;
			//free(PageFrame);
			break;
		}
		//free(PageFrame);
	}
	return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	
		for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		if (PageFrame->pageNum == page->pageNum)
		{
			SM_FileHandle fHandle;
			openPageFile(bm->pageFile, &fHandle);
			writeBlock(PageFrame->pageNum, &fHandle, PageFrame->data);
			PageFrame->dirty=0;
			closePageFile(&fHandle);
			bm->totalWriteCount++;
			//free(PageFrame);
			break;
		}
		//free(PageFrame);
	}
	return RC_OK;
}

void FIFO(BM_BufferPool *const bm, BM_PageHandle *page)
{
	BM_PageHandle *frames = (BM_PageHandle*)bm->mgmtData;
	int replaceIdx = (bm->totalReadCount) % (bm->numPages);

	for(int i = 0; i < bm->numPages; i++)
	{
		if(frames[replaceIdx].fixCount == 0)
		{
			if(frames[replaceIdx].dirty == 1)
			{
				SM_FileHandle fHandle;
				openPageFile(bm->pageFile, &fHandle);
				writeBlock(frames[replaceIdx].pageNum, &fHandle, frames[replaceIdx].data);
				closePageFile(&fHandle);
				bm->totalWriteCount++;
			}
			frames[replaceIdx].dirty = page->dirty;
			frames[replaceIdx].fixCount = page->fixCount;
			frames[replaceIdx].pageNum = page->pageNum;
			frames[replaceIdx].data = page->data;
			break;
		}
		else
		{
			replaceIdx++;
			if(replaceIdx % bm->numPages == 0){
				replaceIdx = 0;
			}
		}
	}
	return;
}

void LRU(BM_BufferPool *const bm, BM_PageHandle *page)
{
	
	BM_PageHandle *frames = (BM_PageHandle*)bm->mgmtData;	
	int replaceIdx = 0;
	for (int i = 0; i < bm->numPages; i++) {
		if (frames[i].frequency <= frames[replaceIdx].frequency && frames[replaceIdx].fixCount == 0) {
			replaceIdx = i;
		}
	}

	if (frames[replaceIdx].dirty == 1) {
		SM_FileHandle fHandle;
		openPageFile(bm->pageFile, &fHandle);
		writeBlock(frames[replaceIdx].pageNum, &fHandle, frames[replaceIdx].data);
		closePageFile(&fHandle);
		bm->totalWriteCount++;
	}
	frames[replaceIdx].dirty = page->dirty;
	frames[replaceIdx].fixCount = page->fixCount;
	frames[replaceIdx].pageNum = page->pageNum;
	frames[replaceIdx].data = page->data;	
	frames[replaceIdx].frequency = ++totalFreq;
	return;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum)
		{
			int pageExists=0;
			BM_PageHandle *PageFrame = (BM_PageHandle *)(bm->mgmtData);
			for (int i=0;i<bm->numPages;i++)
			{
				PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
				if (PageFrame->pageNum == page->pageNum)
					{
						pageExists=1;
						break;
					}
				
			}
			if (pageExists==0)
			{
				PageFrame = (BM_PageHandle *)calloc(1,sizeof(BM_PageHandle));
				PageFrame->dirty=0;
				PageFrame->fixCount=1;
				PageFrame->pageNum=-1;
				PageFrame->frequency = ++totalFreq;
				PageFrame->data=(SM_PageHandle *)calloc(PAGE_SIZE,sizeof(char));
			}
			if (bm->strategy==RS_FIFO)
			{
				FIFO(bm,PageFrame);
			}
			else if(bm->strategy==RS_LRU)
			{
				LRU(bm,PageFrame);
			}
			PageFrame->fixCount=1;
			page->data=PageFrame->data;
			page->pageNum=PageFrame->pageNum;
			//free(PageFrame);
			return RC_OK;
		}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	for (int i=0;i<bm->numPages;i++)
	{
		BM_PageHandle *PageFrame = (BM_PageHandle *)(i+bm->mgmtData);
		if (PageFrame->pageNum == page->pageNum)
		{
			PageFrame->fixCount=PageFrame->fixCount-1;
			//free(PageFrame);
			break;
		}
		//free(PageFrame);
	}
	return RC_OK;
}

PageNumber * getFrameContents ( BM_BufferPool * const bm){
	BM_PageHandle* frames = (BM_PageHandle*)bm->mgmtData;
	PageNumber* frameContent = (PageNumber*)calloc(bm->numPages,sizeof(PageNumber));
	
	for(int i=0; i < bm->numPages; i++){
		frameContent[i] = frames[i].pageNum;
	}
	
	return frameContent;
}

bool * getDirtyFlags ( BM_BufferPool * const bm){
	BM_PageHandle* frames = (BM_PageHandle*)bm->mgmtData;
	bool* dirtyFlags = (bool*)calloc(bm->numPages,sizeof(bool));
	
	for(int i=0; i< bm->numPages; i++){
		if(frames[i].dirty == 1){
			dirtyFlags[i] = true;
		}
		else{
			dirtyFlags[i] = false;
		}
	}
	return dirtyFlags;
}


int * getFixCounts ( BM_BufferPool * const bm) {
	BM_PageHandle* frames = (BM_PageHandle*)bm->mgmtData;
	int* fixCounts = (int*)calloc(bm->numPages , sizeof(int));
	
	for (int i = 0; i < bm->numPages; i++) {
		fixCounts[i] = frames[i].fixCount;
	}
	return fixCounts;
	
}

int getNumReadIO (BM_BufferPool *const bm){
	return bm->totalReadCount;
}

int getNumWriteIO (BM_BufferPool *const bm){
	return bm->totalWriteCount;
}
