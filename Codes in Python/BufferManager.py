from StorageManager import *


class BM_PageHandle:
    def __init__(self):
        self.pageNum = 0
        self.data = None
        self.dirty = 0
        self.fixCount = 0
        self.frequency = 0

    def getPageNum(self):
        return self.pageNum

    def setPageNum(self, pageNum):
        self.pageNum = pageNum

    def getData(self):
        return self.data

    def setData(self, data):
        self.data = data

    def getDirty(self):
        return self.dirty

    def setDirty(self, dirty):
        self.dirty = dirty

    def getFixCount(self):
        return self.fixCount

    def setFixCount(self, fixCount):
        self.fixCount = fixCount

    def getFrequency(self):
        return self.frequency

    def setFrequency(self, frequency):
        self.frequency = frequency

class BM_BufferPool:

    def __init__(self, pageFileName, numPages, strategy, stratData):

        if os.path.exists(pageFileName+".txt"):
            self.numPages = numPages
            self.pageFile = pageFileName
            self.strategy = strategy
            self.totalReadCount = 0
            self.totalWriteCount = 0
            self.stratData = stratData
            self.mgmtData = []

            for i in range(numPages):
                fileHandle = SM_FileHandle()
                fileHandle.setFileName(pageFileName)
                ph = BM_PageHandle()
                ph.setPageNum(-1)
                ph.setData(fileHandle)
                ph.setDirty(0)
                ph.setFixCount(0)
                ph.setFrequency(0)
                self.mgmtData.append(ph)
            print("BUFFER POOL INITIALIZED")
        else:
            print("ERROR IN BUFFER POOL INITIALIZATION")

    def getPageFile(self):
        return self.pageFile

    def setPageFile(self, pageFile):
        self.pageFile = pageFile

    def getNumPages(self):
        return self.numPages

    def setNumPages(self, numPages):
        self.numPages = numPages

    def getStrategy(self):
        return self.strategy

    def setStrategy(self, strategy):
        self.strategy = strategy

    def getMgmtData(self):
        return self.mgmtData

    def setMgmtData(self, mgmtData):
        self.mgmtData = mgmtData

    def getTotalReadCount(self):
        return self.totalReadCount

    def setTotalReadCount(self, totalReadCount):
        self.totalReadCount = totalReadCount

    def getTotalWriteCount(self):
        return self.totalWriteCount

    def setTotalWriteCount(self, totalWriteCount):
        self.totalWriteCount = totalWriteCount





# ---------------------------------------------------
class bufferManager:

    def __init__(self, pageFileName, numPages, strategy, stratData):
        self.BM_BufferPool=BM_BufferPool( pageFileName, numPages, strategy, stratData)
        self.totalFreq=0

    def shutdownBufferPool(self):
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getFixCount() == 1):
                print("BUFFER POOL SHUTDOWN FAILED")
            elif (pageFrame.getFixCount() == 0 and pageFrame.getDirty() == 1):
                fHandle = pageFrame.getData()
                storageManager().writeBlock(pageFrame.getPageNum(), fHandle)
                pageFrame.setDirty(0)
                storageManager().closePageFile(fHandle)
                self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)
        print("BUFFER POOL IS SHUTDOWN")

    def forceFlushPool(self):
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getFixCount() == 1):
                print("BUFFER POOL FORCE FLUSH FAILED")
            elif (pageFrame.getFixCount() == 0 and pageFrame.getDirty() == 1):
                fHandle = pageFrame.getData()
                storageManager().writeBlock(pageFrame.getPageNum(), fHandle)
                pageFrame.setDirty(0)
                storageManager().closePageFile(fHandle)
                self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)
        print("BUFFER POOL IS SHUTDOWN")

    def markDirty(self, page):
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getPageNum() == page.getPageNum()):
                pageFrame.setDirty(1)
                print("MARK DIRTY OK")
                return pageFrame

    def forcePage(self, page):
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getPageNum() == page.getPageNum()):
                fHandle = pageFrame.getData()
                storageManager().writeBlock(pageFrame.getPageNum()+1, fHandle)
                pageFrame.setDirty(0)
                storageManager().closePageFile(fHandle)
                self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)
                print("FORCE PAGE OK")
                break

    def FIFO(self, page):
        frames = self.BM_BufferPool.getMgmtData()
        replaceIdx = self.BM_BufferPool.getTotalReadCount() % self.BM_BufferPool.getNumPages()
        for i in range(self.BM_BufferPool.getNumPages()):
            if (frames[replaceIdx].getFixCount() == 0):
                if (frames[replaceIdx].getDirty() == 1):
                    fHandle = frames[replaceIdx].getData()
                    storageManager().writeBlock(frames[replaceIdx].getPageNum(), fHandle)
                    storageManager().closePageFile(fHandle)
                    self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)

                frames[replaceIdx].setDirty(page.getDirty())
                frames[replaceIdx].setFixCount(page.getFixCount())
                frames[replaceIdx].setPageNum(page.getPageNum())
                frames[replaceIdx].setData(page.getData())
                break
            else:
                replaceIdx = replaceIdx + 1
                if (replaceIdx % self.BM_BufferPool.getNumPages() == 0):
                    replaceIdx = 0
        print("FIFO OK")
        return

    def LRU(self, page):
        frames = self.BM_BufferPool.getMgmtData()
        replaceIdx = 0
        for i in range(self.BM_BufferPool.getNumPages()):
            if ((frames[i].getFrequency() <= frames[replaceIdx].getFrequency()) and (
                    frames[replaceIdx].getFixCount() == 0)):
                replaceIdx = i

        if (frames[replaceIdx].getDirty() == 1):
            fHandle = frames[replaceIdx].getData()
            storageManager().writeBlock(frames[replaceIdx].getPageNum(), fHandle)
            storageManager().closePageFile(fHandle)
            self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)

        frames[replaceIdx].setDirty(page.getDirty())
        frames[replaceIdx].setFixCount(page.getFixCount())
        frames[replaceIdx].setPageNum(page.getPageNum())
        frames[replaceIdx].setData(page.getData())
        frames[replaceIdx].setFrequency(self.totalFreq + 1)
        print("LRU OK")
        return

    def pinPage(self, page):
        pageExists = 0
        pageFrame = BM_PageHandle()
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getPageNum() == page.getPageNum()):
                pageExists = 1
                break
        if (pageExists == 0):
            fileHandle = SM_FileHandle()

            ph = BM_PageHandle()
            ph.setPageNum(-1)
            ph.setData(fileHandle)
            ph.setDirty(0)
            ph.setFixCount(1)
            ph.setFrequency(self.totalFreq + 1)
            pageFrame = ph

        if (self.BM_BufferPool.getStrategy() == "RS_FIFO"):
            self.FIFO(pageFrame)
        elif (self.BM_BufferPool.getStrategy() == "RS_LRU"):
            self.LRU(pageFrame)
        pageFrame.setFixCount(1)
        page.setData(pageFrame.getData())
        page.setPageNum(pageFrame.getPageNum())
        print("PIN PAGE OK")
        return page

    def unpinPage(self, page):
        for i in range(self.BM_BufferPool.getNumPages()):
            pageFrame = self.BM_BufferPool.getMgmtData()[i]
            if (pageFrame.getPageNum() == page.getPageNum()):
                if (pageFrame.getDirty() == 1):
                    fHandle = page.getData()
                    # f = open(fHandle.getFileName() + ".txt", "a")
                    # f.truncate(0)
                    # f.close()
                    storageManager().writeBlock(pageFrame.getPageNum(), fHandle)
                    pageFrame.setDirty(0)
                    storageManager().closePageFile(fHandle)
                    self.BM_BufferPool.setTotalWriteCount(self.BM_BufferPool.getTotalWriteCount() + 1)
                pageFrame.setFixCount(pageFrame.getFixCount() - 1)
                print("UNPIN PAGE OK")
                break

            else:
                print("UNPIN PAGE NOT FOUND")

    def getFrameContents(self):
        frames = self.BM_BufferPool.getMgmtData()
        frameContent = []
        for i in range(self.BM_BufferPool.getNumPages()):
            frameContent[i] = frames[i].getPageNum()
        return frameContent

    def getDirtyFlags(self):
        frames = self.BM_BufferPool.getMgmtData()
        dirtyFlags = []
        for i in range(self.BM_BufferPool.getNumPages()):
            if frames[i].getDirty() == 1:
                dirtyFlags[i] = True
            else:
                dirtyFlags[i] = False

        return dirtyFlags

    def getFixCounts(self):
        frames = self.BM_BufferPool.getMgmtData()
        fixCounts = []
        for i in range(self.BM_BufferPool.getNumPages()):
            fixCounts[i] = frames[i].getFixCount()
        return fixCounts

    def getNumReadIO(self):
        return self.BM_BufferPool.getTotalReadCount()

    def getNumWriteIO(self):
        return self.BM_BufferPool.getTotalWriteCount()


# -------------------------------------------------
sm = storageManager()
sm.createPageFile("testbuffer")
ph = BM_PageHandle()

bm1 = bufferManager("testbuffer", 22, "RS_FIFO", None)

for i in range(22):
    bm1.pinPage(ph)
    bm1.unpinPage(ph)
bm1.shutdownBufferPool()

bm2 = bufferManager("testbuffer", 3, "RS_FIFO", None)
bm2.pinPage(ph)
# bm2.markDirty(ph)
bm2.unpinPage(ph)
bm2.forcePage(ph)
bm2.shutdownBufferPool()