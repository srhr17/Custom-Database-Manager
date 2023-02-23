import os

class SM_FileHandle:
    def __init__(self):
        self.fileName = ""
        self.totalPages = 0
        self.curPos = 0
        self.mgmtInfo = None
    def getFileName(self):
        return self.fileName
    def setFileName(self,name):
        self.fileName=name
    def getTotalPages(self):
        return self.totalPages
    def setTotalPages(self,totalPages):
        self.totalPages=totalPages
    def getCurPos(self):
        return self.curPos
    def setCurPos(self,curpos):
        self.curPos=curpos
    def getMgmtInfo(self):
        return self.mgmtInfo
    def setMgmtInfo(self,mgmtInfo):
        self.mgmtInfo=mgmtInfo

class storageManager:
    def __init__(self):
        self.PAGESIZE=4096
        self.pageHandle=""
    def createPageFile(self,fileName):
        fileName+=".txt"
        f=open(fileName,"a")
        f.close()
        return "Page Creation Successful!"

    def openPageFile(self,fileName):
        fileName += ".txt"
        f=open(fileName,"r")
        if not os.path.exists(fileName):
            print("FILE NOT FOUND")
        fileHandle=SM_FileHandle()
        fileHandle.setFileName(fileName)
        fileSize=os.stat(fileName).st_size
        if (fileSize%self.PAGESIZE)>0:
            fileHandle.setTotalPages((fileSize/self.PAGESIZE)+1)
        else:
            fileHandle.setTotalPages(fileSize / self.PAGESIZE)
        fileHandle.setCurPos(0)
        fileHandle.setMgmtInfo(f)
        print("Page Open : OK")
        return fileHandle

    def closePageFile(self,fileHandle):
        if os.path.exists(fileHandle.getFileName()):
            open(fileHandle.getFileName(),"r").close()
            fileHandle.setMgmtInfo(None)
        else:
            print("Close Page : File Not Found")
        print("Close Page : OK")

    def destroyPageFile(self,fileHandle):
        if os.path.exists(fileHandle.getFileName()):
            os.remove(fileHandle.getFileName())
        else:
            print("Destroy Page : File Not Found")
        print("Destroy Page : OK")

    def readBlock(self,pageNum,fileHandle):
        if os.path.exists(fileHandle.getFileName()):
            if pageNum<0 or pageNum>fileHandle.getTotalPages():
                print("Read Block : Tried to read non existing page")
                return fileHandle,''
            f=open(fileHandle.getFileName(),"r")
            if pageNum<0:
                pageNum=0
            f.seek(pageNum*self.PAGESIZE)
            # f=open(fileHandle.getFileName(),"r")
            content=f.readline()
            f.close()
            print("read Block : OK")
            fileHandle.setCurPos(pageNum)
            return fileHandle,content
        else:
            print("Read Block : File Not Found")
            return fileHandle, ''

    def getBlockPos(self,fileHandle):
        if os.path.exists(fileHandle.getFileName()):
            print("Get Block Position : OK")
            return fileHandle.getCurPos()
        else:
            print("Get Block Position : File Not Found")
            return 0

    def readFirstBlock(self,fileHandle):
        return self.readBlock(0,fileHandle)

    def readPreviousBlock(self,fileHandle):
        return self.readBlock(fileHandle.getCurPos()-1, fileHandle)

    def readCurrentBlock(self,fileHandle):
        return self.readBlock(fileHandle.getCurPos(), fileHandle)

    def readNextBlock(self,fileHandle):
        return self.readBlock(fileHandle.getCurPos()+1, fileHandle)

    def makeData(self,data):
        memPage=""
        memPage+=data.getFileName()
        memPage+="#"
        memPage+=str(data.getTotalPages())
        memPage+="#"
        memPage+=str(data.getCurPos())
        memPage+="#"
        memPage+=str(data.getMgmtInfo())
        memPage+="$"
        return memPage

    def writeBlock(self,pageNum,fileHandle):

        if os.path.exists(fileHandle.getFileName()+".txt"):
            f=open(fileHandle.getFileName()+".txt","a")
            if pageNum<0:
                pageNum=0

            print("curPos : ",pageNum*self.PAGESIZE)
            f.seek(pageNum*self.PAGESIZE)
            if type(fileHandle.getMgmtInfo())!='str':
                fileHandle.setMgmtInfo(str(fileHandle.getMgmtInfo()))
            print(" In write block : ", fileHandle.getMgmtInfo())
            f.write(fileHandle.getMgmtInfo())
            f.close()
            fileHandle.setCurPos(pageNum)
            print("Write Block : OK")
            return fileHandle,1
        else:
            print("Write Block : File Not Found")
            return fileHandle, 0

    def writeCurrentBlock(self,fileHandle):
        pageNum=self.getBlockPos(fileHandle)
        if os.path.exists(fileHandle.getFileName()):
            f=open(fileHandle.getFileName(),"a")
            print("curPos : ",pageNum*self.PAGESIZE)
            f.seek(pageNum*self.PAGESIZE)
            f.write(fileHandle.getMgmtInfo())
            f.close()
            fileHandle.setCurPos(pageNum)
            print("Write Block : OK")
            return fileHandle,1
        else:
            print("Write Block : File Not Found")
            return fileHandle,0

    def appendEmptyBlock(self,fileHandle):
        fileHandle.setTotalPages(fileHandle.getTotalPages()+1)
        return fileHandle

    def ensureCapacity(self,NumberOfPages,fileHandle):
        if fileHandle.getTotalPages()>=NumberOfPages:
            print("Ensure Capacity : OK")
            return fileHandle,1
        fileHandle.setTotalPages(NumberOfPages)
        return fileHandle,1


#Testing - Needs proper ordering
#
# storageManager().openPageFile("Hello")
# test_FileHandle=SM_FileHandle()
# test_FileHandle.setFileName("Hello.txt")
# f=open("Hello.txt","r")
# test_FileHandle.setMgmtInfo(f)
# test_FileHandle.setCurPos(0)
# test_FileHandle.setTotalPages(1)
# storageManager().closePageFile(test_FileHandle)
# test_FileHandle,content=storageManager().readBlock(0,test_FileHandle)
# print("Block Position",storageManager().getBlockPos(test_FileHandle))
# k="a"*4096
# test_FileHandle,flag=storageManager().writeBlock(1,test_FileHandle,test_FileHandle)
# print("Block Position",storageManager().getBlockPos(test_FileHandle))
# test_FileHandle,content=storageManager().readBlock(1,test_FileHandle)
# test_FileHandle,content=storageManager().readNextBlock(test_FileHandle)