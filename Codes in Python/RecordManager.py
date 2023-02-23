from StorageManager import *
from BufferManager import *
import sys

DataTypes={'DT_INT':0,'DT_STRING':1,'DT_FLOAT':2,'DT_BOOL':3}

class RecordManagerScanHandle:
    def __init__(self):
        self.rel=None
        self.mgmtData=None

    def getRelation(self):
        return self.rel

    def setRelation(self,rel):
        self.rel=rel

    def getMgmtData(self):
        return self.mgmtData

    def setMgmtData(self,mgmtData):
        self.mgmtData=mgmtData

class Value:
    def __init__(self):
        self.datatype=None
        self.value=[None,None,None,None]

    def getDataType(self):
        return self.datatype

    def setDataType(self,datatype):
        self.datatype=datatype

    def getValue(self):
        return self.value[DataTypes[self.datatype]]

    def setValue(self,val):
        self.value[DataTypes[self.datatype]]=val

class Table:
    #RM_tabledata
    def __init__(self):
        self.name=""
        self.schema=None
        self.mgmtData=None
        self.tupleCount=0


    def getName(self):
        return self.name

    def setName(self,name):
        self.name=name

    def getSchema(self):
        return self.schema

    def setSchema(self,schema):
        self.schema=schema

    def getMgmtData(self):
        return self.mgmtData

    def setMgmtData(self,mgmtData):
        self.mgmtData=mgmtData

    def getTupleCount(self):
        return self.tupleCount

    def incrementTupleCount(self):
        self.tupleCount+=1

    def decrementTupleCount(self):
        self.tupleCount-=1

class Schema:
    def __init__(self):
        self.NumOfAttr=0
        self.AttrNames=[]
        self.AttrDtypes=[]
        self.AttrSizes=[]

    def getNumOfAttr(self):
        return self.NumOfAttr

    def setNumOfAttr(self,numofattr):
        self.NumOfAttr=numofattr

    def getAttrNames(self):
        return self.AttrNames

    def setAttrNames(self,AttrNames):
        self.AttrNames=AttrNames

    def getAttrDtypes(self):
        return self.AttrDtypes

    def setAttrDtypes(self,AttrDtypes):
        self.AttrDtypes=AttrDtypes

    def getAttrSizes(self):
        return self.AttrSizes

    def setAttrSizes(self,attrsizes):
        self.AttrSizes=attrsizes

class Record:
    def __init__(self):
        self.data=[]
        self.page=0
        self.slot=0

    def getData(self):
        return self.data

    def setData(self,val):
        self.data.append(val)

    def getPage(self):
        return self.page

    def setPage(self,page):
        self.page=page

    def getSlot(self):
        return self.slot

    def setSlot(self,slot):
        self.slot=slot

class RecordManager:
    def __init__(self):
        self.storageManager=storageManager()
        self.table=Table()
        self.schema=Schema()
        self.PAGE_SIZE=4096
        self.ref = []

    def createSchema(self,numattr,attrnames,dtypes,attrsizes):
        schema=Schema()
        schema.setNumOfAttr(numattr)
        schema.setAttrNames(attrnames)
        schema.setAttrDtypes(dtypes)
        schema.setAttrSizes(attrsizes)
        print(" Create Schema : OK ")
        return schema,1

    def freeSchema(self,schema):
        del schema
        print(" Free Schema : OK ")

    def createTable(self,name,schema):
        self.storageManager.createPageFile(name)
        self.schema=schema
        # storageFileHandle=self.storageManager.openPageFile(name)
        print("Create Table : OK")
        return self.table

    def openTable(self,name):
        buffer_pool=bufferManager(name,4,"RS_FIFO",None)
        self.table.setName(name)
        self.table.setMgmtData(buffer_pool)
        self.table.setSchema(self.schema)
        print("Open Table : OK")
        return self.table,1

    def closeTable(self,table):
        table.getMgmtData().shutdownBufferPool()
        print("Close Table : OK")
        return 1

    def deleteTable(self,name):
        name=name+".txt"
        if os.path.exists(name):
            os.remove(name)
        else:
            print("Delete Table : File Not Found")
        print("Delete Table : OK")

    def getNumTuples(self,table):
        return table.getTupleCount()

    def createRecord(self,schema):
        newRecord=Record()
        newRecord.setPage(-1)
        newRecord.setSlot(-1)
        print(" Create Record : OK ")
        return newRecord

    def getRecordSize(self,schema):
        sz=0
        dtypes=schema.getAttrDtypes()
        for i in range(len(schema.getAttrDtypes())):
            if dtypes[i]=='DT_INT':
                sz+=sys.getsizeof(int)
            elif dtypes[i]=='DT_FLOAT':
                sz+=sys.getsizeof(float)
            elif dtypes[i]=='DT_BOOL':
                sz+=sys.getsizeof(bool)
            elif dtypes[i]=='DT_STRING':
                sz+=sys.getsizeof(str)
        return sz+len(schema.getAttrNames())

    def insertRecord(self,table,record):
        slotNumber=0
        pageNumber=1
        bp=table.getMgmtData()
        sm_handle=bp.BM_BufferPool.getMgmtData()
        totalRecordLength=self.getRecordSize(table.getSchema())
        flag=0
        bph=0
        for i in range(len(sm_handle)):
            # bp.pinPage(sm_handle[i])
            bph=sm_handle[i]
            sph=bph.getData()
            print(i)
            if sph.getMgmtInfo()==None:
                bp.pinPage(bph)
                record.setPage(i)
                record.setSlot(slotNumber)
                sph.setMgmtInfo(str(record)+"$")
                print(".....",sph.getFileName())
                bph.setData(sph)
                bp.markDirty(bph)
                bp.unpinPage(bph)
                break
        table.incrementTupleCount()
        print(" Insert Record : OK")
        return table,1

    def deleteRecord(self,table,record):
        bp=table.getMgmtData()
        bph=BM_PageHandle()
        bph=bp.pinPage(bph)
        bph=bp.markDirty(bph)
        f = open(bp.BM_BufferPool.getPageFile() + ".txt", "r")
        fread=f.read()
        f.close()
        if str(record) in fread:
            li=fread.split("$")
            li.remove(str(record))
            li='$'.join(li)
            bph.getData().setMgmtInfo(li)
            bph = bp.unpinPage(bph)
            print(" Delete Record : OK")
        else:
            print(" Delete Record : FAILED")

        table.decrementTupleCount()
        print(" Delete Record : OK")
        return table,1

    def updateRecord(self,table,record):
        bp=table.getMgmtData()
        bph=BM_PageHandle()
        recordLength=self.getRecordSize(table.getSchema())
        bph=bp.pinpage(bph,record.getPage())
        sp=bph.getData()+str(recordLength*record.getSlot())
        record.setData(sp)
        print(" Update Record : OK ")
        return table,1

    def getRecord(self,table,record):
        bp=table.getMgmtData()
        bph=bp.BM_BufferPool.getMgmtData()[0]
        recordLength=self.getRecordSize(table.getSchema())
        bph=bp.pinPage(bph)
        sp=str(recordLength*record.getSlot())#bph.getData()+
        f=open(bp.BM_BufferPool.getPageFile()+".txt","r")
        if str(record) in f.read():
            print(" Get Record : OK")
            record.setData(sp)
        else:
            print(" Get Record : FAILED")
        bph=bp.unpinPage(bph)
        return record,1

    def setAttr(self,schema,attrNum,attrlist):
        dtypes=schema.getAttrDtypes()
        myrecord=Record()
        for i in range(attrNum):
            value = Value()
            value.setDataType(dtypes[i])
            value.setValue(attrlist[i])
            myrecord.setData(value)
            self.ref.append(attrlist)
        print(" SetAttr : OK")
        return myrecord

    def getAttr(self,attrNum):
        return self.ref[attrNum]
 
    def startScan(self, table, scan, cond):
        scan.setMgmtData(cond)
        scan.setRelation(table)
        print("Start Scan : OK")
        return scan

    def closeScan(self, scan):
        del scan
        print("Close Scan : OK")

    def next(self, scan, record):
        bp = scan.getRelation().getMgmtData()
        f = open(bp.BM_BufferPool.getPageFile() + ".txt", "r")
        fread = f.read()
        f.close()
        if str(record) in fread:
            li = fread.split("$")
            for i in range(len(li) - 1):
                if (li[i] == str(record)):
                    print("NEXT : OK")
                    return li[i + 1]




