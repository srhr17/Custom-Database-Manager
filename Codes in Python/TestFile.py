from StorageManager import *
from BufferManager import *
from RecordManager import *


recordManager=RecordManager()
schema,flag=recordManager.createSchema(2,["a","b","c"],["DT_INT","DT_STRING","DT_INT"],[1,4,1])
mytable=recordManager.createTable("MyTable",schema)
mytable,flag=recordManager.openTable("MyTable")
if flag==1:
    print(" Opened Table Succesfully ")
print(" Record Size : ",recordManager.getRecordSize(schema))
print(" Number of Tuples : ",recordManager.getNumTuples(mytable))
myrecord=recordManager.createRecord(schema)
print(" New Record : ",myrecord)
mytable,flag=recordManager.insertRecord(mytable,myrecord)
myrecord2=recordManager.createRecord(schema)
mytable,flag=recordManager.insertRecord(mytable,myrecord2)
myrecord3=recordManager.createRecord(schema)
mytable,flag=recordManager.insertRecord(mytable,myrecord3)
print(" Number of Tuples : ",recordManager.getNumTuples(mytable))
print("This ",mytable.getMgmtData().BM_BufferPool.getMgmtData()[0].getData().getMgmtInfo())
mytable,flag=recordManager.deleteRecord(mytable,myrecord)
print(" Number of Tuples : ",recordManager.getNumTuples(mytable))
recordManager.getRecord(mytable,myrecord)
mytable,flag=recordManager.deleteRecord(mytable,myrecord)
print(recordManager.getRecord(mytable,myrecord))
recordInserted=recordManager.setAttr(schema,3,[1,"Hello",3])
print(recordInserted.getData()[0].getValue())
print(recordInserted.getData()[1].getValue())
print(recordInserted.getData()[2].getValue())
print(" Get Attr : ",recordManager.getAttr(0))
recordManager.closeTable(mytable)
recordManager.deleteTable(mytable.getName())
