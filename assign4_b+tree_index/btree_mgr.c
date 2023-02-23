#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"

extern RC initIndexManager (void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

extern RC shutdownIndexManager ()
{
    return RC_OK;
}

extern RC getNumNodes (BTreeHandle *tree, int *result)
{
    *result=tree->countNodes;
    return RC_OK;
}
extern RC getNumEntries (BTreeHandle *tree, int *result)
{
    *result=tree->countEntries;
    return RC_OK;
}
extern RC getKeyType (BTreeHandle *tree, DataType *result)
{
    *result=tree->keyType;
    return RC_OK;
}