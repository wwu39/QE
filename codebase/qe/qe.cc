
#include "qe.h"
#include <cstring>
#include <cmath>
#include <iostream>
using namespace std;

static int proj_count = 0;
static int filter_count = 0;
static int inljoin_count = 0;

/*
                              Filter
*/

Filter::Filter(Iterator* input, const Condition &condition) {
    cond = condition;

    // create a temp rbf file for operation
    rbf->RecordBasedFileManager::instance();
    tempFileName = "Temp_filter_" + to_string(filter_count++);
    rbf->createFile(tempFileName);
    // open the file
    rbf->openFile(tempFileName, fileHandle);
    // insert all tuples from the input to temp file
    char data[PAGE_SIZE];
    input->getAttributes(recordDescriptor);
    vector<string> attrNames;
    int totalSize = recordDescriptor.size();
    for (uint i = 0; i < totalSize; ++i)
        attrNames.push_back(recordDescriptor[i].name); // we need all attributes
    while (input->getNextTuple(data) != QE_EOF) { // inserting
        rbf->insertRecord(fileHandle, recordDescriptor, data, rid);
    }

    // filtering
    for (int i = 0; i < totalSize; ++i) { // figure out type of lhs
        if (cond.lhsAttr == recordDescriptor[i].name) {
            ltype = recordDescriptor[i].type;
            break;
        }
    }
    if (cond.bRhsIsAttr) { // if rhs is an sttr
        // we need to scan everything to determine
        rbf->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, attrNames, rbfmsi);
        for (int i = 0; i < totalSize; ++i) { // figure out type of rhs
            if (cond.rhsAttr == recordDescriptor[i].name) {
                rtype = recordDescriptor[i].type;
                break;
            }
        }
    } else { // if rhs is a value
        rbf->scan(fileHandle, recordDescriptor, cond.lhsAttr, cond.op, cond.rhsValue.data, attrNames, rbfmsi);
        rtype = cond.rhsValue.type;  // figure out type of rhs
    }
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
    if (!cond.bRhsIsAttr) {  // if rhs is a value
        if (rbfmsi.getNextRecord(rid, data)) {
            // if hit the end, close temp file and delete it
            rbfmsi.close();
            rbf->closeFile(fileHandle);
            rbf->destroyFile(tempFileName);
            return QE_EOF;
        }
        if (cond.op == NO_OP) return SUCCESS; // NO_OP always true regardless lhs and rhs
        if (ltype != rtype) return QE_EOF; // check type, if not match, QE_EOF
        return SUCCESS;
    } else { // if rhs is an sttr
        while (1) {
            if (rbfmsi.getNextRecord(rid, data)) {
                // if hit the end, close temp file and delete it
                rbfmsi.close();
                rbf->closeFile(fileHandle);
                rbf->destroyFile(tempFileName);
                return QE_EOF;
            }
            if (cond.op == NO_OP) return SUCCESS; // NO_OP always true regardless lhs and rhs
            if (ltype != rtype) return QE_EOF; // check type, if not match, QE_EOF
            // read attrs and compare
            rbf->readAttribute(fileHandle, recordDescriptor, rid, cond.lhsAttr, lhsData);
            rbf->readAttribute(fileHandle, recordDescriptor, rid, cond.rhsAttr, rhsData);
            if (lhsData[0] || rhsData[0]) continue; // if either is null, fetch next tuple
            if (satisfyCondition(ltype, lhsData + 1, rhsData + 1, cond.op)) return SUCCESS;
            else continue;
        }
    }
    // never reach
    return SUCCESS;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = recordDescriptor;
}

/*
                              Project
*/

Project::Project(Iterator *input, const vector<string> &attrNames) {
    // create a temp rbf file for operation
    rbf->RecordBasedFileManager::instance();
    tempFileName = "Temp_projection_" + to_string(proj_count++);
    rbf->createFile(tempFileName);
    // open the file
    rbf->openFile(tempFileName, fileHandle);
    // insert all tuples from the input to temp file
    char data[PAGE_SIZE];
    vector<Attribute> recordDescriptor;
    input->getAttributes(recordDescriptor);
    uint j = 0;
    for (uint i = 0; i < recordDescriptor.size(); ++i) { // BTW fill projAttrs
        if (j == attrNames.size()) break;
        if (recordDescriptor[i].name == attrNames[j]) {
            projAttrs.push_back(recordDescriptor[i]);
            ++j;
        }
    }
    while (input->getNextTuple(data) != QE_EOF) { // inserting
        rbf->insertRecord(fileHandle, recordDescriptor, data, rid);
    }
    // scan the temp file with proj attrs
    rbf->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, attrNames, rbfmsi);
}

Project::~Project() {

}

RC Project::getNextTuple(void *data) {
    if (rbfmsi.getNextRecord(rid, data)) {
        // if hit the end, close temp file and delete it
        rbfmsi.close();
        rbf->closeFile(fileHandle);
        rbf->destroyFile(tempFileName);
        return QE_EOF;
    }
    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = projAttrs;
}

/*
                              INLJoin
*/

INLJoin::INLJoin (Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    cond = condition; 

    // assemble record descriptors
    vector<string> attrNames;
    vector<Attribute> leftAttrs;
    // vector<string> leftAttrNames;
    leftIn->getAttributes(leftAttrs);
    int leftSize = leftAttrs.size();
    for (int i = 0; i < leftSize; ++i) {
        recordDescriptor.push_back(leftAttrs[i]);
        // leftAttrNames.push_back(leftAttrs[i].name);
        attrNames.push_back(leftAttrs[i].name);
    }
    vector<Attribute> rightAttrs;
    vector<string> rightAttrNames; // we need rightAttrNames when joining
    rightIn->getAttributes(rightAttrs);
    int rightSize = rightAttrs.size();
    for (int i = 0; i < rightSize; ++i) {
        recordDescriptor.push_back(rightAttrs[i]);
        rightAttrNames.push_back(rightAttrs[i].name);
        attrNames.push_back(rightAttrs[i].name);
    }
    int totalSize = leftSize + rightSize;

    // create a temp rbf file for operation
    rbf->RecordBasedFileManager::instance();
    tempFileName = "Temp_inljoin_" + to_string(inljoin_count++);
    rbf->createFile(tempFileName);

    // we need to store rightIn for a moment
    string rightInfileName = tempFileName + "~";
    rbf->createFile(rightInfileName);
    FileHandle rfh;
    rbf->openFile(rightInfileName, rfh);
    char rdata[PAGE_SIZE];
    while (rightIn->getNextTuple(rdata) != QE_EOF) {
        RID rid;
        rbf->insertRecord(rfh, rightAttrs, rdata, rid);
    }

    // start joining
    rbf->openFile(tempFileName, fileHandle);
    char ldata[PAGE_SIZE];
    while (leftIn->getNextTuple(ldata) != QE_EOF) {
        // scan through the rightIn tuples for each leftIn tuples
        RBFM_ScanIterator right;
        rbf->scan(rfh, rightAttrs, "", NO_OP, NULL, rightAttrNames, right);
        RID rid;
        while (right.getNextRecord(rid, rdata) != RBFM_EOF) {
            // join ldata and rdata

            int NISize = getNullIndicatorSize(totalSize);
            char NI[NISize];
            memset(NI, 0, NISize); // all 0s

            int leftNISize = getNullIndicatorSize(leftSize);
            char leftNI[leftNISize];
            memcpy(leftNI, ldata, leftNISize);

            int rightNISize = getNullIndicatorSize(rightSize);
            char rightNI[leftNISize];
            memcpy(rightNI, rdata, rightNISize);


            char data[PAGE_SIZE];
            int offset = NISize;
            int leftoff = leftNISize;
            int rightoff = rightNISize;

            // start joining
            for (int i = 0; i < leftSize; ++i) {
                if (fieldIsNull(leftNI, i)) {
                    setFieldNull(NI, i); // set null bit
                } else {
                    // copy from ldata to data
                    int vclen;
                    switch (recordDescriptor[i].type) {
                        case TypeInt: 
                            memcpy(data + offset, ldata + leftoff, INT_SIZE);
                            offset += INT_SIZE;
                            leftoff += INT_SIZE;
                            break;
                        case TypeReal:
                            memcpy(data + offset, ldata + leftoff, REAL_SIZE);
                            offset += REAL_SIZE;
                            leftoff += REAL_SIZE;
                            break;
                        case TypeVarChar:
                            memcpy(&vclen, ldata + leftoff, 4);
                            memcpy(data + offset, ldata + leftoff, vclen + 4);
                            offset += (vclen + 4);
                            leftoff += (vclen + 4);
                            break;
                    }
                }
            }
            for (int i = leftSize; i < totalSize; ++i) {
                if (fieldIsNull(rightNI, i - leftSize)) {
                    setFieldNull(NI, i); // set null bit
                } else {
                    // copy from rdata to data
                    int vclen;
                    switch (recordDescriptor[i].type) {
                        case TypeInt: 
                            memcpy(data + offset, rdata + rightoff, INT_SIZE);
                            offset += INT_SIZE;
                            rightoff += INT_SIZE;
                            break;
                        case TypeReal:
                            memcpy(data + offset, rdata + rightoff, REAL_SIZE);
                            offset += REAL_SIZE;
                            rightoff += REAL_SIZE;
                            break;
                        case TypeVarChar:
                            memcpy(&vclen, rdata + rightoff, 4);
                            memcpy(data + offset, rdata + rightoff, vclen + 4);
                            offset += (vclen + 4);
                            rightoff += (vclen + 4);
                            break;
                    }
                }
            }
            // finally copy NI into data
            memcpy(data, NI, NISize);

            // finish joining, insert record
            rbf->insertRecord(fileHandle, recordDescriptor, data, rid);
        }
        right.close();
    }
    // finish joining so we can delete rightIn file
    rbf->closeFile(rfh);
    rbf->destroyFile(rightInfileName);

    // same as filter now
    for (int i = 0; i < totalSize; ++i) { // figure out type of lhs
        if (cond.lhsAttr == recordDescriptor[i].name) {
            ltype = recordDescriptor[i].type;
            break;
        }
    }
    if (cond.bRhsIsAttr) { // if rhs is an sttr
        // we need to scan everything to determine
        rbf->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, attrNames, rbfmsi);
        for (int i = 0; i < totalSize; ++i) { // figure out type of rhs
            if (cond.rhsAttr == recordDescriptor[i].name) {
                rtype = recordDescriptor[i].type;
                break;
            }
        }
    } else { // if rhs is a value
        rbf->scan(fileHandle, recordDescriptor, cond.lhsAttr, cond.op, cond.rhsValue.data, attrNames, rbfmsi);
        rtype = cond.rhsValue.type;  // figure out type of rhs
    }
}

INLJoin::~INLJoin () {

}

RC INLJoin::getNextTuple(void *data) {
    if (!cond.bRhsIsAttr) {  // if rhs is a value
        if (rbfmsi.getNextRecord(rid, data)) {
            // if hit the end, close temp file and delete it
            rbfmsi.close();
            rbf->closeFile(fileHandle);
            rbf->destroyFile(tempFileName);
            return QE_EOF;
        }
        if (cond.op == NO_OP) return SUCCESS; // NO_OP always true regardless lhs and rhs
        if (ltype != rtype) return QE_EOF; // check type, if not match, QE_EOF
        return SUCCESS;
    } else { // if rhs is an sttr
        while (1) {
            if (rbfmsi.getNextRecord(rid, data)) {
                // if hit the end, close temp file and delete it
                rbfmsi.close();
                rbf->closeFile(fileHandle);
                rbf->destroyFile(tempFileName);
                return QE_EOF;
            }
            if (cond.op == NO_OP) return SUCCESS; // NO_OP always true regardless lhs and rhs
            if (ltype != rtype) return QE_EOF; // check type, if not match, QE_EOF
            // read attrs and compare
            rbf->readAttribute(fileHandle, recordDescriptor, rid, cond.lhsAttr, lhsData);
            rbf->readAttribute(fileHandle, recordDescriptor, rid, cond.rhsAttr, rhsData);
            if (lhsData[0] || rhsData[0]) continue; // if either is null, fetch next tuple
            if (satisfyCondition(ltype, lhsData + 1, rhsData + 1, cond.op)) return SUCCESS;
            else continue;
        }
    }
    // never reach
    return SUCCESS;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = recordDescriptor;
}

bool INLJoin::fieldIsNull(char *nullIndicator, int i)
{
    return (nullIndicator[i / CHAR_BIT] & (1 << (CHAR_BIT - 1 - (i % CHAR_BIT)))) != 0;
}

void INLJoin::setFieldNull(char *nullIndicator, int i)
{
    nullIndicator[i / CHAR_BIT] |= (1 << (CHAR_BIT - 1 - (i % CHAR_BIT)));
}

int INLJoin::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

int Iterator::keyCompare(const AttrType type, const void * key1, const void * key2)
{
    // given an attribute, compare two keys
    // key1 > key2 -> pos
    // key1 < key2 -> neg
    // key1 == key2 -> 0
    switch (type) {
        case TypeInt: 
            if(*(int *)key1 < *(int *)key2) return -1;
            else if(*(int *)key1 > *(int *)key2) return 1;
            else return 0;
        case TypeReal:
            if(*(float *)key1 < *(float *)key2) return -1;
            else if(*(float *)key1 > *(float *)key2) return 1;
            else return 0;
        case TypeVarChar:
        {
            int vclen;
            memcpy(&vclen, key1, 4);
            char key1str[vclen + 1];
            memcpy(key1str, (char *)key1 + sizeof(int), vclen);
            key1str[vclen]='\0';
            int vclen2;
            memcpy(&vclen2, key2, 4);
            char key2str[vclen2 + 1];
            memcpy(key2str, (char *)key2 + sizeof(int), vclen2);
            key2str[vclen2]='\0';

            return strcmp(key1str, key2str);
        }
    }
    return -1;
}

bool Iterator::satisfyCondition(const AttrType type, const void * value1, const void * value2, CompOp op)
{
    int rc = keyCompare(type, value1, value2);
    switch (op) {
        case EQ_OP: if (rc == 0) return true; break;
        case LT_OP: if (rc < 0) return true; break;
        case LE_OP: if (rc <= 0) return true; break;
        case GT_OP: if (rc > 0) return true; break;
        case GE_OP: if (rc >= 0) return true; break;
        case NE_OP: if (rc != 0) return true; break;
        case NO_OP: return true; break;
    }
    return false;
}