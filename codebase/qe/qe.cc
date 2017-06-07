
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
    for (uint i = 0; i < recordDescriptor.size(); ++i)
        attrNames.push_back(recordDescriptor[i].name); // we need all attributes
    while (input->getNextTuple(data) != QE_EOF) { // inserting
        RID rid;
        rbf->insertRecord(fileHandle, recordDescriptor, data, rid);
    }
    // scan the temp file with proj attrs
    if (condition.bRhsIsAttr) { // if rhs is an sttr
        RID rid;
        char rhsValue_data[PAGE_SIZE];
        rbf->readAttribute(fileHandle, recordDescriptor, rid, condition.rhsAttr, rhsValue_data); // read rhs
        rbf->scan(fileHandle, recordDescriptor, condition.lhsAttr, condition.op, rhsValue_data, attrNames, rbfmsi);
    } else {
        rbf->scan(fileHandle, recordDescriptor, condition.lhsAttr, condition.op, condition.rhsValue.data, attrNames, rbfmsi);
    }
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
    RID rid;
    if (rbfmsi.getNextRecord(rid, data)) {
        // if hit the end, close temp file and delete it
        rbfmsi.close();
        rbf->closeFile(fileHandle);
        rbf->destroyFile(tempFileName);
        return QE_EOF;
    }
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
        RID rid;
        rbf->insertRecord(fileHandle, recordDescriptor, data, rid);
    }
    // scan the temp file with proj attrs
    rbf->scan(fileHandle, recordDescriptor, "", NO_OP, NULL, attrNames, rbfmsi);
}

Project::~Project() {

}

RC Project::getNextTuple(void *data) {
    RID rid;
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

}

INLJoin::~INLJoin () {

}

RC INLJoin::getNextTuple(void *data) {
    return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {

}
