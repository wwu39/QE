
#include "qe.h"
#include <cstring>
#include <iostream>
using namespace std;

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

/*
                              Filter
*/

Filter::Filter(Iterator* input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    rm = RelationManager::instance();
    tableName = input->tableName;

    // find the type of lhs
    vector<Attribute> attrs;
    input->getAttributes(attrs);
    for (uint i = 0; i < attrs.size(); ++i) {
        if (condition.lhsAttr == attrs[i].name) {
            type = attrs[i].type;
            break;
        }
    }

    leftAttrNameNoTbl = condition.lhsAttr.substr(tableName.size() + 1);
    if (condition.bRhsIsAttr) {
        rightAttrNameNoTbl = condition.rhsAttr.substr(tableName.size() + 1);
    }
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
    while(1) {
        if (input->getNextTuple(data)) return QE_EOF;
        rm->readAttribute(tableName, input->rid, leftAttrNameNoTbl, lhsValue);
        // NO_OP is always true
        if (condition.op == NO_OP) return SUCCESS;
        // lhs attr value is null
        if (lhsValue[0]) return QE_EOF;
        if (condition.bRhsIsAttr) {
            rm->readAttribute(tableName, input->rid, rightAttrNameNoTbl, rhsValue);
            if (!rhsValue[0]) continue; // if rhs attr is null, fetch next tuple
            if (satisfyCondition(type, lhsValue + 1, rhsValue + 1, condition.op)) return SUCCESS;
            else continue;
        } else {
            if (type != condition.rhsValue.type) return QE_EOF; // type mismatched
            if (satisfyCondition(type, lhsValue + 1, condition.rhsValue.data, condition.op)) return SUCCESS;
            else continue;
        }
    }
    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    input->getAttributes(attrs);
}

/*
                              Project
*/

Project::Project(Iterator *input, const vector<string> &attrNames) {

}

Project::~Project() {

}

RC Project::getNextTuple(void *data) {
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const {

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
