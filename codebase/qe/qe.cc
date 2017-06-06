
#include "qe.h"
#include <cstring>
#include <cmath>

int Iterator::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool Iterator::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

bool Iterator::getValueByAttrName(const vector<Attribute> &attrs, const string &attrName, const void * tuple, void * value)
{
    // get the value by attribute name
    // value doesn't contain null bit
    // if value is null return true

    int fieldCount = attrs.size();
    int offset = getNullIndicatorSize(fieldCount);
    // get null indicator
    char NI[offset];
    memcpy(NI, tuple, offset);
    int i = 0;
    // find the attribute with given attrName
    for (; i < fieldCount; ++i) {
        if (attrs[i].name == attrName) break;
        if (fieldIsNull(NI, i)) continue;
        switch (attrs[i].type) {
            case TypeInt: offset += INT_SIZE; break;
            case TypeReal: offset += REAL_SIZE; break;
            case TypeVarChar:
            {
                int vcl;
                memcpy(&vcl, (char *)tuple + offset, 4);
                offset += (vcl + 4);
                break;
            }
        }
    }
    // copy its value
    if (fieldIsNull(NI, i)) return true;
    switch (attrs[i].type) {
        case TypeInt: 
            memcpy(value, (char *)tuple + offset, INT_SIZE); 
            break;
        case TypeReal: 
            memcpy(value, (char *)tuple + offset, REAL_SIZE); 
            break;
        case TypeVarChar:
        {
            int vcl;
            memcpy(&vcl, (char *)tuple + offset, 4);
            memcpy(value, (char *)tuple + offset, vcl + 4); 
            break;
        }
    }
    return false;
}

int Iterator::keyCompare(const Attribute& attr, const void * key1, const void * key2)
{
    // given an attribute, compare two keys
    // key1 > key2 -> pos
    // key1 < key2 -> neg
    // key1 == key2 -> 0
    switch (attr.type) {
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

bool Iterator::satisfyCondition(const Attribute& attr, const void * value1, const void * value2, CompOp op)
{
    int rc = keyCompare(attr, value1, value2);
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
    attrs = input->getOriAttr();
    for (uint i = 0; i < attrs.size(); ++i) {
        if (condition.lhsAttr == attrs[i].name) {
            condAttr = attrs[i];
            break;
        }
    }
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
    while (1) {
        if (input->getNextTuple(data)) return QE_EOF;
        if (condition.bRhsIsAttr) { // attributes comparsion
            // finding two attributes and compare them
            // if satisfy condition, return success
            // else continue
            bool lhsIsNull = getValueByAttrName(attrs, condition.lhsAttr, data, lhsValue);
            bool rhsIsNull = getValueByAttrName(attrs, condition.rhsAttr, data, rhsValue);
            if (lhsIsNull || rhsIsNull) continue;
            if (satisfyCondition(condAttr, lhsValue, rhsValue, condition.op)) return SUCCESS;
            else continue;
        } else { // attribute compares against value
            bool lhsIsNull = getValueByAttrName(attrs, condition.lhsAttr, data, lhsValue);
            if (lhsIsNull) continue;
            if (satisfyCondition(condAttr, lhsValue, condition.rhsValue.data, condition.op)) return SUCCESS;
            else continue;
        }
    }
    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    
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
