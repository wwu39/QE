
#include "qe.h"
#include <stdlib.h>
/*
                              Filter
*/

Filter::Filter(Iterator* input, const Condition &condition) {
    baseIterator = input;
    baseCondition = condition;
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {

    while(baseIterator->getNextTuple(data) != RM_EOF){
        if(checkSatisfyCondition(data)){
            return SUCCESS;
        }
    }
    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {

}

bool Filter::checkSatisfyCondition(void *nextTuple) {

    int left_index_attribute = 0;

    if((left_index_attribute = get_base_attribute(baseCondition.lhsAttr)) == -1)
        return -1;

    RelationManager *processData = RelationManager::instance();
    void* LeftdataTuple = malloc(PAGE_SIZE);

    if(processData->readAttribute(baseIterator->tableName,baseIterator->rid,baseCondition.lhsAttr,LeftdataTuple))
        return -1;


    if(baseCondition.bRhsIsAttr == true){

        int right_index_attribute = 0;
        if((left_index_attribute = get_base_attribute(baseCondition.rhsAttr)) == -1)
            return -1;

        if(baseIterator->attrs[left_index_attribute].type != baseIterator->attrs[right_index_attribute].type)
            return -1;

        void* rightDataTuple = malloc(PAGE_SIZE);
        if(processData->readAttribute(baseIterator->tableName,baseIterator->rid,baseCondition.rhsAttr,rightDataTuple))
            return -1;

        if(baseIterator->attrs[left_index_attribute].type == TypeInt)
        {
            uint32_t left_int_attribute;
            uint32_t right_int_attribute;
            memcpy(&left_int_attribute,LeftdataTuple,INT_SIZE);
            memcpy(&right_int_attribute,rightDataTuple,INT_SIZE);
            if(baseCondition.op == EQ_OP) {
                if (left_int_attribute == right_index_attribute ){
                        return true;
                } else
                    return false;
            }

        }
        else if (baseIterator->attrs[left_index_attribute].type == TypeReal)
        {

        }
        else if (baseIterator->attrs[left_index_attribute].type == TypeVarChar)
        {

        }

    }
    else{

    }
    return false;
}

int Filter::get_base_attribute(string attributeName) {
    int base_attribute = 0;
    while((baseIterator->attrs[base_attribute].name != attributeName)
          && (base_attribute < baseIterator->attrs.size()))
    {
         base_attribute++;
    }
    if(base_attribute == baseIterator->attrs.size()){
        return -1;
    }

    return base_attribute;
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
