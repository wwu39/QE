
#include "qe.h"
/*
                              Filter
*/

Filter::Filter(Iterator* input, const Condition &condition) {

}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {
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
