#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) : p_(p), child_(child){

}

Predicate *Filter::getPredicate() {
    return &p_;
}

const TupleDesc &Filter::getTupleDesc() const {
    return child_->getTupleDesc();
}

void Filter::open() {
    child_->open();

    Operator::open();
}

void Filter::close() {
    child_->close();
    Operator::close();
}

void Filter::rewind() {
    child_->rewind();
    Operator::rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    return {child_};
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    child_ = children[0];
}

std::optional<Tuple> Filter::fetchNext() {
  while(child_->hasNext()){
    Tuple item = child_->next();
    if(p_.filter(item)){
      return {item};
    }
  }
    return std::nullopt;
}
