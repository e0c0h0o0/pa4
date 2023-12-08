#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
  // some code goes here
  if(iterator_->hasNext()){
    return {iterator_->next()};
  }
  return std::nullopt;
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) :
  child_(child), afield_(afield), gbfiled_(gfield), aop_(aop){

  auto field_type = (gfield == Aggregator::NO_GROUPING ? std::nullopt: std::optional<Types::Type>(child->getTupleDesc().getFieldType(gfield)));
  aggregator_ = new IntegerAggregator(gfield, field_type, afield, aop);

  iterator_ = aggregator_->iterator();
}

int Aggregate::groupField() {
  return gbfiled_;
}

std::string Aggregate::groupFieldName() {
  if(gbfiled_ == Aggregator::NO_GROUPING){
    return "";
  }
  return child_->getTupleDesc().getFieldName(gbfiled_);
}

int Aggregate::aggregateField() {
  return afield_;
}

std::string Aggregate::aggregateFieldName() {
  return child_->getTupleDesc().getFieldName(afield_);
}

Aggregator::Op Aggregate::aggregateOp() {
  return aop_;
}

void Aggregate::open() {
  child_->open();
  while(child_->hasNext()){
    auto item = child_->next();
    aggregator_->mergeTupleIntoGroup(&item);
  }
  iterator_->open();
  Operator::open();
}

void Aggregate::rewind() {
  iterator_->rewind();
  child_->rewind();
  Operator::rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
  return iterator_->getTupleDesc();
}

void Aggregate::close() {
  iterator_->close();
  child_->close();

  Operator::close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
  std::vector<DbIterator *> ret = {child_};
  return ret;
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
  if (!children.empty()) child_ = children[0];
}
