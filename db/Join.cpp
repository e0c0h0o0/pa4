#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) : p_(p), child1_(child1), child2_(child2), tp_list_(){
  td_ = TupleDesc::merge(child1->getTupleDesc(),child2->getTupleDesc());
  iterator_ = tp_list_.end();
}

JoinPredicate *Join::getJoinPredicate() {
  return p_;
}

std::string Join::getJoinField1Name() {
  int field = p_->getField1();
  return child1_->getTupleDesc().getFieldName(field);
}

std::string Join::getJoinField2Name() {
  int field = p_->getField2();
  return child2_->getTupleDesc().getFieldName(field);
}

const TupleDesc &Join::getTupleDesc() const {
  return td_;
}

void Join::open() {
  child1_->open();
  child2_->open();
  while (child1_->hasNext()){
    Tuple tp1 = child1_->next();
    while (child2_->hasNext()) {
      Tuple tp2 = child2_->next();
      if(p_->filter(&tp1, &tp2)){
        Tuple merge = Tuple(td_);
        auto it1 = tp1.begin();
        auto it2 = tp2.begin();
        int idx = 0;
        while(it1 != tp1.end()){
          merge.setField(idx++, *it1);
          it1 ++;
        }
        while(it2 != tp2.end()){
          merge.setField(idx++, *it2);
          it2 ++;
        }
        tp_list_.push_back(merge);
      }
    }
    child2_->rewind();
  }
  iterator_ = tp_list_.begin();

  Operator::open();
}

void Join::close() {
  // some code goes here
  child1_->close();
  child2_->close();
  tp_list_.clear();
  iterator_ = tp_list_.end();
  Operator::close();
}

void Join::rewind() {
  child1_->rewind();
  child2_->rewind();
  iterator_ = tp_list_.begin();
  Operator::rewind();
}

std::vector<DbIterator *> Join::getChildren() {
  return {child1_, child2_};
}

void Join::setChildren(std::vector<DbIterator *> children) {
  child1_ = children[0];
  if (children.size() > 1) {
    child2_ = children[1];
  }
}

std::optional<Tuple> Join::fetchNext() {

  if (iterator_ != tp_list_.end()) {
    auto ret =  *iterator_;
    ++iterator_;
    return ret;
  }
  return std::nullopt;
}
