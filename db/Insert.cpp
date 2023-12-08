#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
  if(child_->hasNext()){
    Tuple tp = child_->next();
    Database::getBufferPool().insertTuple(tid_,tableId_,&tp);

  }
  return {};
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) : tid_(t), child_(child), tableId_(tableId){

}

const TupleDesc &Insert::getTupleDesc() const {
  return child_->getTupleDesc();
}

void Insert::open() {
  child_->open();
  Operator::open();
}

void Insert::close() {
  child_->close();
  Operator::close();
}

void Insert::rewind() {
  child_->rewind();
  Operator::rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
  return {child_};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
  if (children.empty()) child_ = nullptr;
  child_ = children[0];
}
