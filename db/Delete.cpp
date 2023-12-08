#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child): tid_(t), child_(child) {

}

const TupleDesc &Delete::getTupleDesc() const {
  return child_->getTupleDesc();
}

void Delete::open() {
  child_->open();
  Operator::open();
}

void Delete::close() {
  child_->close();
  Operator::close();
}

void Delete::rewind() {
  child_->rewind();
  Operator::rewind();
}

std::vector<DbIterator *> Delete::getChildren() {
  return {child_};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
  child_ = children[0];
}

std::optional<Tuple> Delete::fetchNext() {
  if (child_->hasNext()) {
    auto t = child_->next();
    Database::getBufferPool().deleteTuple(tid_, &t);
    return {t};
  }

  return std::nullopt;
}
