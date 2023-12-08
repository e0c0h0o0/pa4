#include <db/JoinPredicate.h>
#include <db/Tuple.h>

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2) : f1_(field1), jop_(op), f2_(field2) {
}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
  return t1->getField(f1_).compare(jop_, &t2->getField(f2_));
}

int JoinPredicate::getField1() const {
  return f1_;
}

int JoinPredicate::getField2() const {
  return f2_;
}

Predicate::Op JoinPredicate::getOperator() const {
  return jop_;
}
