#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) : pred_(p), child1_(child1), child2_(child2){
    td_ = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
    tp1_ = std::nullopt;
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    return &pred_;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    return td_;
}

std::string HashEquiJoin::getJoinField1Name() {
    return child1_->getTupleDesc().getFieldName(pred_.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    return child2_->getTupleDesc().getFieldName(pred_.getField2());
}

void HashEquiJoin::open() {
    child1_->open();
    child2_->open();
    Operator::open();
}

void HashEquiJoin::close() {

    child2_->close();
    child1_->close();
    Operator::close();
}

void HashEquiJoin::rewind() {
    child1_->rewind();
    child2_->rewind();
    tp1_ = std::nullopt;
    Operator::rewind();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    return {child1_, child2_};
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    if (children.empty()) return;
    child1_ = children[0];
    if (children.size() > 1) {
        child2_ = children[1];
    }
}

std::optional<Tuple> HashEquiJoin::fetchNext() {

    if (tp1_ == std::nullopt) {
        if (!child1_->hasNext()) return std::nullopt;
        tp1_ = child1_->next();
    }
    for (;;) {
        Tuple tp1 = tp1_.value();
        while (child2_->hasNext()) {
            Tuple tp2 = child2_->next();
            if(pred_.filter(&tp1, &tp2)){
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
                return merge;
            }
        }
        if (!child1_->hasNext()) return std::nullopt;
        tp1_ = child1_->next();
        child2_->rewind();
    }
}
