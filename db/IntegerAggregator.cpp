#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
  int gbfield_;
  std::optional<Types::Type>  gbfield_type_;
  Aggregator::Op what_;
  TupleDesc td_;
  const std::unordered_map<const Field *, std::vector<const Field *>> &group_;
  std::vector<Tuple> results_;
  std::vector<Tuple>::const_iterator iterator_;
public:
  IntegerAggregatorIterator(int gbfield,
                            std::optional<Types::Type> gbfield_type,
                            Aggregator::Op what,
                            const std::unordered_map<const Field *, std::vector<const Field *>> &group) :
    gbfield_(gbfield),gbfield_type_(gbfield_type), what_(what), group_(group), results_(){
    iterator_ = results_.end();
    if (gbfield == -1) {
      td_ = TupleDesc({Types::INT_TYPE});
    } else {
      td_ = TupleDesc({gbfield_type.value(), Types::INT_TYPE});
    }
  }

  void open() override {
    results_.clear();

    for (const auto & it : group_) {
      const Field *gfield = it.first;
      const auto &v = it.second;
      results_.emplace_back(td_);
      Tuple &tup = results_.back();
      int i = 0;
      int agg_val, val;
      switch (what_) {
        case Aggregator::Op::COUNT:
          agg_val = (int)v.size();
          break;
        case Aggregator::Op::MIN:
          agg_val = INT32_MAX;
          for (auto it1 : v) {
            it1->serialize(&val);
            if (val < agg_val) agg_val = val;
          }
          break;
        case Aggregator::Op::MAX:
          agg_val = INT32_MIN;
          for (auto it1 : v) {
            it1->serialize(&val);
            if (val > agg_val) agg_val = val;
          }
          break;
        case Aggregator::Op::SUM:
          agg_val = 0;
          for (auto it1 : v) {
            it1->serialize(&val);
            agg_val += val;
          }
          break;
        case Aggregator::Op::AVG:
          agg_val = 0;
          for (auto it1 : v) {
            it1->serialize(&val);
            agg_val += val;
          }
          agg_val /= v.size();
          break;
      }

      if(gbfield_ != -1 && gfield != nullptr){
        tup.setField(i++, gfield);
      }
      tup.setField(i,new IntField(agg_val));
    }

    iterator_ = results_.begin();

  }

  bool hasNext() override {
    return iterator_ != results_.end();
  }

  Tuple next() override {
    auto tup = *iterator_;
    iterator_++;
    return tup;
  }

  void rewind() override {
    iterator_ = results_.begin();
  }

  const TupleDesc &getTupleDesc() const override {
    return td_;
  }

  void close() override {
    results_.clear();
    iterator_ = results_.end();
  }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what): gbfield_(gbfield), gbfieldtype_(gbfieldtype), afield_(afield), what_(what), group_(){
  iterator_ = new IntegerAggregatorIterator(gbfield_, gbfieldtype_, what_, group_);
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {

  const Field *gfield = gbfield_ == -1 ? nullptr : &tup->getField(gbfield_);
  const Field *afield = &tup->getField(afield_);
  if (group_.find(gfield) == group_.end()) {
    group_.insert({gfield, {}});
  }
  group_[gfield].push_back(afield);

}

DbIterator *IntegerAggregator::iterator() const {
  return static_cast<DbIterator*>(iterator_);
}

IntegerAggregator::~IntegerAggregator() {
  delete(static_cast<IntegerAggregatorIterator*>(iterator_));
}
