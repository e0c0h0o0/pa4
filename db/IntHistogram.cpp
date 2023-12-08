#include <db/IntHistogram.h>
#include <cmath>
#include <string>
#include <sstream>

using namespace db;

IntHistogram::IntHistogram(int buckets, int min, int max) : n_buckets_(buckets), min_(min), max_(max), n_tuples_(0){

    avg_ = std::ceil((max - min) * 1.0/ (buckets * 1.0));
    double l = min * 1.0;
    buckets_ = new Bucket[n_buckets_];
    for (int i = 0; i < buckets; ++i) {
        buckets_[i].lo_ = l;
        buckets_[i].hi_ = l + avg_;
        l = buckets_[i].hi_;
    }
}

void IntHistogram::addValue(int v) {
    if (v < min_ || v >= max_) return;
    Bucket *bucket = find_bucket(v);
    bucket->in_bucket_ += 1;
    n_tuples_ += 1;
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    const Bucket *bucket = find_bucket_const(v);
    const Bucket *end = buckets_ + n_buckets_;
    double candidates = 0.0;
    switch (op) {
        case Predicate::Op::EQUALS:
            if(!bucket) return 0.0;
            candidates = (bucket->in_bucket_ / bucket->interval());
            break;
        case Predicate::Op::GREATER_THAN:
            if(v< min_){
                return 1.0;
            }else if(v>= max_ || !bucket){
                return 0.0;
            }
            candidates = ((bucket->hi_ - v) / bucket->interval()) * (bucket->in_bucket_ * 1.0);
            for(auto p = bucket + 1; p < end; ++p){
                candidates += (p->in_bucket_ *1.0);
            }
            break;
        case Predicate::Op::GREATER_THAN_OR_EQ:
            if(v<=min_) return 1.0;
            if(v>max_ || !bucket) return 0.0;
            candidates = ((bucket->hi_ - v + 1) / bucket->interval()) * (bucket->in_bucket_ * 1.0);
            for(auto p = bucket + 1; p < end; ++p){
                candidates += (p->in_bucket_ *1.0);
            }
            break;
        case Predicate::Op::LESS_THAN:
            if (v <= min_ || !bucket) return 0.0;
            if (v > max_) return 1.0;
            candidates = ((v- bucket->lo_ * 1.0) / bucket->interval()) * (bucket->in_bucket_);
            for(auto p = buckets_; p < bucket; ++p){
                candidates += (p->in_bucket_ *1.0);
            }
            break;
        case Predicate::Op::LESS_THAN_OR_EQ:
            if (v <= min_ || !bucket) return 0.0;
            if (v > max_) return 1.0;
            candidates = ((v - bucket->lo_ + 1) * 1.0 / bucket->interval()) * (bucket->in_bucket_);
            for(auto p = buckets_; p < bucket; ++p){
                candidates += (p->in_bucket_ *1.0);
            }
            break;
        case Predicate::Op::NOT_EQUALS:
            if(!bucket) return 1.0;
            candidates = 1-(bucket->in_bucket_/ buckets_->interval());
            break;
        default:
            break;
    }

    return candidates / ((double)n_tuples_);
}

double IntHistogram::avgSelectivity() const {
    return avg_;
}

std::string IntHistogram::to_string() const {
    std::stringstream ss{};
    char buf[1024];

    sprintf(buf, "IntHistogram { buckets=%d, min=%d, max=%d, avg=%.2lf, buckets={", n_buckets_, min_, max_, avg_);
    ss << buf;
    for (int i = 0; i < n_buckets_; ++i) {
        ss << buckets_[i].to_string();
    }
    ss << "}";
    return ss.str();
}

IntHistogram::Bucket *IntHistogram::find_bucket(int v) {

    int l = 0;
    int r = n_buckets_-1;

    while(l<=r){
        int mid = (l+r)/2;
        if(buckets_[mid].within_range(v)){
            return &buckets_[mid];
        }else if(buckets_[mid].lo_ > v){
            r  = mid -1;
        }else if(buckets_[mid].hi_ <= v){
            l = mid+1;
        }
    }
    return nullptr;
}

const IntHistogram::Bucket *IntHistogram::find_bucket_const(int v) const {
    int l = 0;
    int r = n_buckets_-1;

    while(l<=r){
        int mid = (l+r)/2;
        if(buckets_[mid].within_range(v)){
            return &buckets_[mid];
        }else if(buckets_[mid].lo_ > v){
            r  = mid -1;
        }else if(buckets_[mid].hi_ <= v){
            l = mid+1;
        }
    }
    return nullptr;
}
