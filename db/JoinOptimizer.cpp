#include <db/JoinOptimizer.h>
#include <db/PlanCache.h>
#include <cfloat>

using namespace db;

double JoinOptimizer::estimateJoinCost(int card1, int card2, double cost1, double cost2) {
  return cost1+card1*cost2+card1*card2;
}

int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp,
                                                const std::string &table1Alias, const std::string &table2Alias,
                                                const std::string &field1PureName, const std::string &field2PureName,
                                                int card1, int card2, bool t1pkey, bool t2pkey,
                                                const std::unordered_map<std::string, TableStats> &stats,
                                                const std::unordered_map<std::string, int> &tableAliasToId) {
  switch (joinOp) {
    case Predicate::Op::EQUALS:
      if(!t1pkey&&!t2pkey){
        return std::max(card1,card2);
      }else if(!t2pkey){
        return card2;
      }else if(!t1pkey){
        return card1;
      }else{
        return std::min(card1,card2);
      }
    case Predicate::Op::NOT_EQUALS:
      if(!t1pkey && !t2pkey){
        return card1 * card2 - std::max(card1,card2);
      }else if (!t2pkey){
        return card2*card1 - card2;
      }else if(!t1pkey){
        return card1*card2 - card1;
      }else{
        return card1*card2 - std::min(card1,card2);
      }
    default:
      break;
  }

  return (int)(0.3 * card1 * card2);
}

std::vector<LogicalJoinNode> JoinOptimizer::orderJoins(std::unordered_map<std::string, TableStats> stats,
                                                       std::unordered_map<std::string, double> filterSelectivities) {
  auto size = joins.size();
  PlanCache plan_cache{};
  CostCard *best_card = nullptr;
  for(auto i=0;i<size; ++i){
    for(auto & s: enumerateSubsets(joins,i + 1)){
      double best_cost = 1000000000.0f;
      best_card = nullptr;
      for(const LogicalJoinNode &node:s){
        auto card = computeCostAndCardOfSubplan(stats, filterSelectivities, node, s, best_cost, plan_cache);
        if(!card){
          continue;
        }
        if(!best_card || card->cost < best_cost){
          best_cost = card->cost;
          best_card = card;
        }
      }
      if (best_card) plan_cache.addPlan(s, best_card);
    }
  }
  if (best_card) return best_card->plan;
  return {};
}
