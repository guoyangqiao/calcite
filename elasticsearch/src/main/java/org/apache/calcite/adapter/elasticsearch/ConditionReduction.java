package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;

/**
 * Tag which will decide the result of {@link PredicateAnalyzer.PromisedQueryExpression#builder()}
 */
public enum ConditionReduction {
  CHILDREN_AGGREGATION(AnalyzePredicationConditionKey.CHILD_TYPE_JOIN_EQUATION, AnalyzePredicationConditionKey.ROOT_ID_SELECTION);

  private Object[] requiredConditions;

  ConditionReduction(Object... conditions) {
    this.requiredConditions = conditions;
  }

  static class AnalyzePredicationCondition {
    private final ConditionReduction predication;
    private Map<Object, Object> conditions;

    AnalyzePredicationCondition(ConditionReduction predication) {
      this.predication = predication;
      this.conditions = new HashMap<>();
    }

    public void add(Object cdnObj) {
      add(cdnObj, null);
    }

    public void add(Object cdnObj, Object val) {
      conditions.put(cdnObj, val);
    }

    boolean allMatched() {
      return conditions.keySet().containsAll(Sets.newHashSet(predication.requiredConditions));
    }

    Object forCondition(Object condition) {
      return conditions.get(condition);
    }
  }


  static class AnalyzePredicationConditionKey {
    final static String CHILD_TYPE_JOIN_EQUATION = "childTypeJoinEquation";
    final static String ROOT_ID_SELECTION = "rootIdSelection";
  }
}
