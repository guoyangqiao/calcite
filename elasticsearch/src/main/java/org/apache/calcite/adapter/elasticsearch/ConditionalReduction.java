package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;

/**
 * Tag which will decide the result of {@link PredicateAnalyzer.PromisedQueryExpression#builder()}
 */
public enum ConditionalReduction {
  CHILDREN_AGGREGATION(ConditionKey.CHILD_TYPE_JOIN_EQUATION, ConditionKey.ROOT_ID_SELECTION),
  HAS_CHILD();

  private Object[] requiredConditions;

  ConditionalReduction(Object... conditions) {
    this.requiredConditions = conditions;
  }

  static class ConditionCollector {
    private final ConditionalReduction predication;
    private Map<Object, Object> conditions;

    ConditionCollector(ConditionalReduction predication) {
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


  static class ConditionKey {
    final static String CHILD_TYPE_JOIN_EQUATION = "childTypeJoinEquation";
    final static String ROOT_ID_SELECTION = "rootIdSelection";
  }
}
