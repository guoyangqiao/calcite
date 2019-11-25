package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

/**
 * Tag which will decide the result of {@link PredicateAnalyzer.PromisedQueryExpression#builder()}
 */
public enum AnalyzePredication {
  CHILDREN_AGGREGATION(AnalyzePredicationConditionKey.ChildTypeJoinEquation, AnalyzePredicationConditionKey.RootIdSelection);

  private Object[] requiredConditions;

  AnalyzePredication(Object... conditions) {
    this.requiredConditions = conditions;
  }

  public static class AnalyzePredicationCondition {
    private final AnalyzePredication predication;
    private Set<Object> conditions;

    public AnalyzePredicationCondition(AnalyzePredication predication) {
      this.predication = predication;
      this.conditions = new HashSet<>();
    }

    public boolean add(Object cdnObj) {
      return conditions.add(cdnObj);
    }

    public boolean allMatched() {
      return conditions.containsAll(Sets.newHashSet(predication.requiredConditions));
    }
  }


  static class AnalyzePredicationConditionKey {
    final static String ChildTypeJoinEquation = "childTypeJoinEquation";
    final static String RootIdSelection = "rootIdSelection";
  }
}
