package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tag which will decide the result of {@link PredicateAnalyzer.PromisedQueryExpression#builder()}
 */
public enum AnalyzePredication {
  CHILDREN_AGGREGATION("childTypeJoinEquation", "RootIdSelection");

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

    public boolean addAll(Collection<?> c) {
      return conditions.addAll(c);
    }

    public boolean add(Object cdnObj) {
      return conditions.add(cdnObj);
    }

    public boolean allMatched() {
      return conditions.containsAll(Sets.newHashSet(predication.requiredConditions));
    }
  }
}

class AnalyzePredicationCondition {
  final static String childTypeJoinEquation = "childTypeJoinEquation";
  final static String RootIdSelection = "rootIdSelection";
}