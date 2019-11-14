package org.apache.calcite.adapter.elasticsearch;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Tag which will decide the result of {@link PredicateAnalyzer.PromisedQueryExpression#builder()}
 */
public enum AnalyzePredication {
  CHILDREN_AGGREGATION("childTypeJoin", "RootIdSelection");

  private String[] requiredConditions;

  AnalyzePredication(String... conditions) {
    this.requiredConditions = conditions;
  }

  public static class AnalyzePredicationCondition {
    private final AnalyzePredication predication;
    private List<Supplier<String>> conditions;

    public AnalyzePredicationCondition(AnalyzePredication predication) {
      this.predication = predication;
      this.conditions = new ArrayList<>();
    }

    public boolean add(Supplier<String> stringSupplier) {
      return conditions.add(stringSupplier);
    }

    public boolean allMatched() {
      Set<String> matchedConditions = conditions.stream().map(Supplier::get).collect(Collectors.toSet());
      return Sets.newHashSet(predication.requiredConditions).containsAll(matchedConditions);
    }
  }
}