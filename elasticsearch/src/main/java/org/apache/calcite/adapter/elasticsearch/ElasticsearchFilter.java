/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Implementation of a {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Elasticsearch.
 */
public class ElasticsearchFilter extends Filter implements ElasticsearchRel {
  ElasticsearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
                      RexNode condition) {
    super(cluster, traitSet, child, condition);
    assert getConvention() == ElasticsearchRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override
  public Filter copy(RelTraitSet relTraitSet, RelNode input, RexNode condition) {
    return new ElasticsearchFilter(getCluster(), relTraitSet, input, condition);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    ObjectMapper mapper = implementor.elasticsearchTable.mapper;
    PredicateAnalyzerTranslator translator = new PredicateAnalyzerTranslator(mapper);
    try {
      implementor.add(translator.translateMatch(this, implementor.relContext));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (PredicateAnalyzer.ExpressionNotAnalyzableException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * New version of translator which uses visitor pattern
   * and allow to process more complex (boolean) predicates.
   */
  static class PredicateAnalyzerTranslator {
    private final ObjectMapper mapper;

    PredicateAnalyzerTranslator(final ObjectMapper mapper) {
      this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    /**
     * We need the node which expression belongs to and the node, so we use node as parameter
     *
     * @return Query string
     * @throws IOException                                        ignore
     * @throws PredicateAnalyzer.ExpressionNotAnalyzableException ignore
     */
    QueryBuilders.QueryBuilder translateMatch(Filter filter, ElasticsearchImplementContext relContext) throws IOException,
        PredicateAnalyzer.ExpressionNotAnalyzableException {
      return QueryBuilders.constantScoreQuery(PredicateAnalyzer.analyze(filter.getCondition(), filter, relContext));
    }
  }

}

// End ElasticsearchFilter.java
