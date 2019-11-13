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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rules and relational operators for
 * {@link ElasticsearchRel#CONVENTION ELASTICSEARCH}
 * calling convention.
 */
class ElasticsearchRules {
  static final RelOptRule[] RULES = {
      ElasticsearchSortRule.INSTANCE,
      ElasticsearchFilterRule.INSTANCE,
      ElasticsearchProjectRule.INSTANCE,
      ElasticsearchAggregateRule.INSTANCE,
      ElasticsearchFilterLikeToMatchRule.INSTANCE
  };

  private ElasticsearchRules() {
  }


  /**
   * Returns 'string' if it is a call to item['string'], null otherwise.
   *
   * @param call current relational expression
   * @return literal value
   */
  private static String isItemCall(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.getOperands().get(0);
    final RexNode op1 = call.getOperands().get(1);

    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  /**
   * Checks if current node represents item access as in {@code _MAP['foo']} or
   * {@code cast(_MAP['foo'] as integer)}
   *
   * @return true if expression is item, false otherwise
   */
  static boolean isItem(RexNode node) {
    final Boolean result = node.accept(new RexVisitorImpl<Boolean>(false) {
      @Override
      public Boolean visitCall(final RexCall call) {
        return isItemCall(uncast(call)) != null;
      }
    });
    return Boolean.TRUE.equals(result);
  }

  static boolean isCase(RexNode node) {
    final Boolean result = node.accept(new RexVisitorImpl<Boolean>(false) {
      @Override
      public Boolean visitCall(final RexCall call) {
        return SqlKind.CASE == call.getOperator().getKind();
      }
    });
    return Boolean.TRUE.equals(result);
  }

  /**
   * Unwraps cast expressions from current call. {@code cast(cast(expr))} becomes {@code expr}.
   */
  private static RexCall uncast(RexCall maybeCast) {
    if (maybeCast.getKind() == SqlKind.CAST && maybeCast.getOperands().get(0) instanceof RexCall) {
      return uncast((RexCall) maybeCast.getOperands().get(0));
    }

    // not a cast
    return maybeCast;
  }

  static List<String> elasticsearchFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override
          public String get(int index) {
            final String name = rowType.getFieldList().get(index).getName();
            return name.startsWith("$") ? "_" + name.substring(2) : name;
          }

          @Override
          public int size() {
            return rowType.getFieldCount();
          }
        },
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  static String quote(String s) {
    return "\"" + s + "\"";
  }

  static String stripQuotes(String s) {
    return s.length() > 1 && s.startsWith("\"") && s.endsWith("\"")
        ? s.substring(1, s.length() - 1) : s;
  }

  /**
   * Translator from {@link RexNode} to strings in Elasticsearch's expression
   * language.
   */
  static class RexToElasticsearchTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;
    private final List<RelOptTable> allTables;
    private final ObjectMapper mapper;

    RexToElasticsearchTranslator(JavaTypeFactory typeFactory, List<String> inFields, List<RelOptTable> tables, ObjectMapper mapper) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
      this.allTables = tables;
      this.mapper = mapper;
    }

    @Override
    public String visitLiteral(RexLiteral literal) {
      if (literal.getValue() == null) {
        return "null";
      }
      return "\"literal\":\""
          + RexToLixTranslator.translateLiteral(literal, literal.getType(),
          typeFactory, RexImpTable.NullAs.NOT_POSSIBLE)
          + "\"";
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return quote(inFields.get(inputRef.getIndex()));
    }

    @Override
    public String visitCall(RexCall call) {
      final String name = isItemCall(call);
      if (name != null) {
        return name;
      }
      if (call.getKind() == SqlKind.CASE) {
        final QueryBuilders.QueryBuilder rexBuilder;
        try {
          rexBuilder = PredicateAnalyzer.analyze(call, allTables);
        } catch (Throwable e) {
          throw new IllegalArgumentException("Translation of " + call
              + " is not supported by ElasticsearchProject", e);
        }
        assert rexBuilder != null;
        try {
          StringWriter writer = new StringWriter();
          JsonGenerator generator = mapper.getFactory().createGenerator(writer);
          rexBuilder.writeJson(generator);
          generator.flush();
          generator.close();
          return writer.toString();
        } catch (Throwable ignored) {
          throwIllegalException(call);
        }
      }
      final List<String> strings = visitList(call.operands);

      if (call.getKind() == SqlKind.CAST) {
        return call.getOperands().get(0).accept(this);
      }

      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op1 = call.getOperands().get(1);
        final SqlTypeName sqlTypeName = op1.getType().getSqlTypeName();
        if (op1 instanceof RexLiteral && (sqlTypeName == SqlTypeName.INTEGER || sqlTypeName == SqlTypeName.CHAR)) {
          return stripQuotes(strings.get(0)) + "[" + ((RexLiteral) op1).getValue2() + "]";
        }
      }
      return throwIllegalException(call);
    }

    private String throwIllegalException(RexCall call) {
      throw new IllegalArgumentException("Translation of " + call
          + " is not supported by ElasticsearchProject");
    }

    List<String> visitList(List<RexNode> list) {
      final List<String> strings = new ArrayList<>();
      for (RexNode node : list) {
        strings.add(node.accept(this));
      }
      return strings;
    }
  }

  /**
   * Base class for planner rules that convert a relational expression to
   * Elasticsearch calling convention.
   */
  abstract static class ElasticsearchConverterRule extends ConverterRule {
    final Convention out;

    ElasticsearchConverterRule(Class<? extends RelNode> clazz, RelTrait in, Convention out,
                               String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to an
   * {@link ElasticsearchSort}.
   */
  private static class ElasticsearchSortRule extends ElasticsearchConverterRule {
    private static final ElasticsearchSortRule INSTANCE =
        new ElasticsearchSortRule();

    private ElasticsearchSortRule() {
      super(Sort.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchSortRule");
    }

    @Override
    public RelNode convert(RelNode relNode) {
      final Sort sort = (Sort) relNode;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
      return new ElasticsearchSort(relNode.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation(),
          sort.offset, sort.fetch);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to an
   * {@link ElasticsearchFilter}.
   */
  private static class ElasticsearchFilterRule extends ElasticsearchConverterRule {
    private static final ElasticsearchFilterRule INSTANCE = new ElasticsearchFilterRule();

    private ElasticsearchFilterRule() {
      super(LogicalFilter.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchFilterRule");
    }

    @Override
    public RelNode convert(RelNode relNode) {
      final LogicalFilter filter = (LogicalFilter) relNode;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new ElasticsearchFilter(relNode.getCluster(), traitSet,
          convert(filter.getInput(), out),
          filter.getCondition());
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * to an {@link ElasticsearchAggregate}.
   */
  private static class ElasticsearchAggregateRule extends ElasticsearchConverterRule {
    static final RelOptRule INSTANCE = new ElasticsearchAggregateRule();

    private ElasticsearchAggregateRule() {
      super(LogicalAggregate.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet = agg.getTraitSet().replace(out);
      try {
        return new ElasticsearchAggregate(
            rel.getCluster(),
            traitSet,
            convert(agg.getInput(), traitSet.simplify()),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        return null;
      }
    }
  }


  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to an {@link ElasticsearchProject}.
   */
  private static class ElasticsearchProjectRule extends ElasticsearchConverterRule {
    private static final ElasticsearchProjectRule INSTANCE = new ElasticsearchProjectRule();

    private ElasticsearchProjectRule() {
      super(LogicalProject.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchProjectRule");
    }

    @Override
    public RelNode convert(RelNode relNode) {
      final LogicalProject project = (LogicalProject) relNode;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new ElasticsearchProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(), project.getRowType());
    }
  }

  /**
   * Rule to modify {@link ElasticsearchFilter} condition.
   * Implemented:
   * a field with LIKEs to single elasticsearch MATCH semantic
   */
  private static class ElasticsearchFilterLikeToMatchRule extends RelOptRule {
    private static final ElasticsearchFilterLikeToMatchRule INSTANCE = new ElasticsearchFilterLikeToMatchRule();

    ElasticsearchFilterLikeToMatchRule() {
      super(operand(Filter.class, any()), "ElasticsearchFilterLikeToMatchRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter rel = call.rel(0);
      final RexBuilder rexBuilder = call.builder().getRexBuilder();
      final RexShuttle shuttle = new RexShuttle() {
        @Override
        public RexNode visitCall(RexCall rexCall) {
          final SqlOperator operator = rexCall.getOperator();
          if (SqlStdOperatorTable.AND.equals(operator) || SqlStdOperatorTable.OR.equals(operator)) {
            final List<RexNode> conditionGroup = rexCall.getOperands();
            if (allLike(conditionGroup) && equivalentInput(conditionGroup)) {
              try {
                final String matchStr = conditionGroup.stream().map(x -> {
                  final RexNode rexNode = ((RexCall) x).getOperands().get(1);
                  if (rexNode instanceof RexLiteral) {
                    return ElasticsearchConstants.trimPercentSign(((RexLiteral) rexNode).getValueAs(String.class));
                  }
                  throw new IllegalArgumentException("Not compatible with non literal");
                }).collect(Collectors.joining(ElasticsearchConstants.WHITE_SPACE));
                return call.builder().
                    getRexBuilder().
                    makeCall(
                        SqlStdOperatorTable.AND.equals(operator) ? ElasticsearchConstants.MATCH_AND_SQL_OPERATOR : ElasticsearchConstants.MATCH_OR_SQL_OPERATOR,
                        ((RexCall) conditionGroup.get(0)).getOperands().get(0),
                        rexBuilder.makeLiteral(matchStr)
                    );
              } catch (IllegalArgumentException t) {
                //ok, not like contains some unusual value, return to normal case
              }
            }
          }
          return super.visitCall(rexCall);
        }
      };
      final RelNode accept = rel.accept(shuttle);
      call.transformTo(accept);
    }

    private boolean equivalentInput(List<RexNode> conditionGroup) {
      return conditionGroup.stream().map(x -> {
        if (x instanceof RexCall) {
//          CAST(ITEM($0, 'all_uni_shop_id')):VARCHAR(65535)
          try {
            final RexNode rexNode = (RexNode) ((List) ((RexCall) ((List) ((RexCall) ((List) ((RexCall) x).operands).get(0)).operands).get(0)).operands).get(0);
            if (rexNode instanceof RexInputRef) {
              return ((RexInputRef) rexNode).getIndex();
            }
          } catch (Throwable t) {
            //desire failed
          }
        }
        return -1;
      }).collect(Collectors.toSet()).size() == 1;
    }

    private boolean allLike(List<RexNode> conditionGroup) {
      return conditionGroup.stream().allMatch(x -> (x instanceof RexCall && ((RexCall) x).getOperator().equals(SqlStdOperatorTable.LIKE)));
    }
  }

}

// End ElasticsearchRules.java
