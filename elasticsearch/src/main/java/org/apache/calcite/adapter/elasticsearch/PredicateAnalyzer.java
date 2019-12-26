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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.elasticsearch.QueryBuilders.BoolQueryBuilder;
import org.apache.calcite.adapter.elasticsearch.QueryBuilders.QueryBuilder;
import org.apache.calcite.adapter.elasticsearch.QueryBuilders.RangeQueryBuilder;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Query predicate analyzer. Uses visitor pattern to traverse existing expression
 * and convert it to {@link QueryBuilder}.
 *
 * <p>Major part of this class have been copied from
 * <a href="https://www.dremio.com/">dremio</a> ES adapter
 * (thanks to their team for improving calcite-ES integration).
 */
class PredicateAnalyzer {

  /**
   * Internal exception
   */
  @SuppressWarnings("serial")
  private static final class PredicateAnalyzerException extends RuntimeException {

    PredicateAnalyzerException(String message) {
      super(message);
    }

    PredicateAnalyzerException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Thrown when {@link org.apache.calcite.rel.RelNode} expression can't be processed
   * (or converted into ES query)
   */
  static class ExpressionNotAnalyzableException extends Exception {
    ExpressionNotAnalyzableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private PredicateAnalyzer() {
  }

  /**
   * Walks the expression tree, attempting to convert the entire tree into
   * an equivalent Elasticsearch query filter. If an error occurs, or if it
   * is determined that the expression cannot be converted, an exception is
   * thrown and an error message logged.
   *
   * <p>Callers should catch ExpressionNotAnalyzableException
   * and fall back to not using push-down filters.
   *
   * @param expression expression will be analyzed
   * @param topNode    node which the expression's belongs to, usually the filter itself, could be null
   * @return search query which canL be used to query ES cluster
   * @throws ExpressionNotAnalyzableException when expression can't processed by this analyzer
   */
  static QueryBuilder analyze(RexNode expression, RelNode topNode, ElasticsearchRel.ElasticsearchImplementContext elasticsearchRelContext) throws ExpressionNotAnalyzableException {
    Objects.requireNonNull(expression, "expression");

    try {
      // visits expression tree
      QueryExpression e = (QueryExpression) expression.accept(new Visitor(topNode, elasticsearchRelContext.analyzePredicationMap));

      if (e != null && e.isPartial()) {
        throw new UnsupportedOperationException("Can't handle partial QueryExpression: " + e);
      }
      return e != null ? e.builder() : null;
    } catch (Throwable e) {
      Throwables.propagateIfPossible(e, UnsupportedOperationException.class);
      throw new ExpressionNotAnalyzableException("Can't convert " + expression, e);
    }
  }

  /**
   * Converts expressions of the form NOT(LIKE(...)) into NOT_LIKE(...)
   */
  private static class NotLikeConverter extends RexShuttle {
    final RexBuilder rexBuilder;

    NotLikeConverter(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getOperator().getKind() == SqlKind.NOT) {
        RexNode child = call.getOperands().get(0);
        if (child.getKind() == SqlKind.LIKE) {
          List<RexNode> operands = ((RexCall) child).getOperands()
              .stream()
              .map(rexNode -> rexNode.accept(NotLikeConverter.this))
              .collect(Collectors.toList());
          return rexBuilder.makeCall(SqlStdOperatorTable.NOT_LIKE, operands);
        }
      }
      return super.visitCall(call);
    }
  }

  /**
   * Traverses {@link RexNode} tree and builds ES query.
   */
  private static class Visitor extends RexVisitorImpl<Expression> {
    static final String ITEM_FUNC = "ITEM";
    static final String CAST_FUNC = "CAST";
    static final String PARENT_FIELD = "PARENT";
    static final String NAME_FIELD = "NAME";
    static final String JOIN_TYPE = "JOIN";
    static final String ID = "ID";
    static final String TYPE_KEY = "type";
    static final String RELATIONS_KEY = "relations";
    private final RelNode topNode;
    private final RelOptTable relOptTable;
    private final EnumMap<ConditionalReduction, ConditionalReduction.ConditionCollector> predicationConditionMap;

    private Visitor(RelNode relNode, EnumMap<ConditionalReduction, ConditionalReduction.ConditionCollector> analyzePredicationMap) {
      super(true);
      List<RelOptTable> tables = RelOptUtil.findAllTables(relNode);
      assert tables.size() == 1;
      this.topNode = relNode;
      this.relOptTable = tables.get(0);
      this.predicationConditionMap = analyzePredicationMap;
    }

    public Expression visitSubQuery(RexSubQuery subQuery) {
      Expression hasChild = predicateHasChild(subQuery);
      if (hasChild != null) {
        return hasChild;
      }
      Expression childrenAggregation = predicateChildrenAggregation(subQuery);
      if (childrenAggregation != null) {
        return childrenAggregation;
      }
      return super.visitSubQuery(subQuery);
    }

    /**
     * Predicate if a subquery could be part of children aggregation
     *
     * @param subQuery
     * @return True if matched, otherwise null
     */
    private Expression predicateChildrenAggregation(RexSubQuery subQuery) {
      final RexNode inField = subQuery.operands.get(0);
      if (subQuery.op instanceof SqlInOperator) {
        if (inField.isA(SqlKind.FIELD_ACCESS)) {
          final RexFieldAccess fieldAccess = (RexFieldAccess) inField;
          final RelDataTypeField field = fieldAccess.getField();
          final RexNode referenceExpr = fieldAccess.getReferenceExpr();
          if (PARENT_FIELD.equalsIgnoreCase(field.getName())) {
            if (referenceExpr instanceof RexCall && SqlKind.CAST == ((RexCall) referenceExpr).getOperator().getKind()) {
              final RexNode castOp1 = ((RexCall) referenceExpr).getOperands().get(0);
              if (((RexCall) castOp1).getOperator().isName(ITEM_FUNC, false)) {
                final List<RexNode> operands = ((RexCall) castOp1).getOperands();
                final RexLiteral literalRef = (RexLiteral) operands.get(1);
                final String fieldName = literalRef.getValueAs(String.class);
                final ElasticsearchMapping mapping = getElasticTable().transport.mapping;
                final Map<String, ElasticsearchMapping.Datatype> fieldMapping = mapping.mapping();
                final ElasticsearchMapping.Datatype datatype = fieldMapping.get(fieldName);
                if (datatype != null) {
                  final JsonNode properties = datatype.properties();
                  if (isJoinType(properties)) {
                    final RelNode subQueryNode = RelCopyShuttle.copyOf(subQuery.rel);
                    if (subQueryNode.getRowType().getFieldList().size() == 1) {
                      final Project probeProject = firstClassInstance(Project.class, subQueryNode);
                      if (probeProject != null) {
                        final List<RelDataTypeField> fieldList = probeProject.getRowType().getFieldList();
                        if (fieldList.size() == 1) {
                          final RelDataTypeField project = fieldList.get(0);
                          final String name = project.getName();
                          if (ID.equalsIgnoreCase(name)) {
                            QueryBuilder queryBuilder;
                            final Filter filter = firstClassInstance(Filter.class, subQueryNode);
                            if (filter != null) {
                              RelNode bestExp = implElasticsearchRel(filter);
                              ElasticsearchRel.Implementor implementor = implementRel((ElasticsearchRel) bestExp);
                              List<Object> queryBuilders = implementor.list.stream().filter(x -> x instanceof QueryBuilder).collect(Collectors.toList());
                              assert queryBuilders.size() == 1;
                              queryBuilder = (QueryBuilder) queryBuilders.get(0);
                            } else {
                              queryBuilder = QueryBuilders.matchAll();
                            }
                            return new PromisedQueryExpression(predicationConditionMap).promised(ConditionalReduction.CHILDREN_AGGREGATION, ConditionalReduction.ConditionKey.ROOT_ID_SELECTION, null, queryBuilder).orElse(null);
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }

        }
      }
      return null;
    }

    /**
     * Whether field's data type is a JOIN type
     *
     * @param properties Properties belong to a certain field
     */
    private static boolean isJoinType(JsonNode properties) {
      return JOIN_TYPE.equalsIgnoreCase(properties.get(TYPE_KEY).asText());
    }

    private ElasticsearchTable getElasticTable() {
      return relOptTable.unwrap(ElasticsearchTable.class);
    }

    /**
     * Find first object that is a sub-class of given class type.
     * Note that it only handle case with one input which means all nodes are {@link  org.apache.calcite.rel.SingleRel}
     *
     * @param sample target class type
     * @param start  top node
     * @return null if not found
     */
    static <T> T firstClassInstance(Class<T> sample, RelNode start) {
      RelNode probeProject;
      for (probeProject = start; probeProject.getInputs().size() != 0 && !sample.isAssignableFrom(probeProject.getClass()); probeProject = probeProject.getInput(0)) {
        //ignore
      }
      return (T) (sample.isAssignableFrom(probeProject.getClass()) ? probeProject : null);
    }

    /**
     * convert subquery to has child.
     * TODO will fail if the eq'e right hand was inputRef
     */
    private Expression predicateHasChild(RexSubQuery subQuery) {
      final AtomicBoolean projectionTest = new AtomicBoolean(false);
      final AtomicBoolean filterTest = new AtomicBoolean(false);
      final RexNode rexNode = subQuery.operands.get(0);
      if (subQuery.op instanceof SqlInOperator) {
        if (rexNode.isA(SqlKind.CAST)) {
          final RexNode rexNode1 = ((RexCall) rexNode).getOperands().get(0);
          if (((RexCall) rexNode1).getOperator().isName(ITEM_FUNC, false)) {
            final List<RexNode> operands = ((RexCall) rexNode1).getOperands();
            final RexInputRef inputRef = (RexInputRef) operands.get(0);
            final RexLiteral literalRef = (RexLiteral) operands.get(1);
            if (inputRef.getIndex() == 0 || ID.equalsIgnoreCase(literalRef.getValueAs(String.class))) {
              //start matching the subquery
              final ElasticsearchTable elasticsearchTable = getElasticTable();
              if (elasticsearchTable != null) {
                final RelNode query = RelCopyShuttle.copyOf(subQuery.rel);
                if (query.getRowType().getFieldList().size() == 1) {
                  RelNode probeProject;
                  for (probeProject = query; probeProject.getInputs().size() != 0 && !(probeProject instanceof Project); probeProject = probeProject.getInput(0)) {
                    //ignore
                  }
                  if (probeProject instanceof Project) {
                    testProjection(projectionTest, elasticsearchTable.transport.mapping, probeProject);
                    if (projectionTest.get()) {
                      final RexLiteral literal = testHasChildFilter(filterTest, query, elasticsearchTable.transport.mapping);
                      if (filterTest.get()) {
                        final RelNode imp = implElasticsearchRel(query);
                        Filter filter = firstClassInstance(Filter.class, imp);
                        if (filter != null) {
                          ElasticsearchRel.Implementor implementor = implementRel((ElasticsearchRel) imp);
                          QueryBuilder subQueryBuilder = implementor.firstQuery();
                          if (subQueryBuilder != null) {
                            return new PromisedQueryExpression(predicationConditionMap).orElse(new SimpleQueryExpression(null).hasChild((LiteralExpression) literal.accept(this), subQueryBuilder).builder());
                          } else {
                            throw new IllegalArgumentException("Unexpected subquery implementation");
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      return null;
    }

    /**
     * To find whether a rex node contains join type equation, such as customer_child.name = 'parent'
     */
    static class JoinTypeEquationFinderShuttle extends RexShuttle {
      private final ElasticsearchMapping elasticsearchMapping;
      String joinType = null;
      Boolean parent = null;

      private JoinTypeEquationFinderShuttle(ElasticsearchMapping elasticsearchMapping) {
        this.elasticsearchMapping = elasticsearchMapping;
      }

      @Override
      public RexNode visitCall(RexCall call) {
        final SqlOperator operator = call.getOperator();
        if (operator.getKind() == SqlKind.EQUALS) {
          final RexNode typeLiteral = call.getOperands().get(1);
          if (typeLiteral instanceof RexLiteral) {
            final RexNode inputRef = call.getOperands().get(0);
            if (inputRef instanceof RexFieldAccess) {
              if (isTargetFieldAccess((RexFieldAccess) inputRef, NAME_FIELD, elasticsearchMapping)) {
                final String typeName = ((RexLiteral) typeLiteral).getValueAs(String.class);
                finish:
                for (Map.Entry<String, ElasticsearchMapping.Datatype> x : elasticsearchMapping.mapping().entrySet()) {
                  if (isJoinType(x.getValue().properties())) {
                    //Code below will be triggered once, cause one index can have at most one join type
                    final Map.Entry<String, JsonNode> relations = x.getValue().properties().get(RELATIONS_KEY).fields().next();
                    final String key = relations.getKey();
                    if (typeName.equalsIgnoreCase(key)) {
                      joinType = key;
                      parent = true;
                    } else {
                      for (JsonNode jsonNode : relations.getValue()) {
                        final String childType = jsonNode.asText();
                        //This mean we just support 2 layer join
                        if (typeName.equalsIgnoreCase(childType)) {
                          joinType = childType;
                          parent = false;
                          break finish;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        return super.visitCall(call);
      }
    }

    /**
     * Test if field access name equals given target field name
     *
     * @param fieldAccess          Source field access
     * @param targetField          Target field name
     * @param elasticsearchMapping Filed mapping info
     */
    private static boolean isTargetFieldAccess(RexFieldAccess fieldAccess, String targetField, ElasticsearchMapping elasticsearchMapping) {
      RelDataTypeField field = fieldAccess.getField();
      RexNode referenceExpr = fieldAccess.getReferenceExpr();
      if (referenceExpr instanceof RexCall && SqlStdOperatorTable.CAST.equals(((RexCall) referenceExpr).getOperator())) {
        RexNode node = ((RexCall) referenceExpr).getOperands().get(0);
        if (node instanceof RexCall && SqlStdOperatorTable.ITEM.equals(((RexCall) node).getOperator())) {
          RexNode node1 = ((RexCall) node).getOperands().get(1);
          if (node1 instanceof RexLiteral) {
            String valueAs = ((RexLiteral) node1).getValueAs(String.class);
            ElasticsearchMapping.Datatype datatype = elasticsearchMapping.mapping().get(valueAs);
            if (isJoinType(datatype.properties())) {
              return field.getName().equalsIgnoreCase(targetField);
            }
          }
        }
      }
      return false;
    }

    /**
     * copy relNode
     */
    static class RelCopyShuttle extends RelShuttleImpl {
      static RelNode copyOf(RelNode relNode) {
        return new RelCopyShuttle().visit(relNode);
      }

      /**
       * copy a filter entirely
       *
       * @param filter original filter
       */
      @Override
      public RelNode visit(LogicalFilter filter) {
        final RelNode visit = super.visit(filter);
        return filter.copy(filter.getTraitSet(), visit.getInput(0), filter.getCluster().getRexBuilder().copy(filter.getCondition()));
      }
    }

    private RelNode implElasticsearchRel(RelNode relNode) {
      RelOptCluster cluster = relNode.getCluster();
      RelOptPlanner planner = cluster.getPlanner();
      RelNode origin = planner.changeTraits(relNode, cluster.traitSetOf(ElasticsearchRel.CONVENTION));
      planner.setRoot(origin);
      return planner.findBestExp();
    }

    /**
     * @param filterTest   result holder
     * @param subQueryNode query which will be used to test
     * @param mapping      use to see if the join type are ok, dammit!
     */
    private RexLiteral testHasChildFilter(AtomicBoolean filterTest, final RelNode subQueryNode, ElasticsearchMapping mapping) {
      final AtomicReference<RexLiteral> nameHolder = new AtomicReference<>();
      for (RelNode current = subQueryNode, previous = null; !(current instanceof TableScan); previous = current, current = current.getInput(0)) {
        if (current instanceof Filter) {
          final RelNode refinedFilter = current.accept(getHasChildEquationShuttle(filterTest, mapping, nameHolder, current));
          if (current != refinedFilter) {
            //ok we find the key, now we can return
            assert previous != null;
            previous.replaceInput(0, refinedFilter);
            break;
          }
        }
      }
      return nameHolder.get();
    }

    private RexShuttle getHasChildEquationShuttle(AtomicBoolean filterTest, ElasticsearchMapping
        mapping, AtomicReference<RexLiteral> nameHolder, RelNode finalProbeFilter) {
      return new RexShuttle() {
        @Override
        public RexNode visitCall(RexCall call) {
          if (filterTest.get()) {
            return call;
          }
          if (call.op.kind == SqlKind.EQUALS) {
            final RexNode ref = call.getOperands().get(0);
            final RexNode rexNode = call.getOperands().get(1);
            if (ref instanceof RexInputRef) {
              final int index = ((RexInputRef) ref).getIndex();
              if (testJoinFieldAccess(index, NAME_FIELD, finalProbeFilter, mapping)) {
                filterTest.set(true);
                nameHolder.set(((RexLiteral) rexNode));
                return finalProbeFilter.getCluster().getRexBuilder().makeLiteral(true);//mute this condition in es grammar builder
              }
            }
          }
          return super.visitCall(call);
        }
      };
    }

    private void testProjection(AtomicBoolean projectionTest, ElasticsearchMapping mapping, RelNode probeProject) {
      probeProject.accept(new RexShuttle() {
        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
          projectionTest.set(testJoinFieldAccess(inputRef.getIndex(), PARENT_FIELD, probeProject, mapping));
          return inputRef;
        }
      });
    }

    /**
     * test if a field is derived from a join and hold the right offset of the join
     *
     * @param index       field to test
     * @param targetField should field name
     * @param rootNode    root relNode
     * @param mapping     ES mapping
     * @return
     */
    private static boolean testJoinFieldAccess(int index, String targetField, RelNode rootNode, ElasticsearchMapping mapping) {
      for (RelNode input = rootNode.getInput(0); input.getInputs().size() != 0; input = input.getInput(0)) {
        if (input instanceof Project) {
          final RexNode rexNode = ((Project) input).getProjects().get(index);
          if (rexNode instanceof RexFieldAccess) {
            final RelDataTypeField field = ((RexFieldAccess) rexNode).getField();
            if (targetField.equalsIgnoreCase(field.getName())) {
              final RexNode referenceExpr = ((RexFieldAccess) rexNode).getReferenceExpr();
              if (referenceExpr instanceof RexCall) {
                final RexCall castCall = (RexCall) referenceExpr;
                if (castCall.op.isName(CAST_FUNC, false)) {
                  final RexNode call = castCall.getOperands().get(0);
                  if (call instanceof RexCall) {
                    RexCall itemCall = (RexCall) call;
                    if (itemCall.op.isName(ITEM_FUNC, false)) {
                      final ImmutableList<RexNode> operands1 = itemCall.operands;
                      if (operands1.size() == 2) {
                        final RexLiteral rexLiteral = (RexLiteral) operands1.get(1);
                        if (JOIN_TYPE.equalsIgnoreCase(mapping.mapping().get(rexLiteral.getValueAs(String.class)).name())) {
                          return true;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      return false;
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
      return new NamedFieldExpression(inputRef);
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
      return new LiteralExpression(literal);
    }

    private boolean supportedRexCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
        case BINARY:
          switch (call.getKind()) {
            case AND:
            case OR:
            case LIKE:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
              return true;
            case OTHER_FUNCTION:
              final String operator = call.getOperator().getName();
              if (ElasticsearchConstants.ES_MATCH_AND.equalsIgnoreCase(operator) || ElasticsearchConstants.ES_MATCH_OR.equalsIgnoreCase(operator)) {
                return true;
              }
            default:
              return false;
          }
        case SPECIAL:
          switch (call.getKind()) {
            case CAST:
            case LIKE:
            case OTHER_FUNCTION:
            case CASE:
              return true;
            case SIMILAR:
            default:
              return false;
          }
        case FUNCTION:
          return true;
        case POSTFIX:
          switch (call.getKind()) {
            case IS_NOT_NULL:
            case IS_NULL:
              return true;
          }
        case PREFIX: // NOT()
          switch (call.getKind()) {
            case NOT:
              return true;
          }
          // fall through
        case FUNCTION_ID:
        case FUNCTION_STAR:
        default:
          return false;
      }
    }

    @Override
    public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode referenceExpr = fieldAccess.getReferenceExpr();
      Expression accept = referenceExpr.accept(this);
      assert accept instanceof TerminalExpression;
      RelDataTypeField field = fieldAccess.getField();
      RexBuilder rexBuilder = topNode.getCluster().getRexBuilder();
      NamedFieldExpression namedFieldExpression = new NamedFieldExpression(rexBuilder.makeLiteral("" + "." + field.getName()));
      throw new RuntimeException("TODO");
    }

    @Override
    public Expression visitCall(RexCall call) {

      SqlSyntax syntax = call.getOperator().getSyntax();
      if (!supportedRexCall(call)) {
        String message = String.format(Locale.ROOT, "Unsupported call: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      switch (syntax) {
        case BINARY:
          return binary(call);
        case POSTFIX:
          return postfix(call);
        case PREFIX:
          return prefix(call);
        case SPECIAL:
          switch (call.getKind()) {
            case CAST:
              return toCastExpression(call);
            case LIKE:
              return binary(call);
            case CASE:
              return caze(call);
            default:
              // manually process ITEM($0, 'foo') which in our case will be named attribute
              if (call.getOperator().getName().equalsIgnoreCase("ITEM")) {
                return toNamedField((RexLiteral) call.getOperands().get(1));
              }
              String message = String.format(Locale.ROOT, "Unsupported call: [%s]", call);
              throw new PredicateAnalyzerException(message);
          }
        case FUNCTION:
          if (call.getOperator().getName().equalsIgnoreCase("CONTAINS")) {
            List<Expression> operands = new ArrayList<>();
            for (RexNode node : call.getOperands()) {
              final Expression nodeExpr = node.accept(this);
              operands.add(nodeExpr);
            }
            String query = convertQueryString(operands.subList(0, operands.size() - 1),
                operands.get(operands.size() - 1));
            return QueryExpression.create(new NamedFieldExpression()).queryString(query);
          }
          // fall through
        default:
          String message = format(Locale.ROOT, "Unsupported syntax [%s] for call: [%s]",
              syntax, call);
          throw new PredicateAnalyzerException(message);
      }
    }

    /**
     * Implement CASE [WHEN THEN...] ELSE END in {@link org.apache.calcite.rel.logical.LogicalProject},
     *
     * @param call RexCall
     */
    private QueryExpression caze(RexCall call) {
      assert SqlStdOperatorTable.CASE.equals(call.getOperator());
      final RexCall fieldLiteral = rangeSemanticCheck(call);
      final List<RexNode> operands = call.getOperands();
      //Shuttle will discard what doesn't need
      class FromAndToShuttle extends RexShuttle {

        private RexLiteral from;
        private RexLiteral to;
        private final Set<SqlKind> validOperator = ImmutableSet.of(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN);

        @Override
        public RexNode visitCall(RexCall call) {
          final SqlOperator operator = call.getOperator();
          if (validOperator.contains(operator.getKind())) {
            for (RexNode operand : call.getOperands()) {
              if (operand instanceof RexLiteral) {
                switch (operator.getKind()) {
                  case LESS_THAN:
                    to = (RexLiteral) operand;
                    break;
                  case GREATER_THAN_OR_EQUAL:
                    from = (RexLiteral) operand;
                    break;
                }
              }
            }
          }
          return super.visitCall(call);
        }
      }
      final List<Pair<Pair<LiteralExpression, LiteralExpression>, LiteralExpression>> rangeList = new ArrayList<>();
      for (int i = 0; i < operands.size() / 2; i++) {
        final FromAndToShuttle fromAndToShuttle = new FromAndToShuttle();
        final RexNode ranges = operands.get(2 * i);
        ranges.accept(fromAndToShuttle);
        final RexNode key = operands.get(2 * i + 1);
        assert key instanceof RexLiteral;
        rangeList.add(
            Pair.of(
                Pair.of(
                    fromAndToShuttle.from != null ? (LiteralExpression) visitLiteral(fromAndToShuttle.from) : null,
                    fromAndToShuttle.to != null ? (LiteralExpression) visitLiteral(fromAndToShuttle.to) : null
                ),
                (LiteralExpression) visitLiteral((RexLiteral) key)));
      }
      return QueryExpression.create((NamedFieldExpression) visitCall(fieldLiteral)).range(rangeList);
    }

    /**
     * Check if CASE call match the restrictions of es ranges
     */
    private RexCall rangeSemanticCheck(RexCall call) {
      final Map<Integer, RexCall> rexInputRefs = new HashMap<>();
      final RexVisitorImpl<Void> sameInputVisitor = new RexVisitorImpl<Void>(true) {
        @Override
        public Void visitCall(RexCall call) {
          if (call.getOperator().equals(SqlStdOperatorTable.ITEM)) {
            final RexInputRef inputRef = ((RexInputRef) call.getOperands().get(0));
            rexInputRefs.put(inputRef.getIndex(), call);
          }
          return super.visitCall(call);
        }
      };
      call.accept(sameInputVisitor);
      if (rexInputRefs.size() > 1) {
        throw new IllegalArgumentException("Unexpected arguments of CASE, should use only one input ref but got " + rexInputRefs.size());
      }
      return (RexCall) rexInputRefs.values().toArray()[0];
    }

    private static String convertQueryString(List<Expression> fields, Expression query) {
      int index = 0;
      Preconditions.checkArgument(query instanceof LiteralExpression,
          "Query string must be a string literal");
      String queryString = ((LiteralExpression) query).stringValue();
      Map<String, String> fieldMap = new LinkedHashMap<>();
      for (Expression expr : fields) {
        if (expr instanceof NamedFieldExpression) {
          NamedFieldExpression field = (NamedFieldExpression) expr;
          String fieldIndexString = String.format(Locale.ROOT, "$%d", index++);
          fieldMap.put(fieldIndexString, field.getReference());
        }
      }
      try {
        return queryString;
      } catch (Exception e) {
        throw new PredicateAnalyzerException(e);
      }
    }

    private QueryExpression prefix(RexCall call) {
      Preconditions.checkArgument(call.getKind() == SqlKind.NOT,
          "Expected %s got %s", SqlKind.NOT, call.getKind());

      if (call.getOperands().size() != 1) {
        String message = String.format(Locale.ROOT, "Unsupported NOT operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      QueryExpression expr = (QueryExpression) call.getOperands().get(0).accept(this);
      return expr.not();
    }

    private QueryExpression postfix(RexCall call) {
      Preconditions.checkArgument(call.getKind() == SqlKind.IS_NULL
          || call.getKind() == SqlKind.IS_NOT_NULL);
      if (call.getOperands().size() != 1) {
        String message = String.format(Locale.ROOT, "Unsupported operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }
      Expression a = call.getOperands().get(0).accept(this);
      // Elasticsearch does not want is null/is not null (exists query)
      // for _id and _index, although it supports for all other metadata column
      isColumn(a, call, ElasticsearchConstants.ID, true);
      isColumn(a, call, ElasticsearchConstants.INDEX, true);
      QueryExpression operand = QueryExpression.create((TerminalExpression) a);
      return call.getKind() == SqlKind.IS_NOT_NULL ? operand.exists() : operand.notExists();
    }

    /**
     * Process a call which is a binary operation, transforming into an equivalent
     * query expression. Note that the incoming call may be either a simple binary
     * expression, such as {@code foo > 5}, or it may be several simple expressions connected
     * by {@code AND} or {@code OR} operators, such as {@code foo > 5 AND bar = 'abc' AND 'rot' < 1}
     *
     * @param call existing call
     * @return evaluated expression
     */
    private QueryExpression binary(RexCall call) {

      // if AND/OR, do special handling
      if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
        return andOr(call);
      }

      checkForIncompatibleDateTimeOperands(call);

      Preconditions.checkState(call.getOperands().size() == 2);
      final Expression a = call.getOperands().get(0).accept(this);
      final Expression b = call.getOperands().get(1).accept(this);

      final SwapResult pair = swap(a, b);
      final boolean swapped = pair.isSwapped();

      // For _id and _index columns, only equals/not_equals work!
      if (isColumn(pair.getKey(), call, ElasticsearchConstants.ID, false)
          || isColumn(pair.getKey(), call, ElasticsearchConstants.INDEX, false)
          || isColumn(pair.getKey(), call, ElasticsearchConstants.UID, false)) {
        switch (call.getKind()) {
          case EQUALS:
          case NOT_EQUALS:
            break;
          default:
            throw new PredicateAnalyzerException(
                "Cannot handle " + call.getKind() + " expression for _id field, " + call);
        }
      }

      switch (call.getKind()) {
        case LIKE:
          return QueryExpression.create(pair.getKey()).like(pair.getValue());
//          throw new UnsupportedOperationException("LIKE not yet supported");
        case EQUALS: {
          final QueryExpression equals = QueryExpression.create(pair.getKey()).equals(pair.getValue());
          JoinTypeEquationFinderShuttle joinTypeEquationFinderShuttle = new JoinTypeEquationFinderShuttle(getElasticTable().transport.mapping);
          call.accept(joinTypeEquationFinderShuttle);
          if (Boolean.FALSE.equals(joinTypeEquationFinderShuttle.parent) && joinTypeEquationFinderShuttle.joinType != null) {
            return new PromisedQueryExpression(predicationConditionMap).
                promised(ConditionalReduction.CHILDREN_AGGREGATION, ConditionalReduction.ConditionKey.CHILD_TYPE_JOIN_EQUATION, joinTypeEquationFinderShuttle.joinType, QueryBuilders.voidQuery()).
                orElse(equals.builder());
          }
          return equals;
        }
        case NOT_EQUALS:
          return QueryExpression.create(pair.getKey()).notEquals(pair.getValue());
        case GREATER_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gt(pair.getValue());
        case GREATER_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).lte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).gte(pair.getValue());
        case LESS_THAN:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gt(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lt(pair.getValue());
        case LESS_THAN_OR_EQUAL:
          if (swapped) {
            return QueryExpression.create(pair.getKey()).gte(pair.getValue());
          }
          return QueryExpression.create(pair.getKey()).lte(pair.getValue());
        case OTHER_FUNCTION:
          final SqlOperator operator = call.getOperator();
          if (operator instanceof ElasticsearchConstants.ElasticsearchMatchOperator) {
            return QueryExpression.create(pair.getKey()).match(pair.getValue(), ((ElasticsearchConstants.ElasticsearchMatchOperator) operator).getOperator());
          }
        default:
          break;
      }
      String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
      throw new PredicateAnalyzerException(message);
    }

    private QueryExpression andOr(RexCall call) {
      QueryExpression[] expressions = new QueryExpression[call.getOperands().size()];
      PredicateAnalyzerException firstError = null;
      boolean partial = false;
      for (int i = 0; i < call.getOperands().size(); i++) {
        try {
          Expression expr = call.getOperands().get(i).accept(this);
          if (expr instanceof NamedFieldExpression) {
            // nop currently
          } else {
            expressions[i] = (QueryExpression) call.getOperands().get(i).accept(this);
          }
          partial |= expressions[i].isPartial();
        } catch (PredicateAnalyzerException e) {
          if (firstError == null) {
            firstError = e;
          }
          partial = true;
        }
      }

      switch (call.getKind()) {
        case OR:
          if (partial) {
            if (firstError != null) {
              throw firstError;
            } else {
              final String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
              throw new PredicateAnalyzerException(message);
            }
          }
          return CompoundQueryExpression.or(expressions);
        case AND:
          return CompoundQueryExpression.and(partial, expressions);
        default:
          String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
          throw new PredicateAnalyzerException(message);
      }
    }

    /**
     * Holder class for a pair of expressions. Used to convert {@code 1 = foo} into {@code foo = 1}
     */
    private static class SwapResult {
      final boolean swapped;
      final TerminalExpression terminal;
      final LiteralExpression literal;

      SwapResult(boolean swapped, TerminalExpression terminal, LiteralExpression literal) {
        super();
        this.swapped = swapped;
        this.terminal = terminal;
        this.literal = literal;
      }

      TerminalExpression getKey() {
        return terminal;
      }

      LiteralExpression getValue() {
        return literal;
      }

      boolean isSwapped() {
        return swapped;
      }
    }

    /**
     * Swap order of operands such that the literal expression is always on the right.
     *
     * <p>NOTE: Some combinations of operands are implicitly not supported and will
     * cause an exception to be thrown. For example, we currently do not support
     * comparing a literal to another literal as convention {@code 5 = 5}. Nor do we support
     * comparing named fields to other named fields as convention {@code $0 = $1}.
     *
     * @param left  left expression
     * @param right right expression
     */
    private static SwapResult swap(Expression left, Expression right) {

      TerminalExpression terminal;
      LiteralExpression literal = expressAsLiteral(left);
      boolean swapped = false;
      if (literal != null) {
        swapped = true;
        terminal = (TerminalExpression) right;
      } else {
        literal = expressAsLiteral(right);
        terminal = (TerminalExpression) left;
      }

      if (literal == null || terminal == null) {
        String message = String.format(Locale.ROOT,
            "Unexpected combination of expressions [left: %s] [right: %s]", left, right);
        throw new PredicateAnalyzerException(message);
      }

      if (CastExpression.isCastExpression(terminal)) {
        terminal = CastExpression.unpack(terminal);
      }

      return new SwapResult(swapped, terminal, literal);
    }

    private CastExpression toCastExpression(RexCall call) {
      TerminalExpression argument = (TerminalExpression) call.getOperands().get(0).accept(this);
      return new CastExpression(call.getType(), argument);
    }

    private static NamedFieldExpression toNamedField(RexLiteral literal) {
      return new NamedFieldExpression(literal);
    }

    /**
     * Try to convert a generic expression into a literal expression.
     */
    private static LiteralExpression expressAsLiteral(Expression exp) {

      if (exp instanceof LiteralExpression) {
        return (LiteralExpression) exp;
      }

      return null;
    }

    private static boolean isColumn(Expression exp, RexNode node,
                                    String columnName, boolean throwException) {
      if (!(exp instanceof NamedFieldExpression)) {
        return false;
      }

      final NamedFieldExpression termExp = (NamedFieldExpression) exp;
      if (columnName.equals(termExp.getRootName())) {
        if (throwException) {
          throw new PredicateAnalyzerException("Cannot handle _id field in " + node);
        }
        return true;
      }
      return false;
    }
  }

  private static ElasticsearchRel.Implementor implementRel(ElasticsearchRel bestExp) {
    ElasticsearchRel.Implementor implementor = new ElasticsearchRel.Implementor();
    bestExp.implement(implementor);
    return implementor;
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  interface Expression {
  }

  /**
   * Main expression operators (like {@code equals}, {@code gt}, {@code exists} etc.)
   */
  abstract static class QueryExpression implements Expression {

    public abstract QueryBuilder builder();

    public boolean isPartial() {
      return false;
    }

    /**
     * @param name  join type
     * @param query query
     * @return
     */
    public abstract QueryExpression hasChild(LiteralExpression name, QueryBuilder query);

    public abstract QueryExpression range(List<Pair<Pair<LiteralExpression, LiteralExpression>, LiteralExpression>> rangeList);

    /**
     * Negate {@code this} QueryExpression (not the next one).
     */
    public abstract QueryExpression not();

    public abstract QueryExpression exists();

    public abstract QueryExpression notExists();

    public abstract QueryExpression like(LiteralExpression literal);

    public abstract QueryExpression notLike(LiteralExpression literal);

    public abstract QueryExpression equals(LiteralExpression literal);

    public abstract QueryExpression notEquals(LiteralExpression literal);

    public abstract QueryExpression gt(LiteralExpression literal);

    public abstract QueryExpression gte(LiteralExpression literal);

    public abstract QueryExpression lt(LiteralExpression literal);

    public abstract QueryExpression lte(LiteralExpression literal);

    public abstract QueryExpression match(LiteralExpression literal, String operator);

    public abstract QueryExpression queryString(String query);

    public abstract QueryExpression isTrue();

    public static QueryExpression create(TerminalExpression expression) {

      if (expression instanceof NamedFieldExpression) {
        return new SimpleQueryExpression((NamedFieldExpression) expression);
      } else {
        String message = String.format(Locale.ROOT, "Unsupported expression: [%s]", expression);
        throw new PredicateAnalyzerException(message);
      }
    }

  }

  /**
   * {@link SimpleQueryExpression} that hold conditioned {@link QueryBuilder}, when condition matches, related {@link QueryExpression} will be returned,
   * otherwise default expression returned
   */
  static class PromisedQueryExpression extends SimpleQueryExpression {

    private QueryBuilder orElse;
    private final EnumMap<ConditionalReduction, QueryBuilder> ifPromised;
    private final EnumMap<ConditionalReduction, ConditionalReduction.ConditionCollector> predicationConditionMap;


    PromisedQueryExpression(EnumMap<ConditionalReduction, ConditionalReduction.ConditionCollector> predicationConditionMap) {
      super(null);
      this.predicationConditionMap = predicationConditionMap;
      this.ifPromised = new EnumMap<>(ConditionalReduction.class);
    }

    PromisedQueryExpression promised(ConditionalReduction predication, Object condition, Object conditionVal, QueryBuilder queryBuilder) {
      ifPromised.put(predication, queryBuilder);
      ConditionalReduction.ConditionCollector analyzePredicationCondition = predicationConditionMap.get(predication);
      if (analyzePredicationCondition != null) {
        analyzePredicationCondition.add(condition, conditionVal);
      }
      return this;
    }

    PromisedQueryExpression orElse(QueryBuilder orElse) {
      this.orElse = orElse;
      return this;
    }

    @Override
    public QueryBuilder builder() {
      return new QueryBuilders.DelayedQueryBuilder(() -> {
        for (Map.Entry<ConditionalReduction, QueryBuilder> analyzePredicationConsumerEntry : ifPromised.entrySet()) {
          ConditionalReduction.ConditionCollector analyzePredicationCondition = predicationConditionMap.get(analyzePredicationConsumerEntry.getKey());
          if (analyzePredicationCondition.allMatched()) {
            return analyzePredicationConsumerEntry.getValue();
          }
        }
        return orElse;
      });
    }
  }

  /**
   * Builds conjunctions / disjunctions based on existing expressions
   */
  static class CompoundQueryExpression extends QueryExpression {

    private final boolean partial;
    private final BoolQueryBuilder builder;

    public static CompoundQueryExpression or(QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      for (QueryExpression expression : expressions) {
        bqe.builder.should(expression.builder());
      }
      return bqe;
    }

    /**
     * if partial expression, we will need to complete it with a full filter
     *
     * @param partial     whether we partially converted and for push down purposes.
     * @param expressions list of expressions to join with {@code and} boolean
     * @return new instance of expression
     */
    public static CompoundQueryExpression and(boolean partial, QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(partial);
      for (QueryExpression expression : expressions) {
        if (expression != null) { // partial expressions have nulls for missing nodes
          bqe.builder.must(expression.builder());
        }
      }
      return bqe;
    }

    private CompoundQueryExpression(boolean partial) {
      this(partial, QueryBuilders.boolQuery());
    }

    private CompoundQueryExpression(boolean partial, BoolQueryBuilder builder) {
      this.partial = partial;
      this.builder = Objects.requireNonNull(builder, "builder");
    }

    @Override
    public boolean isPartial() {
      return partial;
    }

    @Override
    public QueryExpression hasChild(LiteralExpression rexLiteral, QueryBuilder accept) {
      throw new PredicateAnalyzerException("Query semantic ['hasChild'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression range(List<Pair<Pair<LiteralExpression, LiteralExpression>, LiteralExpression>> rangeList) {
      throw new PredicateAnalyzerException("Query semantic ['hasChild'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryBuilder builder() {
      return builder;
    }

    @Override
    public QueryExpression not() {
      return new CompoundQueryExpression(partial, QueryBuilders.boolQuery().mustNot(builder()));
    }

    @Override
    public QueryExpression exists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['exists'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notExists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['notExists'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['like'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notLike(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['notLike'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['='] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['not'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>='] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<='] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression match(LiteralExpression literal, String operator) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['match'] "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression queryString(String query) {
      throw new PredicateAnalyzerException("QueryString "
          + "cannot be applied to a compound expression");
    }

    @Override
    public QueryExpression isTrue() {
      throw new PredicateAnalyzerException("isTrue cannot be applied to a compound expression");
    }
  }

  /**
   * Usually basic expression of type {@code a = 'val'} or {@code b > 42}.
   */
  static class SimpleQueryExpression extends QueryExpression {

    protected final NamedFieldExpression rel;
    protected QueryBuilder builder;

    private String getFieldReference() {
      return rel.getReference();
    }

    private SimpleQueryExpression(NamedFieldExpression rel) {
      this.rel = rel;
    }

    @Override
    public QueryBuilder builder() {
      if (builder == null) {
        throw new IllegalStateException("Builder was not initialized");
      }
      return builder;
    }

    @Override
    public QueryExpression hasChild(LiteralExpression literalExpression, QueryBuilder childQuery) {
      assert builder == null;
      builder = QueryBuilders.hasChild(literalExpression.stringValue(), childQuery);
      return this;
    }

    @Override
    public QueryExpression range(List<Pair<Pair<LiteralExpression, LiteralExpression>, LiteralExpression>> rangeList) {
      builder = QueryBuilders.multiRanges(getFieldReference(), rangeList.stream().map(rangeBlock -> {
        final Pair<LiteralExpression, LiteralExpression> ranges = rangeBlock.left;
        final LiteralExpression left = ranges.left;
        final LiteralExpression right = ranges.right;
        return Pair.of(Pair.of(left != null ? left.value() : null, right != null ? right.value() : null), rangeBlock.right.value());
      }).collect(Collectors.toList()));
      return this;
    }

    @Override
    public QueryExpression not() {
      builder = QueryBuilders.boolQuery().mustNot(builder());
      return this;
    }

    @Override
    public QueryExpression exists() {
      builder = QueryBuilders.existsQuery(getFieldReference());
      return this;
    }

    @Override
    public QueryExpression notExists() {
      // Even though Lucene doesn't allow a stand alone mustNot boolean query,
      // Elasticsearch handles this problem transparently on its end
      builder = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(getFieldReference()));
      return this;
    }

    @Override
    public QueryExpression like(LiteralExpression literal) {
      builder = QueryBuilders.wildcardQuery(getFieldReference(), literal.stringValue());
      return this;
    }

    @Override
    public QueryExpression notLike(LiteralExpression literal) {
      builder = QueryBuilders.boolQuery()
          // NOT LIKE should return false when field is NULL
          .must(QueryBuilders.existsQuery(getFieldReference()))
          .mustNot(QueryBuilders.regexpQuery(getFieldReference(), literal.stringValue()));
      return this;
    }

    @Override
    public QueryExpression equals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = QueryBuilders.boolQuery()
            .must(addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).gte(value)))
            .must(addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).lte(value)));
      } else {
        builder = QueryBuilders.termQuery(getFieldReference(), value);
      }
      return this;
    }

    @Override
    public QueryExpression notEquals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = QueryBuilders.boolQuery()
            .should(addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).gt(value)))
            .should(addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).lt(value)));
      } else {
        builder = QueryBuilders.boolQuery()
            // NOT LIKE should return false when field is NULL
            .must(QueryBuilders.existsQuery(getFieldReference()))
            .mustNot(QueryBuilders.termQuery(getFieldReference(), value));
      }
      return this;
    }

    @Override
    public QueryExpression gt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal,
          QueryBuilders.rangeQuery(getFieldReference()).gt(value));
      return this;
    }

    @Override
    public QueryExpression gte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).gte(value));
      return this;
    }

    @Override
    public QueryExpression lt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).lt(value));
      return this;
    }

    @Override
    public QueryExpression lte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, QueryBuilders.rangeQuery(getFieldReference()).lte(value));
      return this;
    }

    @Override
    public QueryExpression match(LiteralExpression literal, String operator) {
      builder = QueryBuilders.matchQuery(getFieldReference(), literal.stringValue(), operator);
      return this;
    }

    @Override
    public QueryExpression queryString(String query) {
      throw new UnsupportedOperationException("QueryExpression not yet supported: " + query);
    }

    @Override
    public QueryExpression isTrue() {
      builder = QueryBuilders.termQuery(getFieldReference(), true);
      return this;
    }
  }


  /**
   * By default, range queries on date/time need use the format of the source to parse the literal.
   * So we need to specify that the literal has "date_time" format
   *
   * @param literal           literal value
   * @param rangeQueryBuilder query builder to optionally add {@code format} expression
   * @return existing builder with possible {@code format} attribute
   */
  private static RangeQueryBuilder addFormatIfNecessary(LiteralExpression literal,
                                                        RangeQueryBuilder rangeQueryBuilder) {
    if (literal.value() instanceof GregorianCalendar) {
      rangeQueryBuilder.format("date_time");
    }
    return rangeQueryBuilder;
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  interface TerminalExpression extends Expression {
  }

  /**
   * SQL cast: {@code cast(col as INTEGER)}
   */
  static final class CastExpression implements TerminalExpression {
    private final RelDataType type;
    private final TerminalExpression argument;

    private CastExpression(RelDataType type, TerminalExpression argument) {
      this.type = type;
      this.argument = argument;
    }

    public boolean isCastFromLiteral() {
      return argument instanceof LiteralExpression;
    }

    static TerminalExpression unpack(TerminalExpression exp) {
      if (!(exp instanceof CastExpression)) {
        return exp;
      }
      return ((CastExpression) exp).argument;
    }

    static boolean isCastExpression(Expression exp) {
      return exp instanceof CastExpression;
    }

  }

  /**
   * Used for bind variables
   */
  static final class NamedFieldExpression implements TerminalExpression {

    private final String name;

    private NamedFieldExpression() {
      this.name = null;
    }

    private NamedFieldExpression(RexInputRef schemaField) {
      this.name = schemaField == null ? null : schemaField.getName();
    }

    private NamedFieldExpression(RexLiteral literal) {
      this.name = literal == null ? null : RexLiteral.stringValue(literal);
    }

    String getRootName() {
      return name;
    }

    boolean isMetaField() {
      return ElasticsearchConstants.META_COLUMNS.contains(getRootName());
    }

    String getReference() {
      return getRootName();
    }
  }

  /**
   * Literal like {@code 'foo' or 42 or true} etc.
   */
  static final class LiteralExpression implements TerminalExpression {

    final RexLiteral literal;

    LiteralExpression(RexLiteral literal) {
      this.literal = literal;
    }

    Object value() {

      if (isIntegral()) {
        return longValue();
      } else if (isFloatingPoint()) {
        return doubleValue();
      } else if (isBoolean()) {
        return booleanValue();
      } else if (isString()) {
        return RexLiteral.stringValue(literal);
      } else {
        return rawValue();
      }
    }

    boolean isIntegral() {
      return SqlTypeName.INT_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isFloatingPoint() {
      return SqlTypeName.APPROX_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isBoolean() {
      return SqlTypeName.BOOLEAN_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isString() {
      return SqlTypeName.CHAR_TYPES.contains(literal.getType().getSqlTypeName());
    }

    long longValue() {
      return ((Number) literal.getValue()).longValue();
    }

    double doubleValue() {
      return ((Number) literal.getValue()).doubleValue();
    }

    boolean booleanValue() {
      return RexLiteral.booleanValue(literal);
    }

    String stringValue() {
      return RexLiteral.stringValue(literal);
    }

    Object rawValue() {
      return literal.getValue();
    }

  }

  /**
   * If one operand in a binary operator is a DateTime type, but the other isn't,
   * we should not push down the predicate
   *
   * @param call current node being evaluated
   */
  private static void checkForIncompatibleDateTimeOperands(RexCall call) {
    RelDataType op1 = call.getOperands().get(0).getType();
    RelDataType op2 = call.getOperands().get(1).getType();
    if ((SqlTypeFamily.DATETIME.contains(op1) && !SqlTypeFamily.DATETIME.contains(op2))
        || (SqlTypeFamily.DATETIME.contains(op2) && !SqlTypeFamily.DATETIME.contains(op1))
        || (SqlTypeFamily.DATE.contains(op1) && !SqlTypeFamily.DATE.contains(op2))
        || (SqlTypeFamily.DATE.contains(op2) && !SqlTypeFamily.DATE.contains(op1))
        || (SqlTypeFamily.TIMESTAMP.contains(op1) && !SqlTypeFamily.TIMESTAMP.contains(op2))
        || (SqlTypeFamily.TIMESTAMP.contains(op2) && !SqlTypeFamily.TIMESTAMP.contains(op1))
        || (SqlTypeFamily.TIME.contains(op1) && !SqlTypeFamily.TIME.contains(op2))
        || (SqlTypeFamily.TIME.contains(op2) && !SqlTypeFamily.TIME.contains(op1))) {
      throw new PredicateAnalyzerException("Cannot handle " + call.getKind()
          + " expression for _id field, " + call);
    }
  }
}

// End PredicateAnalyzer.java
