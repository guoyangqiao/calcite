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
package org.apache.calcite.schema.impl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Table whose contents are defined using an SQL statement.
 *
 * <p>It is not evaluated; it is expanded during query planning.</p>
 */
public class ViewTable
    extends AbstractQueryableTable
    implements TranslatableTable {
  private final String viewSql;
  private final List<String> schemaPath;
  private final RelProtoDataType protoRowType;
  private final List<String> viewPath;

  public ViewTable(Type elementType, RelProtoDataType rowType, String viewSql,
                   List<String> schemaPath, List<String> viewPath) {
    super(elementType);
    this.viewSql = viewSql;
    this.schemaPath = ImmutableList.copyOf(schemaPath);
    this.protoRowType = rowType;
    this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
  }

  @Deprecated // to be removed before 2.0
  public static ViewTableMacro viewMacro(SchemaPlus schema,
                                         final String viewSql, final List<String> schemaPath) {
    return viewMacro(schema, viewSql, schemaPath, null, Boolean.TRUE);
  }

  @Deprecated // to be removed before 2.0
  public static ViewTableMacro viewMacro(SchemaPlus schema, String viewSql,
                                         List<String> schemaPath, Boolean modifiable) {
    return viewMacro(schema, viewSql, schemaPath, null, modifiable);
  }

  /**
   * Table macro that returns a view.
   * For the purpose of executing query which has dynamic field
   *
   * @param schema     Schema the view will belong to
   * @param viewSql    SQL query
   * @param schemaPath Path of schema
   * @param modifiable Whether view is modifiable, or null to deduce it
   */
  public static ViewTableMacro viewMacro(SchemaPlus schema, String viewSql,
                                         List<String> schemaPath, List<String> viewPath, Boolean modifiable) {
    return new ViewTableMacro.DynamicRowTypeViewTableMarco(CalciteSchema.from(schema), viewSql, schemaPath, viewPath, modifiable);
//    return new ViewTableMacro(CalciteSchema.from(schema), viewSql, schemaPath,
//        viewPath, modifiable);
  }

  /**
   * Returns the view's SQL definition.
   */
  public String getViewSql() {
    return viewSql;
  }

  /**
   * Returns the the schema path of the view.
   */
  public List<String> getSchemaPath() {
    return schemaPath;
  }

  /**
   * Returns the the path of the view.
   */
  public List<String> getViewPath() {
    return viewPath;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.VIEW;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return queryProvider.createQuery(
        getExpression(schema, tableName, Queryable.class), elementType);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return expandView(context, relOptTable.getRowType(), viewSql).rel;
  }

  private RelRoot expandView(RelOptTable.ToRelContext context,
                             RelDataType rowType, String queryString) {
    try {
      final RelRoot root =
          context.expandView(rowType, queryString, schemaPath, viewPath);
      RelNode project = root.rel;
      //While using DynamicRecordType, row type may changed already, fix it by remake target rel
      assert project instanceof Project;
      List<RexNode> projects = sameOrderRowFieldProject((Project) project, rowType, context.getCluster().getRexBuilder());
      project = ((Project) project).copy(project.getTraitSet(), project.getInput(0), projects, rowType);
      final RelNode rel = RelOptUtil.createCastRel(project, rowType, true);
      // Expand any views
      final RelNode rel2 = rel.accept(
          new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
              final RelOptTable table = scan.getTable();
              final TranslatableTable translatableTable =
                  table.unwrap(TranslatableTable.class);
              if (translatableTable != null) {
                return translatableTable.toRel(context, table);
              }
              return super.visit(scan);
            }
          });
      return root.withRel(rel2);
    } catch (Exception e) {
      throw new RuntimeException("Error while parsing view definition: "
          + queryString, e);
    }
  }

  /**
   * Make sure projection mapping to row type, only check name, does not check type
   *
   * @return
   */
  private List<RexNode> sameOrderRowFieldProject(Project project, RelDataType targetRowType, RexBuilder rexBuilder) {
    List<RexNode> projects = project.getChildExps();
    List<RelDataTypeField> targetFieldList = targetRowType.getFieldList();
    int fieldSize = targetFieldList.size();
    int projectSize = projects.size();
    int min = Math.min(fieldSize, projectSize);
    for (int i = 0; i < min; i++) {
      String rexName = ((RexLiteral) ((RexCall) (((RexCall) projects.get(i)).getOperands().get(0))).getOperands().get(1)).getValueAs(String.class);
      String rowTypeName = targetFieldList.get(i).getName();
      if (!Objects.equals(rexName, rowTypeName)) {
        throw new IllegalArgumentException("Row type check failed, " + i + ", " + rexName + " ," + rowTypeName);
      }
    }
    if (fieldSize > projectSize) {
      RexNode node = projects.get(0);
      final RelDataType mapType = ((RexCall) (((RexCall) node).getOperands().get(0))).getOperands().get(0).getType();
      List<RexNode> sourceFields = new ArrayList<>(projects);
      for (int i = projectSize; i < fieldSize; i++) {
        RelDataTypeField targetField = targetFieldList.get(i);
        sourceFields.add(rexBuilder.makeCast(targetField.getType(), rexBuilder.makeCall(SqlStdOperatorTable.ITEM, rexBuilder.makeInputRef(mapType, 0), rexBuilder.makeLiteral(targetField.getName()))));
      }
      return RexUtil.generateCastExpressions(rexBuilder, targetRowType, sourceFields);
    }
    return project.getChildExps();
  }
}

// End ViewTable.java
