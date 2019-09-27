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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlInOperator;

import java.util.Set;

/**
 * Internal constants referenced in this package.
 */
interface ElasticsearchConstants {
  String ES_MATCH_AND = "ES_MATCH_AND";
  String ES_MATCH_OR = "ES_MATCH_OR";
  String WHITE_SPACE = " ";
  String AND = "and";
  String OR = "or";
  String INDEX = "_index";
  String TYPE = "_type";
  String FIELDS = "fields";
  String SOURCE_PAINLESS = "params._source";
  String SOURCE_GROOVY = "_source";

  /**
   * Attribute which uniquely identifies a document (ID)
   *
   * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html">ID Field</a>
   */
  String ID = "_id";
  String UID = "_uid";

  Set<String> META_COLUMNS = ImmutableSet.of(UID, ID, TYPE, INDEX);

  /**
   * Detects {@code select * from elastic} types of field name (select star).
   *
   * @param name name of the field
   * @return {@code true} if this field represents whole raw, {@code false} otherwise
   */
  static boolean isSelectAll(String name) {
    return "_MAP".equals(name);
  }

  class EsMatchOperator extends SqlInOperator {

    private final String operator;

    EsMatchOperator(String name, SqlKind kind, String operator) {
      super(name, kind);
      this.operator = operator;
    }

    public String getOperator() {
      return operator;
    }

    @Override
    public SqlSyntax getSyntax() {
      return SqlSyntax.BINARY;
    }
  }

  SqlOperator MATCH_AND_SQL_OPERATOR = new EsMatchOperator(ES_MATCH_AND, SqlKind.OTHER_FUNCTION, AND);

  SqlOperator MATCH_OR_SQL_OPERATOR = new EsMatchOperator(ES_MATCH_OR, SqlKind.OTHER_FUNCTION, OR);

  /**
   * Trim SQL like percent sign around
   */
  static String trimPercentSign(RexLiteral rexNode) {
    String s = String.valueOf(rexNode.getValue());
    if (s.startsWith("%")) {
      s = s.substring(1);
    }
    if (s.endsWith("%")) {
      s = s.substring(0, s.length() - 1);
    }
    return s;
  }
}

// End ElasticsearchConstants.java
