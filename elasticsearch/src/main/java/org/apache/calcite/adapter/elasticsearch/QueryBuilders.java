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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to generate elastic search queries. Most query builders have
 * been copied from ES distribution. The reason we have separate definition is
 * high-level client dependency on core modules (like lucene, netty, XContent etc.) which
 * is not compatible between different major versions.
 *
 * <p>The goal of ES adapter is to
 * be compatible with any elastic version or even to connect to clusters with different
 * versions simultaneously.
 *
 * <p>Jackson API is used to generate ES query as JSON document.
 */
class QueryBuilders {

  private QueryBuilders() {
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, String value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, int value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a single character term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, char value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, long value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, float value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, double value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, boolean value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A Query that matches documents containing a term.
   *
   * @param name  The name of the field
   * @param value The value of the term
   */
  static TermQueryBuilder termQuery(String name, Object value) {
    return new TermQueryBuilder(name, value);
  }

  /**
   * A filer for a field based on several terms matching on any of them.
   *
   * @param name   The field name
   * @param values The terms
   */
  static TermsQueryBuilder termsQuery(String name, Iterable<?> values) {
    return new TermsQueryBuilder(name, values);
  }

  /**
   * A Query that matches documents within an range of terms.
   *
   * @param name The field name
   */
  static RangeQueryBuilder rangeQuery(String name) {
    return new RangeQueryBuilder(name);
  }

  /**
   * A Query that matches documents containing terms with a specified regular expression.
   *
   * @param name   The name of the field
   * @param regexp The regular expression
   */
  static RegexpQueryBuilder regexpQuery(String name, String regexp) {
    return new RegexpQueryBuilder(name, regexp);
  }


  /**
   * A Query that matches documents matching boolean combinations of other queries.
   */
  static BoolQueryBuilder boolQuery() {
    return new BoolQueryBuilder();
  }

  /**
   * A query that wraps another query and simply returns a constant score equal to the
   * query boost for every document in the query.
   *
   * @param queryBuilder The query to wrap in a constant score query
   */
  static ConstantScoreQueryBuilder constantScoreQuery(QueryBuilder queryBuilder) {
    return new ConstantScoreQueryBuilder(queryBuilder);
  }

  /**
   * A filter to filter only documents where a field exists in them.
   *
   * @param name The name of the field
   */
  static ExistsQueryBuilder existsQuery(String name) {
    return new ExistsQueryBuilder(name);
  }

  /**
   * A query that matches on all documents.
   */
  static MatchAllQueryBuilder matchAll() {
    return new MatchAllQueryBuilder();
  }

  static MatchQueryBuilder matchQuery(String name, Object text, String operator) {
    return new MatchQueryBuilder(name, text, "match", operator, 1);
  }

  static MatchQueryBuilder matchPhraseQuery(String name, Object text, String operator) {
    return new MatchQueryBuilder(name, text, "match_phrase", operator, 1);
  }

  /**
   * A query that find parent documents according to child documents condition
   *
   * @param childType  Child type name defined in join data type
   * @param childQuery a query that filter child documents
   */
  static HasChildQueryBuilder hasChild(String childType, QueryBuilder childQuery) {
    return new HasChildQueryBuilder(childType, childQuery);
  }

  /**
   * A query that find parent documents
   *
   * @param childType Child type name defined in join data type
   */
  static HasChildQueryBuilder hasChild(String childType) {
    return new HasChildQueryBuilder(childType, QueryBuilders.matchAll());
  }

  /**
   * Base class to build ES queries
   */
  abstract static class QueryBuilder {

    /**
     * Convert existing query to JSON format using jackson API.
     *
     * @param generator used to generate JSON elements
     * @throws IOException if IO error occurred
     */
    abstract void writeJson(JsonGenerator generator) throws IOException;
  }

  /**
   * Query for boolean logic
   */
  static class BoolQueryBuilder extends QueryBuilder {
    private final List<QueryBuilder> mustClauses = new ArrayList<>();
    private final List<QueryBuilder> mustNotClauses = new ArrayList<>();
    private final List<QueryBuilder> filterClauses = new ArrayList<>();
    private final List<QueryBuilder> shouldClauses = new ArrayList<>();

    BoolQueryBuilder must(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      mustClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder filter(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      filterClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder mustNot(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      mustNotClauses.add(queryBuilder);
      return this;
    }

    BoolQueryBuilder should(QueryBuilder queryBuilder) {
      Objects.requireNonNull(queryBuilder, "queryBuilder");
      shouldClauses.add(queryBuilder);
      return this;
    }

    @Override
    protected void writeJson(JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeFieldName("bool");
      gen.writeStartObject();
      writeJsonArray("must", mustClauses, gen);
      writeJsonArray("filter", filterClauses, gen);
      writeJsonArray("must_not", mustNotClauses, gen);
      writeJsonArray("should", shouldClauses, gen);
      gen.writeEndObject();
      gen.writeEndObject();
    }

    private void writeJsonArray(String field, List<QueryBuilder> clauses, JsonGenerator gen)
        throws IOException {
      if (clauses.isEmpty()) {
        return;
      }

      if (clauses.size() == 1) {
        gen.writeFieldName(field);
        clauses.get(0).writeJson(gen);
      } else {
        gen.writeArrayFieldStart(field);
        for (QueryBuilder clause : clauses) {
          clause.writeJson(gen);
        }
        gen.writeEndArray();
      }
    }
  }

  /**
   * A Query that matches documents containing a term.
   */
  static class TermQueryBuilder extends QueryBuilder {
    private final String fieldName;
    private final Object value;

    private TermQueryBuilder(final String fieldName, final Object value) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.value = Objects.requireNonNull(value, "value");
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("term");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      writeObject(generator, value);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A filter for a field based on several terms matching on any of them.
   */
  private static class TermsQueryBuilder extends QueryBuilder {
    private final String fieldName;
    private final Iterable<?> values;

    private TermsQueryBuilder(final String fieldName, final Iterable<?> values) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.values = Objects.requireNonNull(values, "values");
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("terms");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      generator.writeStartArray();
      for (Object value : values) {
        writeObject(generator, value);
      }
      generator.writeEndArray();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * Write usually simple (scalar) value (string, number, boolean or null) to json output.
   * In case of complex objects delegates to jackson serialization.
   *
   * @param generator api to generate JSON document
   * @param value     JSON value to write
   * @throws IOException if can't write to output
   */
  private static void writeObject(JsonGenerator generator, Object value) throws IOException {
    generator.writeObject(value);
  }

  /**
   * A Query that matches documents within an range of terms.
   */
  static class RangeQueryBuilder extends QueryBuilder {
    private final String fieldName;

    private Object lt;
    private boolean lte;
    private Object gt;
    private boolean gte;

    private String format;

    private RangeQueryBuilder(final String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    private RangeQueryBuilder to(Object value, boolean lte) {
      this.lt = Objects.requireNonNull(value, "value");
      this.lte = lte;
      return this;
    }

    private RangeQueryBuilder from(Object value, boolean gte) {
      this.gt = Objects.requireNonNull(value, "value");
      this.gte = gte;
      return this;
    }

    RangeQueryBuilder lt(Object value) {
      return to(value, false);
    }

    RangeQueryBuilder lte(Object value) {
      return to(value, true);
    }

    RangeQueryBuilder gt(Object value) {
      return from(value, false);
    }

    RangeQueryBuilder gte(Object value) {
      return from(value, true);
    }

    RangeQueryBuilder format(String format) {
      this.format = format;
      return this;
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      if (lt == null && gt == null) {
        throw new IllegalStateException("Either lower or upper bound should be provided");
      }

      generator.writeStartObject();
      generator.writeFieldName("range");
      generator.writeStartObject();
      generator.writeFieldName(fieldName);
      generator.writeStartObject();

      if (gt != null) {
        final String op = gte ? "gte" : "gt";
        generator.writeFieldName(op);
        writeObject(generator, gt);
      }

      if (lt != null) {
        final String op = lte ? "lte" : "lt";
        generator.writeFieldName(op);
        writeObject(generator, lt);
      }

      if (format != null) {
        generator.writeStringField("format", format);
      }

      generator.writeEndObject();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A Query that does fuzzy matching for a specific value.
   * We treat fuzzy matching equals ES match which is not the RIGHT way.
   * We recommend that use LIKE to match fulltext single word math such as "%foo%".
   * "%foo" "bar%" "%foo bar%" may lead to unexpected result
   * <p>
   * Case 1: "foo ba%", match_phrase_prefix
   * Case 2: "%foo bar", not found
   * Case 3: "%foo bar%", match_phrase
   */
  static class RegexpQueryBuilder extends QueryBuilder {
    private MatchQueryBuilder matchQueryBuilder;

    RegexpQueryBuilder(final String fieldName, final String value) {
      this.matchQueryBuilder = matchPhraseQuery(fieldName, value, defineOperator(value));
    }

    /**
     * If value split with whitespace and are words, we identified words' relation as OR, otherwise AND
     */
    private String defineOperator(String value) {
      String operator;
      if (value.startsWith("%") || value.endsWith("%")) {
        operator = ElasticsearchConstants.AND;
      } else {
        operator = ElasticsearchConstants.OR;
      }
      return operator;
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      this.matchQueryBuilder.writeJson(generator);
    }
  }

  /**
   * Constructs a query that only match on documents that the field has a value in them.
   */
  static class ExistsQueryBuilder extends QueryBuilder {
    private final String fieldName;

    ExistsQueryBuilder(final String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("exists");
      generator.writeStartObject();
      generator.writeStringField("field", fieldName);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A query that wraps a filter and simply returns a constant score equal to the
   * query boost for every document in the filter.
   */
  static class ConstantScoreQueryBuilder extends QueryBuilder {

    private final QueryBuilder builder;

    private ConstantScoreQueryBuilder(final QueryBuilder builder) {
      this.builder = Objects.requireNonNull(builder, "builder");
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("constant_score");
      generator.writeStartObject();
      generator.writeFieldName("filter");
      builder.writeJson(generator);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A query that matches on all documents.
   * <pre>
   *   {
   *     "match_all": {}
   *   }
   * </pre>
   */
  static class MatchAllQueryBuilder extends QueryBuilder {

    private MatchAllQueryBuilder() {
    }

    @Override
    void writeJson(final JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("match_all");
      generator.writeStartObject();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  /**
   * A query that matches on condition
   */
  static class MatchQueryBuilder extends QueryBuilder {

    private final String operator;
    private final long minimumShouldMatch;
    private final String name;
    private final Object text;
    private final String fulltextMatchType;

    private MatchQueryBuilder(String name, Object text, String fulltextMatchType, String operator, long minimumShouldMatch) {
      this.name = name;
      this.text = text;
      this.fulltextMatchType = fulltextMatchType;
      if ((!ElasticsearchConstants.AND.equals(operator) && !ElasticsearchConstants.OR.equals(operator))) {
        throw new RuntimeException("Match query can use only AND OR");
      }
      this.operator = operator;
      this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    void writeJson(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName(fulltextMatchType);
      generator.writeStartObject();
      generator.writeFieldName(name);
      generator.writeStartObject();
      generator.writeFieldName("query");
      generator.writeObject(text);
      generator.writeFieldName("operator");
      generator.writeString(operator);
      generator.writeFieldName("minimum_should_match");
      generator.writeNumber(minimumShouldMatch);
      generator.writeEndObject();
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }

  static class HasChildQueryBuilder extends QueryBuilder {
    private String childType;
    private QueryBuilder childQuery;

    private HasChildQueryBuilder(String childType, QueryBuilder childQuery) {
      this.childType = childType;
      this.childQuery = childQuery;
    }

    /**
     * "min_children": 0,
     * "max_children": 2147483647,
     *
     * @param generator used to generate JSON elements
     * @throws IOException
     */
    @Override
    void writeJson(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeFieldName("has_child");
      generator.writeStartObject();
      generator.writeFieldName("type");
      generator.writeString(childType);
      generator.writeFieldName("query");
      childQuery.writeJson(generator);
      generator.writeFieldName("max_children");
      generator.writeNumber(0X7FFFFFFF);
      generator.writeFieldName("min_children");
      generator.writeNumber(0);
      generator.writeEndObject();
      generator.writeEndObject();
    }
  }
}

// End QueryBuilders.java
