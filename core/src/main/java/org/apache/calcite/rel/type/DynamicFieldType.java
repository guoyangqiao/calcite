package org.apache.calcite.rel.type;

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.util.List;

public class DynamicFieldType extends RelDataTypeImpl {

  private final DynamicRecordType relayType;
  private final RelDataType endPointType;

  private RelDataType delegate;

  public void switchToRelay() {
    this.delegate = relayType;
    computeDigest();
  }

  void switchToEndPoint() {
    this.delegate = endPointType;
    computeDigest();
  }

  public DynamicFieldType(DynamicRecordType relayType, RelDataType endPointType) {
    this.relayType = relayType;
    this.endPointType = endPointType;
    switchToEndPoint();
  }

  public boolean isStruct() {
    return delegate.isStruct();
  }

  public boolean isDynamicStruct() {
    return delegate.isDynamicStruct();
  }

  public List<RelDataTypeField> getFieldList() {
    return delegate.getFieldList();
  }

  public List<String> getFieldNames() {
    return delegate.getFieldNames();
  }

  public int getFieldCount() {
    return delegate.getFieldCount();
  }

  public StructKind getStructKind() {
    return StructKind.PEEK_FIELDS_DEFAULT;
  }

  public RelDataTypeField getField(String fieldName, boolean caseSensitive,
                                   boolean elideRecord) {
    return delegate.getField(fieldName, caseSensitive, elideRecord);
  }

  public boolean isNullable() {
    return delegate.isNullable();
  }

  public RelDataType getComponentType() {
    return delegate.getComponentType();
  }

  public RelDataType getKeyType() {
    return delegate.getKeyType();
  }

  public RelDataType getValueType() {
    return delegate.getValueType();
  }

  public Charset getCharset() {
    return delegate.getCharset();
  }

  public SqlCollation getCollation() {
    return delegate.getCollation();
  }

  public SqlIntervalQualifier getIntervalQualifier() {
    return delegate.getIntervalQualifier();
  }

  public int getPrecision() {
    return delegate.getPrecision();
  }

  public int getScale() {
    return delegate.getScale();
  }

  public SqlTypeName getSqlTypeName() {
    return delegate.getSqlTypeName();
  }

  public SqlIdentifier getSqlIdentifier() {
    return delegate.getSqlIdentifier();
  }

  public String getFullTypeString() {
    return delegate.getFullTypeString();
  }

  public RelDataTypeFamily getFamily() {
    return delegate.getFamily();
  }

  public void generateTypeString(StringBuilder sb, boolean withDetail) {
    if (delegate instanceof RelDataTypeImpl) {
      ((RelDataTypeImpl) delegate).generateTypeString(sb, withDetail);
    }
  }

  public RelDataTypePrecedenceList getPrecedenceList() {
    return delegate.getPrecedenceList();
  }

  public RelDataTypeComparability getComparability() {
    return delegate.getComparability();
  }

}
