package org.apache.calcite.schema.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.type.DynamicFieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Default implement of {@link CustomColumnResolvingTable}, dynamic table may benefit from this class
 */
public interface AbstractCustomColumnResolvingTable extends CustomColumnResolvingTable {

  default List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType rowType, RelDataTypeFactory typeFactory, List<String> names) {
    final RelDataTypeField rtn = getField(rowType, names.get(0));
    RelDataType previous = rowType;
    for (int i = 0; i < names.size() - 1; i++) {
      String filedName = names.get(i);
      RelDataTypeField field = getField(previous, filedName);
      RelDataType type = field.getType();
      if (type instanceof DynamicFieldType) {
        ((DynamicFieldType) type).switchToRelay();
        previous = type;
      }
    }
    getField(previous, Iterables.getLast(names));
    return ImmutableList.of(new Pair<>(rtn, ImmutableList.copyOf(names.subList(1, names.size()))));
  }

  static RelDataTypeField getField(RelDataType relDataType, String filedName) {
    return relDataType.getField(filedName, true, false);
  }
}
