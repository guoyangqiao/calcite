package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SqlMatchesAnyFunction extends SqlFunction {

  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding -> {
        //TODO operand type check
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createJavaType(boolean.class);
      };

  public SqlMatchesAnyFunction() {
    super(
        "MATCHES_ANY",
        SqlKind.OTHER_FUNCTION,
        RETURN_TYPE_INFERENCE,
        null,//TODO
        null,//TODO
        SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION);
  }
}
