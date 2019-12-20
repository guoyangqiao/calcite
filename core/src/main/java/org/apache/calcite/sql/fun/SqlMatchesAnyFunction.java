package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        null,
        SqlFunctionCategory.SYSTEM);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    boolean ok = true;
    Set<Class<? extends SqlNode>> valueType = new HashSet<>();
    List<SqlNode> operands = callBinding.operands();
    for (int i = 1; i < operands.size(); i++) {
      Class<? extends SqlNode> valClazz = operands.get(i).getClass();
      valueType.add(valClazz);
    }
    if (valueType.size() > 1) {
      ok = false;
    }
    SqlNode sqlNode = operands.get(0);
    if (!(sqlNode instanceof SqlIdentifier)) {
      ok = false;
    }
    if (!ok && throwOnFailure) {
      throw callBinding.newValidationSignatureError();
    } else {
      return ok;
    }
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return new SqlOperandCountRange() {
      @Override
      public boolean isValidCount(int count) {
        return true;
      }

      @Override
      public int getMin() {
        return 2;
      }

      @Override
      public int getMax() {
        return Integer.MAX_VALUE;
      }
    };
  }
}
