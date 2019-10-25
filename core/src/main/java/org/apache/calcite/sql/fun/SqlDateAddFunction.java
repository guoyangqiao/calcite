package org.apache.calcite.sql.fun;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;


/**
 * Copy from {@link org.apache.calcite.sql.fun.SqlTimestampAddFunction}
 */
public class SqlDateAddFunction extends SqlFunction {

  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return deduceType(typeFactory,
            opBinding.getOperandLiteralValue(0, TimeUnit.class),
            opBinding.getOperandType(1), opBinding.getOperandType(2));
      };

  public static RelDataType deduceType(RelDataTypeFactory typeFactory,
                                       TimeUnit timeUnit, RelDataType operandType1, RelDataType operandType2) {
    return SqlTimestampAddFunction.deduceType(typeFactory, timeUnit, operandType1, operandType2);
  }

  SqlDateAddFunction() {
    super("DATE_ADD", SqlKind.TIMESTAMP_ADD, RETURN_TYPE_INFERENCE, null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER,
            SqlTypeFamily.DATETIME),
        SqlFunctionCategory.TIMEDATE);
  }
}