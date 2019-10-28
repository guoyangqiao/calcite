package org.apache.calcite.rex;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ArraySqlType;

import java.util.List;

public class RexList extends RexNode {

  public final List<RexNode> elements;

  public static RexList of(RexNode... rexNodes){
    return of(ImmutableList.copyOf(rexNodes));
  }
  public static RexList of(List<RexNode> rexNodes){
    return new RexList(ImmutableList.copyOf(rexNodes));
  }

  public RexList(List<RexNode> elements) {
    assert elements != null && elements.size() > 0;
    this.elements = elements;
  }

  @Override
  public RelDataType getType() {
    return new ArraySqlType(elements.get(0).getType(), false);
  }

  @Override
  public <R> R accept(RexVisitor<R> visitor) {
    return null;
  }

  @Override
  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj instanceof RexList && (((RexList) obj).elements.equals(this.elements)));
  }

  @Override
  public int hashCode() {
    return elements.hashCode();
  }
}
