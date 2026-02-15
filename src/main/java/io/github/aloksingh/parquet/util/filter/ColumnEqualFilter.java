package io.github.aloksingh.parquet.util.filter;

import com.google.common.base.Objects;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;
import java.util.Map;

public class ColumnEqualFilter implements ColumnFilter{
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Object matchValue;

  public ColumnEqualFilter(LogicalColumnDescriptor targetColumnDescriptor, Object matchValue) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      return java.util.Objects.equals(matchValue, colValue);
    } else {
      if (targetColumnDescriptor.isList()) {
        List listValues = (List) colValue;
        List matches = (List) matchValue;
        if (listValues.size() != matches.size()){
          return false;
        }
        for (int i = 0; i < matches.size(); i++) {
          Object m = matches.get(i);
          Object v = listValues.get(i);
          if (!Objects.equal(m, v)){
            return false;
          }
        }
        return true;
      }
      if (targetColumnDescriptor.isMap()) {
        Map valueMap = (Map) colValue;
        Map matchMap = (Map) matchValue;
        if (valueMap.size() != matchMap.size()){
          return false;
        }
        for (Object key : matchMap.keySet()) {
          if (!valueMap.containsKey(key)){
            return false;
          }
          Object v = valueMap.get(key);
          Object m = matchMap.get(key);
          if (!java.util.Objects.equals(v, m)){
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }
}
