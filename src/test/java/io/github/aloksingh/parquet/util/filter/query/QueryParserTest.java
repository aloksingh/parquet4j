package io.github.aloksingh.parquet.util.filter.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class QueryParserTest {

  @Test
  public void testExpression() {
    Map<String, QueryParser.ColumnFilterDescriptor> expressionColumnFilterMap =
        new LinkedHashMap<>();
//col1=value1
//col1="value1"
//col1=value1*
//col1=*value1*
//col1=*value1
//col1=lt(1)
//col1=not(1)
//col1=not("x")
//col1=gt(1)
//col1=gte(2)
//col1=lte(1)
//col1=isNull()
//col1=isNotNull()
//foo.bar=value1
    expressionColumnFilterMap.put("col1=value1",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("col1=\"value1\"",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("col1=value1*",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.prefix, "value1"));
    expressionColumnFilterMap.put("col1=*value1",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.suffix, "value1"));
    expressionColumnFilterMap.put("col1=*value1*",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.contains, "value1"));
    expressionColumnFilterMap.put("col1=lt(1)",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.lt, "1"));
    expressionColumnFilterMap.put("col1=lte(1)",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.lte, "1"));
    expressionColumnFilterMap.put("col1=gt(1)",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gt, "1"));
    expressionColumnFilterMap.put("col1=gte(1)",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gte, "1"));
    expressionColumnFilterMap.put("col1=isNull()",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.isNull, null));
    expressionColumnFilterMap.put("col1=isNotNull()",
        new QueryParser.ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.isNotNull, null));
    expressionColumnFilterMap.put("foo.bar=value1",
        new QueryParser.ColumnFilterDescriptor("foo.bar", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("\"foo bar\"=value1",
        new QueryParser.ColumnFilterDescriptor("foo bar", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));

    QueryParser parser = new BaseQueryParser();
    for (String expression : expressionColumnFilterMap.keySet()) {
      assertEquals(expressionColumnFilterMap.get(expression), parser.parse(expression),
          expression + " parsing was incorrect");
    }
  }
}
