package io.github.aloksingh.parquet.util.filter.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class QueryParserTest {

  @Test
  public void testExpression() {
    Map<String, ColumnFilterDescriptor> expressionColumnFilterMap =
        new LinkedHashMap<>();
    expressionColumnFilterMap.put("col1=value1",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("col1=\"value1\"",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("col1=value1*",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.prefix, "value1"));
    expressionColumnFilterMap.put("col1=*value1",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.suffix, "value1"));
    expressionColumnFilterMap.put("col1=*value1*",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.contains, "value1"));
    expressionColumnFilterMap.put("col1=lt(1)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.lt, "1"));
    expressionColumnFilterMap.put("col1=lt(\"2024-01-18T00:00:00Z\")",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.lt, "2024-01-18T00:00:00Z"));
    expressionColumnFilterMap.put("col1=lte(1)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.lte, "1"));
    expressionColumnFilterMap.put("col1=gt(1)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gt, "1"));
    expressionColumnFilterMap.put("col1=gt(1.0)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gt, "1.0"));
    expressionColumnFilterMap.put("col1=gte(1)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gte, "1"));
    expressionColumnFilterMap.put("col1=gte(\"2024-01-18T00:00:00Z\")",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gte, "2024-01-18T00:00:00Z"));
    expressionColumnFilterMap.put("col1=isNull()",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.isNull, null));
    expressionColumnFilterMap.put("col1=isNotNull()",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.isNotNull, null));
    expressionColumnFilterMap.put("foo.bar=value1",
        new ColumnFilterDescriptor("foo.bar", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("\"foo bar\"=value1",
        new ColumnFilterDescriptor("foo bar", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1"));
    expressionColumnFilterMap.put("col1[\"foo bar\"]=value1",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value1", Optional.of("foo bar")));
    expressionColumnFilterMap.put("col1[\"foo bar\"]=gte(1)",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.gte, "1", Optional.of("foo bar")));
    expressionColumnFilterMap.put("col1[\"foo bar\"]=\"value 1\"",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value 1", Optional.of("foo bar")));

    expressionColumnFilterMap.put("col1[\"foo bar\"]='value \" 1'",
        new ColumnFilterDescriptor("col1", LogicalType.PRIMITIVE,
            FilterOperator.eq, "value \" 1", Optional.of("foo bar")));

    QueryParser parser = new BaseQueryParser();
    for (String expression : expressionColumnFilterMap.keySet()) {
      assertEquals(expressionColumnFilterMap.get(expression), parser.parse(expression),
          expression + " parsing was incorrect");
    }
  }
}
