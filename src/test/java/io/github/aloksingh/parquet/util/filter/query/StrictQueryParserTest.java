package io.github.aloksingh.parquet.util.filter.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.FilterOperator;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StrictQueryParserTest {
    private final QueryParser parser = new BaseQueryParser();

    static Stream<Arguments> validExpressions() {
        return Stream.of(
                Arguments.of("\"col=with[brackets]\"['key=with]brackets[']=\"a=b[c]\"",
                        "col=with[brackets]", "key=with]brackets[", FilterOperator.eq, "a=b[c]"),
                Arguments.of(" 'odd column' [ \"it's = [ok]\" ] = neq('it\\'s \\\\ fine') ",
                        "odd column", "it's = [ok]", FilterOperator.neq, "it's \\ fine"),
                Arguments.of("map['key']=gte( 12 )", "map", "key", FilterOperator.gte, "12"),
                Arguments.of("map[key]=isNull()", "map", "key", FilterOperator.isNull, null),
                Arguments.of("name=neq(\"a)b=c[d]\")", "name", null, FilterOperator.neq, "a)b=c[d]"),
                Arguments.of("name='*literal*'", "name", null, FilterOperator.eq, "*literal*"),
                Arguments.of("name=\\*literal\\*", "name", null, FilterOperator.eq, "*literal*"),
                Arguments.of("name=\\*literal*", "name", null, FilterOperator.prefix, "*literal"),
                Arguments.of("name=*literal\\*", "name", null, FilterOperator.suffix, "literal*"),
                Arguments.of("name=*'a*b'*", "name", null, FilterOperator.contains, "a*b"),
                Arguments.of("name='a\\\"b\\'c'", "name", null, FilterOperator.eq, "a\"b'c"),
                Arguments.of("name=eq('')", "name", null, FilterOperator.eq, ""),
                Arguments.of("name=\"a\\nb\\tc\"", "name", null, FilterOperator.eq, "a\nb\tc"),
                Arguments.of("name=contains('a*b')", "name", null, FilterOperator.contains, "a*b"),
                Arguments.of("name=*", "name", null, FilterOperator.contains, ""));
    }

    @ParameterizedTest
    @MethodSource("validExpressions")
    void quoteAndEscapeAwareExpressionsPreserveTheCompleteLiteral(String expression, String name,
                                                                  String key, FilterOperator operator, String value) {
        assertEquals(new ColumnFilterDescriptor(name, LogicalType.PRIMITIVE, operator, value,
                Optional.ofNullable(key)), parser.parse(expression));
    }

    static Stream<String> malformedExpressions() {
        return Stream.of("", "value", "=value", "name=", "name='unterminated", "name=\"unterminated",
                "'unterminated=value", "map['key]=value", "map[\"key\"]=value trailing",
                "map[\"key\"]junk=value", "map[key][other]=value", "map[]=value", "map[key=value",
                "name=lt(1)junk", "name=lt(1", "name=lt()", "name=isNull(value)", "name=isNotNull(1)",
                "name=unknown(1)", "name=eq('a') trailing", "name='a'junk", "name=a=b",
                "name=a[bad]", "name=foo*bar", "name='a' 'b'", "name=foo\\", "name='bad\\q'",
                "name=gt(1,2)", "name=gt(gt(1))", "name=neq(\"unclosed)", "map[ 'key' = value");
    }

    @ParameterizedTest
    @MethodSource("malformedExpressions")
    void unmatchedDelimitersUnknownFunctionsAndTrailingInputAreRejected(String expression) {
        assertThrows(IllegalArgumentException.class, () -> parser.parse(expression), expression);
    }
}
