package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.util.Optional;

/**
 * Parses one complete {@code column[optional-key]=predicate} expression. Names, keys and literals
 * may use single or double quotes. Delimiters inside quotes are literal. Backslash escapes quotes,
 * backslash, delimiters, wildcards and {@code n/r/t}. Only unquoted, unescaped edge stars are
 * wildcards: {@code a*}, {@code *a}, {@code *a*}; a lone star matches every non-null string.
 * All {@link FilterOperator} function forms, including {@code eq(value)} and {@code neq(value)},
 * are supported. Null checks take no argument. Extra input and malformed expressions are errors.
 */
public class BaseQueryParser implements QueryParser {
  @Override
  public ColumnFilterDescriptor parse(String expression) {
    if (expression == null) throw new IllegalArgumentException("Expression must not be null");
    Cursor cursor = new Cursor(expression);
    cursor.whitespace();
    String column = cursor.token();
    if (column.isEmpty()) throw cursor.error("column name must not be empty");
    cursor.whitespace();
    Optional<String> key = Optional.empty();
    if (cursor.take('[')) {
      cursor.whitespace();
      key = Optional.of(cursor.token());
      cursor.whitespace();
      cursor.require(']');
      cursor.whitespace();
    }
    cursor.require('=');
    cursor.whitespace();

    FilterOperator operator;
    Object value;
    int start = cursor.position;
    String function = cursor.word();
    cursor.whitespace();
    if (!function.isEmpty() && cursor.take('(')) {
      try {
        operator = FilterOperator.valueOf(function);
      } catch (IllegalArgumentException e) {
        throw cursor.error("unknown operator '" + function + "'");
      }
      cursor.whitespace();
      if (operator == FilterOperator.isNull || operator == FilterOperator.isNotNull) {
        value = null;
      } else {
        value = cursor.token();
        cursor.whitespace();
      }
      cursor.require(')');
    } else {
      cursor.position = start;
      boolean leadingStar = cursor.take('*');
      if (leadingStar && (cursor.end() || cursor.peek('*') || cursor.atWhitespace())) {
        value = "";
      } else {
        value = cursor.token();
      }
      cursor.whitespace();
      boolean trailingStar = cursor.take('*');
      operator = leadingStar && (trailingStar || value.equals("")) ? FilterOperator.contains
          : leadingStar ? FilterOperator.suffix
          : trailingStar ? FilterOperator.prefix : FilterOperator.eq;
    }
    cursor.whitespace();
    if (!cursor.end()) throw cursor.error("unexpected trailing input");
    return new ColumnFilterDescriptor(column, LogicalType.PRIMITIVE, operator, value, key);
  }

  private static final class Cursor {
    private final String input;
    private int position;

    private Cursor(String input) {
      this.input = input;
    }

    private boolean end() {
      return position == input.length();
    }

    private boolean peek(char c) {
      return !end() && input.charAt(position) == c;
    }

    private boolean take(char c) {
      if (!peek(c)) return false;
      position++;
      return true;
    }

    private void require(char c) {
      if (!take(c)) throw error("expected '" + c + "'");
    }

    private boolean atWhitespace() {
      return !end() && Character.isWhitespace(input.charAt(position));
    }

    private void whitespace() {
      while (atWhitespace()) position++;
    }

    private String word() {
      int start = position;
      while (!end() && (Character.isLetterOrDigit(input.charAt(position))
          || input.charAt(position) == '_')) position++;
      return input.substring(start, position);
    }

    private String token() {
      if (peek('\'') || peek('"')) return quoted();
      StringBuilder value = new StringBuilder();
      while (!end()) {
        char c = input.charAt(position);
        if (Character.isWhitespace(c) || "[]=(),*\"'".indexOf(c) >= 0) break;
        position++;
        value.append(c == '\\' ? escaped() : c);
      }
      if (value.isEmpty()) throw error("expected a literal or quoted token");
      return value.toString();
    }

    private String quoted() {
      char quote = input.charAt(position++);
      StringBuilder value = new StringBuilder();
      while (!end()) {
        char c = input.charAt(position++);
        if (c == quote) return value.toString();
        value.append(c == '\\' ? escaped() : c);
      }
      throw error("unmatched quote");
    }

    private char escaped() {
      if (end()) throw error("unfinished escape");
      char c = input.charAt(position++);
      return switch (c) {
        case 'n' -> '\n';
        case 'r' -> '\r';
        case 't' -> '\t';
        case '\\', '\'', '"', '[', ']', '=', '(', ')', ',', '*' -> c;
        default -> throw error("unsupported escape \\" + c);
      };
    }

    private IllegalArgumentException error(String message) {
      return new IllegalArgumentException("Invalid expression at position " + position + ": "
          + message + " in " + input);
    }
  }
}
