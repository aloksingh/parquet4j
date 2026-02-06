#!/usr/bin/env python3
"""
Quick Parquet file inspector using PyArrow

Usage:
    python3 pyarrow_inspect.py <parquet_file>

Example:
    python3 pyarrow_inspect.py src/test/data/lz4_raw_compressed.parquet
"""

import pyarrow.parquet as pq
import sys


def inspect_parquet(file_path, max_rows=20):
    """
    Inspect a Parquet file and print comprehensive information

    Args:
        file_path: Path to the Parquet file
        max_rows: Maximum number of rows to display (default: 20)
    """
    print("=" * 80)
    print(f"PARQUET FILE INSPECTION: {file_path}")
    print("=" * 80)

    # Open the file
    parquet_file = pq.ParquetFile(file_path)
    table = pq.read_table(file_path)

    # Schema
    print("\n### SCHEMA ###")
    print(table.schema)

    # Metadata
    print("\n### METADATA ###")
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    print(f"Number of rows: {len(table)}")
    print(f"Number of columns: {len(table.column_names)}")

    # File metadata
    metadata = parquet_file.metadata
    print(f"\nFile metadata:")
    print(f"  Created by: {metadata.created_by}")
    print(f"  Format version: {metadata.format_version}")
    print(f"  Serialized size: {metadata.serialized_size} bytes")

    # Row group details
    print(f"\nRow Groups:")
    for i in range(parquet_file.num_row_groups):
        rg = metadata.row_group(i)
        print(f"  Row Group {i}:")
        print(f"    Rows: {rg.num_rows}")
        print(f"    Columns: {rg.num_columns}")
        print(f"    Total byte size: {rg.total_byte_size}")

    # Column details
    print("\n### COLUMN DETAILS ###")
    for i, col_name in enumerate(table.column_names):
        col = table.column(col_name)
        values = col.to_pylist()

        print(f"\nColumn {i}: '{col_name}'")
        print(f"  Type: {col.type}")
        print(f"  Length: {len(col)}")
        print(f"  Null count: {col.null_count}")

        # Statistics
        if len(values) > 0:
            unique_count = len(set(str(v) for v in values if v is not None))
            print(f"  Unique values: {unique_count}")

            # Type-specific stats
            if str(col.type) in ['int32', 'int64', 'float', 'double']:
                non_null_values = [v for v in values if v is not None]
                if non_null_values:
                    print(f"  Min: {min(non_null_values)}")
                    print(f"  Max: {max(non_null_values)}")
                    if len(non_null_values) > 1:
                        avg = sum(non_null_values) / len(non_null_values)
                        print(f"  Average: {avg:.2f}")

            # Show sample values
            print(f"  First {min(5, len(values))} values: {values[:5]}")
            if len(values) > 10:
                print(f"  Last 5 values: {values[-5:]}")

    # Data preview
    print("\n### DATA PREVIEW ###")
    df = table.to_pandas()

    if len(df) <= max_rows:
        print(df)
    else:
        print(f"Showing first {max_rows // 2} and last {max_rows // 2} rows:\n")
        print(df.head(max_rows // 2))
        print("...")
        print(df.tail(max_rows // 2))

    # Generate JUnit test template
    print("\n### JUNIT TEST TEMPLATE ###")
    print(generate_junit_template(file_path, table))

    print("\n" + "=" * 80)


def generate_junit_template(file_path, table):
    """Generate a basic JUnit test template based on the data"""

    file_name = file_path.split('/')[-1]
    test_name = ''.join(word.capitalize() for word in file_name.replace('.parquet', '').split('_'))

    template = f"""
@Test
void test{test_name}() throws IOException {{
    String filePath = TEST_DATA_DIR + "{file_name}";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {{
        RowColumnGroupIterator iterator = reader.rowIterator();

        // Expected row count
        assertEquals({len(table)}L, reader.getTotalRowCount());

        // Expected columns
        assertEquals({len(table.column_names)}, reader.getSchema().getNumColumns());
"""

    # Add column assertions
    for i, col_name in enumerate(table.column_names):
        col = table.column(col_name)
        java_type = python_to_java_type(str(col.type))
        template += f"""        assertEquals("{col_name}", reader.getSchema().getColumn({i}).getPathString());
"""

    # Add sample data verification
    values = [table.column(col_name).to_pylist() for col_name in table.column_names]

    if len(table) > 0:
        template += """
        // Verify first row values
        assertTrue(iterator.hasNext());
        RowColumnGroup firstRow = iterator.next();
"""
        for i, col_name in enumerate(table.column_names):
            val = values[i][0]
            template += f"""        assertEquals({format_value(val)}, firstRow.getColumnValue({i}));
"""

    template += """    }
}
"""

    return template


def python_to_java_type(pyarrow_type):
    """Map PyArrow types to Java types"""
    type_map = {
        'int32': 'Integer',
        'int64': 'Long',
        'float': 'Float',
        'double': 'Double',
        'string': 'String',
        'binary': 'String',
        'bool': 'Boolean',
    }

    for key, value in type_map.items():
        if key in pyarrow_type.lower():
            return value

    return 'Object'


def format_value(val):
    """Format a value for Java code"""
    if val is None:
        return 'null'
    elif isinstance(val, str):
        return f'"{val}"'
    elif isinstance(val, bool):
        return 'true' if val else 'false'
    elif isinstance(val, int):
        if val > 2147483647:  # int32 max
            return f'{val}L'
        return str(val)
    elif isinstance(val, float):
        return f'{val}'
    else:
        return f'"{val}"'


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    file_path = sys.argv[1]
    max_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    try:
        inspect_parquet(file_path, max_rows)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error inspecting file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
