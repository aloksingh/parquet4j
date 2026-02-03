package io.github.aloksingh.parquet;

import java.util.Iterator;
import io.github.aloksingh.parquet.model.RowColumnGroup;

/**
 * An iterator for traversing row-column groups in a Parquet file.
 * <p>
 * This interface extends {@link Iterator} to provide sequential access to
 * {@link RowColumnGroup} instances, representing logical groups of rows and
 * their associated column data within a Parquet file structure.
 * </p>
 *
 * @see RowColumnGroup
 * @see Iterator
 */
public interface RowColumnGroupIterator extends Iterator<RowColumnGroup> {

}
