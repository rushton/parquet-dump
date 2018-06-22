package com.tune;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.hadoop.api.ReadSupport;

/**
 * opens up access to ParquetReader constructor for building
 * a ParquetReader from an InputFile directly, allows us to
 * use in-memory data structures instead of having to pass
 * a file path which must be on disk.
 */
public class InMemoryBuilder<T> extends ParquetReader.Builder<T> {
    private ReadSupport<T> readSupport;
    public InMemoryBuilder(InputFile file) {
        super(file);
    }

    protected ReadSupport<T> getReadSupport() {
        return readSupport;
    }

    public InMemoryBuilder<T> useReadSupport(ReadSupport<T> readSupport) {
        this.readSupport = readSupport;
        return this;
    }
}
