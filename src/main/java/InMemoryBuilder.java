package com.tune;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.hadoop.api.ReadSupport;

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
