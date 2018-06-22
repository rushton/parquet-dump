package com.tune;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.hadoop.util.HadoopStreams;
import java.io.IOException;

/**
 * implements InputFile for reading InputStreams where
 * you know the length of the input stream
 */
public class InMemoryInputFile implements InputFile {
  private long size;
  private SeekableInputStream stream;
  public InMemoryInputFile(SeekableInputStream stream, long size) {
      this.stream = stream;
      this.size = size;
  }

  @Override
  public long getLength() {
    return size;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return stream;
  }

  @Override
  public String toString() {
    return stream.toString();
  }
}
