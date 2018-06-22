package com.tune;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.PositionedReadable;

class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable  {
    public SeekableByteArrayInputStream(byte[] buf) {
        super(buf);
        this.pos = pos;
    }

    public void seek(long pos) throws IOException {
        this.pos = java.lang.Math.toIntExact(pos);
    }

    public long getPos() throws IOException {
        return Long.valueOf(this.pos);
    }


    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        seek(position);
        return this.read(buffer, offset, length);
    }

    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        long lastPos = getPos();
        seek(position);
        this.read(buffer, offset, length);
        seek(lastPos);
    }

    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }
}
