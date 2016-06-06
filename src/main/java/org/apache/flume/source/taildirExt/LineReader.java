package org.apache.flume.source.taildirExt;

/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static net.csdn.common.collect.Tuple.tuple;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

import net.csdn.common.collect.Tuple;

/**
 * A class for reading lines of text. Provides the same functionality
 * as {@link java.io.BufferedReader#readLine()} but for all {@link Readable}
 * objects, not just instances of {@link Reader}.
 *
 * @author Chris Nokleberg
 * @since 1.0
 */
@Beta
public final class LineReader {
    private final Readable readable;
    private final Reader reader;
    private final char[] buf = new char[0x1000]; // 4K
    private final CharBuffer cbuf = CharBuffer.wrap(buf);

    private final Queue<Tuple<String, String>> lines = new LinkedList<Tuple<String, String>>();
    private final LineBuffer lineBuf = new LineBuffer() {
        @Override
        protected void handleLine(String line, String end) {
            lines.add(tuple(line, end));
        }
    };

    /**
     * Creates a new instance that will read lines from the given
     * {@code Readable} object.
     */
    public LineReader(Readable readable) {
        Preconditions.checkNotNull(readable);
        this.readable = readable;
        this.reader = (readable instanceof Reader) ? (Reader) readable : null;
    }

    /**
     * Reads a line of text. A line is considered to be terminated by any
     * one of a line feed ({@code '\n'}), a carriage return
     * ({@code '\r'}), or a carriage return followed immediately by a linefeed
     * ({@code "\r\n"}).
     *
     * @return a {@code String} containing the contents of the line, not
     * including any line-termination characters, or {@code null} if the
     * end of the stream has been reached.
     *
     * @throws IOException if an I/O error occurs
     */
    public Tuple<String, String> readLineTuple() throws IOException {
        while (lines.peek() == null) {
            cbuf.clear();
            // The default implementation of Reader#read(CharBuffer) allocates a
            // temporary char[], so we call Reader#read(char[], int, int) instead.
            int read = (reader != null)
                    ? reader.read(buf, 0, buf.length)
                    : readable.read(cbuf);
            if (read == -1) {
                lineBuf.finish();
                break;
            }
            lineBuf.add(buf, 0, read);
        }
        return lines.poll();
    }

//    public String readLine() throws IOException {
//        Tuple<String, String> lineTuple = this.readLineTuple();
//        String line = lineTuple == null ? null : lineTuple.v1();
//        return line;
//    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}