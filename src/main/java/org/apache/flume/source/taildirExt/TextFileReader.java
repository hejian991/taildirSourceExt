package org.apache.flume.source.taildirExt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flume.source.taildirExt.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import net.csdn.common.collect.Tuple;

public class TextFileReader implements org.apache.flume.source.taildirExt.FileReader {
    private static final Logger logger = LoggerFactory.getLogger(TextFileReader.class);

    private final File file;
    private LineReader lineReader;
    private long lineNo;
    private long handledBytes;
    private long readerReadlineBatch;
    private final long readerReadBatchSleep;

    public TextFileReader(File file, long readerReadlineBatch, long readerReadBatchSleep)
            throws FileNotFoundException {
        this.file = file;
        FileReader fileReader = new FileReader(file);
        this.lineReader = new LineReader(fileReader);
        lineNo = 0L;
        handledBytes = 0L;
        this.readerReadlineBatch = readerReadlineBatch;
        this.readerReadBatchSleep = readerReadBatchSleep;
    }

    @Override
    public void seek(long lineNo) throws IOException {

        logger.info(String.format("position start:[lineNo=%d], [file=%s], [fileSize=%d B]",
                lineNo, file.getAbsolutePath(), file.length()));
        long t = TimeUtil.currentTimeMs();

        this.lineReader = new LineReader(new FileReader(file));
        long count = 0L;
        while (lineNo-- > 0) {
            if (lineReader.readLineTuple() == null) {
                break;
            } else {
                count++;

                if (count % readerReadlineBatch == 0) {
                    try {
                        Thread.sleep(readerReadBatchSleep);
                    } catch (InterruptedException e) {
                        logger.info("don't need care.");
                    }
                    logger.debug(String.format("sleep %d ms, when position:[count=%d], [file=%s]", readerReadBatchSleep,
                            count, file.getAbsolutePath()));
                }

            }
        }
        this.lineNo = count;

        logger.info(String.format("position end:[count=%d], [file=%s]", count, file.getAbsolutePath()));
        logger.info(TimeUtil.recordTime(file.getAbsolutePath() + " position time", t));
    }

    @Override
    public long tell() {
        return lineNo;
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }


    @Override
    public byte[] readRecord() throws IOException {
        Tuple<String, String> lineTuple;
        if ((lineTuple = lineReader.readLineTuple()) != null) {
            lineNo++;
            handledBytes += lineTuple.v1().getBytes().length + lineTuple.v2().getBytes().length;
        }
        return lineTuple == null ? null : lineTuple.v1().getBytes(Charsets.UTF_8);
    }

    @Override
    public long getHandledBytes() {
        return handledBytes;
    }
    @Override
    public void setHandledBytes(long handledBytes) {
        this.handledBytes = handledBytes;
    }


    @Override
    public void setHandledRecords(long records) {
        // don't need do anything, seek(long lineNo) do.
    }

    @Override
    public long getHandledRecords() {
        return lineNo;
    }

}
