package org.apache.flume.source.taildirExt;

import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.FILENAME_HEADER_KEY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.INODE_HEADER_KEY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.LINE_OFFSET_HEADER_KEY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.TIMESTAMP_KEY;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.taildirExt.seqfile.SeqfileFileReader;
import org.apache.flume.source.taildirExt.util.TimeUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TailFile2 {
    private static final Logger logger = LoggerFactory.getLogger(TailFile2.class);

    private FileReader fileReader;

    private final String path;
    private final File file;
    private final String fileName;
    private final long inode;

    private long pos;
    private long bytes;
    private long recordNum;
    private long lastUpdated; // 明确定义： 上次本应用处理的时间，commit时修改 以上三个属性
    private boolean needTail; // 不再需要了, 通过 FileDiff.calNeedProcessInodes 判断了

    private final Map<String, String> headers;

    private final boolean isBackTracking;
    private long timestampInFile = 0L;

    @Override
    public String toString() {
        return String.format("TailFile2{path=%s,inode=%d,pos=%d,bytes=%d,lastUpdated=%s,"
                        + "fileReader.position=%d,fileReader.handledBytes=%d,isBackTracking=%s,realBytes=%d}",
                path, inode, pos, bytes,
                TimeUtils.ms2String(lastUpdated),
                fileReader != null ? position() : -1,
                fileReader != null ? handledBytes() : -1,
                isBackTracking,
                file.length());
    }

    public String uuid() {
        return String.format("TailFile2{path=%s,inode=%d}", path, inode);
    }

    public TailFile2(File file, Map<String, String> headers, long inode, long pos, long bytes, long records,
                     long readerReadlineBatch, long readerReadBatchSleep, boolean isBackTracking, String logFileType)
            throws IOException {
        if (LogFileType.TEXT.getFileType().equals(logFileType)) {
            this.fileReader = new TextFileReader(file, readerReadlineBatch, readerReadBatchSleep);
        } else if (LogFileType.SEQFILE.getFileType().equals(logFileType)) {
            this.fileReader = new SeqfileFileReader(file);
        } else {
            logger.error("create FileReader fail. ");
        }

        if (pos > 0) {
            logger.info("TailFile2<init> to position");
            position(pos);
        }
        if (bytes > 0) {
            handledBytes(bytes);
        }
        if (records > 0) {
            fileReaderHandledRecords(records);
        }


        this.path = file.getAbsolutePath();
        this.fileName = file.getName();
        this.inode = inode;
        this.pos = pos;
        this.bytes = bytes;
        this.lastUpdated = System.currentTimeMillis();
        this.needTail = true;
        this.headers = headers;

        this.file = file;

        this.isBackTracking = isBackTracking;
        if (isBackTracking) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMddHH");
            DateTime dt = formatter.parseDateTime(fileName.substring(fileName.length() - 10));
            this.timestampInFile = dt.getMillis();
        }

    }

    public FileReader getFileReader() {
        return fileReader;
    }

    public String getPath() {
        return path;
    }

    public long getInode() {
        return inode;
    }

    public long getPos() {
        return pos;
    }

    public long getBytes() {
        return bytes;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void updatePos(long pos) throws IOException {

        // source每次实例化时，ReliableTaildirEventReader也重新实例化，调用 loadPositionFile ，根据checkpoint中的pos 定位
        logger.info(
                "source init, so ReliableTaildirEventReader init, invoke loadPositionFile, " +
                        "so invoke updatePos to position");
        position(pos);
        setPos(pos);
        logger.info("Updated position, file: " + this.path + ", inode: " + inode + ", pos: " + pos);

    }

    public void updateBytes(Long bytes) {
        handledBytes(bytes);
        setBytes(bytes);
    }

    public void updateRecords(Long records) {
        fileReaderHandledRecords(records);
        setRecordNum(records);
    }

    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                  boolean addByteOffset, boolean addInode, boolean addFileName, boolean timestampHeader)
            throws IOException {
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset, addInode, addFileName, timestampHeader);
            if (event == null) {
                break;
            }
            events.add(event);
        }
        return events;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        byte[] record = fileReader.readRecord();
        if (record == null) {
            return null;
        }

        Event event = EventBuilder.withBody(record);
        if (addByteOffset == true) {
            event.getHeaders().put(LINE_OFFSET_HEADER_KEY, this.position() + "");
        }
        return event;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset, boolean addInode,
                            boolean addFileName, boolean timestampHeader) throws IOException {
        Event event = readEvent(backoffWithoutNL, addByteOffset);
        if (null != event && addInode == true) {
            event.getHeaders().put(INODE_HEADER_KEY, inode + "");
        }
        if (null != event && addFileName == true) {
            event.getHeaders().put(FILENAME_HEADER_KEY, fileName);
        }
        if (null != event && timestampHeader == true) {
            event.getHeaders().put(TIMESTAMP_KEY, isBackTracking ? timestampInFile + "" : file.lastModified() + "");
        }
        return event;
    }

    public long position() {
        return this.fileReader.tell();
    }

    public void position(long pos) throws IOException {
        this.fileReader.seek(pos);
    }

    public void handledBytes(long bytes) {
        this.fileReader.setHandledBytes(bytes);
    }

    public long handledBytes() {
        return this.fileReader.getHandledBytes();
    }

    public void close() {
        try {
            fileReader.close();
            fileReader = null;
        } catch (IOException e) {
            logger.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    public static class CompareByLastModifiedTime implements Comparator<File> {
        @Override
        public int compare(File f1, File f2) {
            return Long.valueOf(f1.lastModified()).compareTo(f2.lastModified());
        }
    }

    public long getRecordNum() {
        return recordNum;
    }

    public void setRecordNum(long recordNum) {
        this.recordNum = recordNum;
    }

    public long fileReaderHandledRecords() {
        return this.fileReader.getHandledRecords();
    }

    public void fileReaderHandledRecords(long records) {
        this.fileReader.setHandledRecords(records);
    }
}
