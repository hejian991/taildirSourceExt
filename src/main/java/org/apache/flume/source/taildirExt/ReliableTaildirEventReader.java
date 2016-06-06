/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.taildirExt;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.source.taildirExt.util.CheckpointUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.stream.JsonReader;

import net.sf.json.JSONObject;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

    private final Table<String, File, Pattern> tailFileTable;
    private final Table<String, String, String> headerTable;

    private TailFile2 currentFile = null;
    private Map<Long, TailFile2> tailFiles = Maps.newConcurrentMap();
    private long updateTime;
    private boolean addByteOffset;
    private boolean addInode;
    private final boolean addFileName;
    private final boolean timestampHeader;
    private final boolean isBackTracking;
    private boolean committed = true;
    private long readerReadlineBatch;
    private Long readerReadBatchSleep;

    private final Long readStartTime;
    private final Long readEndTime;
    private final int fileIdleTimeout;

    private String positionFilePath;

    private final String logFileType;

    /**
     * Create a ReliableTaildirEventReader to watch the given directory.
     */
    private ReliableTaildirEventReader(Map<String, String> filePaths,
                                       Table<String, String, String> headerTable, String positionFilePath,
                                       boolean skipToEnd, boolean addByteOffset, boolean addInode, boolean addFileName,
                                       boolean timestampHeader, Long readerReadlineBatch, Long readerReadBatchSleep,
                                       Long readStartTime, Long readEndTime, int fileIdleTimeout,
                                       boolean isBackTracking, String logFileType)
            throws IOException {
        // Sanity checks
        Preconditions.checkNotNull(filePaths);
        Preconditions.checkNotNull(positionFilePath);

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}",
                    new Object[] {ReliableTaildirEventReader.class.getSimpleName(), filePaths, positionFilePath});
        }

        Table<String, File, Pattern> tailFileTable = HashBasedTable.create();
        for (Entry<String, String> e : filePaths.entrySet()) {
            File f = new File(e.getValue());
            File parentDir = f.getParentFile();
            Preconditions.checkState(parentDir.exists(),
                    "Directory does not exist: " + parentDir.getAbsolutePath());
            Pattern fileNamePattern = Pattern.compile(f.getName());
            tailFileTable.put(e.getKey(), parentDir, fileNamePattern);
        }
        logger.info("tailFileTable: " + tailFileTable.toString());
        logger.info("headerTable: " + headerTable.toString());

        this.tailFileTable = tailFileTable;
        this.headerTable = headerTable;
        this.addByteOffset = addByteOffset;
        this.addInode = addInode;
        this.addFileName = addFileName;
        this.timestampHeader = timestampHeader;
        this.isBackTracking = isBackTracking;
        this.readerReadlineBatch = readerReadlineBatch;
        this.readerReadBatchSleep = readerReadBatchSleep;
        this.readStartTime = readStartTime;
        this.readEndTime = readEndTime;
        this.fileIdleTimeout = fileIdleTimeout;
        this.positionFilePath = positionFilePath;

        this.logFileType = logFileType;

        updateTailFiles(Optional.<JSONObject>absent());

        logger.info("Updating position from position file: " + positionFilePath);
        loadPositionFile(positionFilePath);
    }

    /**
     * Load a position file which has the last read position of each file.
     * If the position file exists, update tailFiles mapping.
     */
    public void loadPositionFile(String filePath) {
        Long inode;
        Long pos;
        Long bytes;
        String path;
        Long records;
        FileReader fr = null;
        JsonReader jr = null;
        try {
            File posFile = new File(filePath);
            if (posFile.length() == 2l) {
                logger.warn("loadPositionFile Now checkpointFile content is [], please check it.");
            }
            fr = new FileReader(posFile);
            jr = new JsonReader(fr);
            jr.beginArray();
            while (jr.hasNext()) {
                inode = null;
                pos = null;
                bytes = null;
                path = null;
                records = 0L;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "pos":
                            pos = jr.nextLong();
                            break;
                        case "bytes":
                            bytes = jr.nextLong();
                            break;
                        case "file":
                            path = jr.nextString();
                            break;
                        case "records":
                            records = jr.nextLong();
                            break;
                        default:
                            logger.error("loadPositionFile error");
                            break;
                    }
                }
                jr.endObject();

                for (Object v : Arrays.asList(inode, pos, path)) {
                    Preconditions.checkNotNull(v, "Detected missing value in position file. "
                            + "inode: " + inode + ", pos: " + pos + ", path: " + path);
                }
                TailFile2 tf = tailFiles.get(inode);

                if (tf != null) {
                    boolean fileHasNext = new File(tf.getPath()).length() > bytes;
                    // fileHasNext 返回true时 才会updatePos追到后 收集数据
                    if (fileHasNext) {
                        logger.info(String.format("fileHasNext: [inode=%d], [path=%s], [pos=%d], [bytes=%d B]", inode,
                                path, pos, bytes));
                        tf.updatePos(pos);
                        tf.updateBytes(bytes);
                        tf.updateRecords(records);
                    } else {
                        logger.info(
                                String.format("fileHasNotNext: [inode=%d], [path=%s], [pos=%d], [bytes=%d B]", inode,
                                        path, pos, bytes));
                        // fileHasNext 返回 false，只需要设置 内存对象tf的 pos和bytes
                        tf.setPos(pos);
                        tf.setBytes(bytes);
                        tf.setRecordNum(records);
                    }

                    tailFiles.put(inode, tf);

                } else {
                    logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
                }
            }
            jr.endArray();
        } catch (FileNotFoundException e) {
            logger.error("loadPositionFile checkpointFile not found: " + filePath
                    + ", not update position. first time is ok, otherwise error");
        } catch (IOException e) {
            logger.error("Failed loading checkpointFile: " + filePath, e);
        } finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (jr != null) {
                    jr.close();
                }
            } catch (IOException e) {
                logger.error("file close Error: " + e.getMessage(), e);
            }
        }
    }

    public Map<Long, TailFile2> getTailFiles() {
        return tailFiles;
    }

    public void setCurrentFile(TailFile2 currentFile) {
        this.currentFile = currentFile;
    }

    @Override
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (events.isEmpty()) {
            return null;
        }
        return events.get(0);
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        return readEvents(numEvents, true);
    }

    @VisibleForTesting
    public List<Event> readEvents(TailFile2 tf, int numEvents) throws IOException {
        setCurrentFile(tf);
        return readEvents(numEvents, true);
    }

    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
            throws IOException {

        List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset, addInode,
                addFileName, timestampHeader);
        if (events.isEmpty()) {
            return events;
        }

        Map<String, String> headers = currentFile.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            for (Event event : events) {
                event.getHeaders().putAll(headers);
            }
        }
        committed = false;
        return events;
    }

    @Override
    public void close() throws IOException {
        for (TailFile2 tf : tailFiles.values()) {
            if (tf.getFileReader() != null) {
                tf.close();
            }
        }
    }

    /**
     * Commit the last lines which were read.
     */
    @Override
    public void commit() throws IOException {
        if (!committed && currentFile != null) {
            currentFile.setPos(currentFile.position());
            currentFile.setBytes(currentFile.handledBytes());
            currentFile.setRecordNum(currentFile.fileReaderHandledRecords());
            currentFile.setLastUpdated(System.currentTimeMillis());
            committed = true;
        }
    }

    /**
     * Update tailFiles mapping if a new file is created or appends are detected
     * to the existing file.
     */
    public List<Long> updateTailFiles(Optional<JSONObject> checkpointMap) {
        List<Long> existingInodes = Lists.newArrayList();

        for (Cell<String, File, Pattern> cell : tailFileTable.cellSet()) {
            Map<String, String> headers = headerTable.row(cell.getRowKey());
            File parentDir = cell.getColumnKey();
            Pattern fileNamePattern = cell.getValue();

            for (File f : getMatchFiles(parentDir, fileNamePattern)) {
                long inode = getInode(f);
                if (-1 == inode) {
                    continue;
                }

                if (existingInodes.contains(inode)) {
                    logger.warn(String.format("inode[%s] repeat, ignore the second.", inode));
                    continue;
                } else {
                    existingInodes.add(inode);
                }

                TailFile2 tf = tailFiles.get(inode);

                if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
                    // reader 创建时，此时 tf == null，设置startPos=0
                    // inode不变，而文件名变化时，设置startPos=tf.getPos()
                    long startPos = tf == null ? 0 : tf.getPos();
                    long startBytes = tf == null ? 0 : tf.getBytes();
                    long startRecords = tf == null ? 0 : tf.getRecordNum();
                    tf = openFile(f, headers, inode, startPos, startBytes, startRecords, readerReadlineBatch,
                            readerReadBatchSleep, isBackTracking, 1);
                } else {
                    // 文件有新内容时
                    boolean hasNext = CheckpointUtil.hasNext(f, checkpointMap.or(new JSONObject()));
                    if (hasNext && tf.getFileReader() == null) {
                        logger.info("closed file {} has new info, so open it.", tf.uuid());
                        tf = openFile(f, headers, inode, tf.getPos(), tf.getBytes(), tf.getRecordNum(),
                                readerReadlineBatch, readerReadBatchSleep, isBackTracking, 2);
                    }

                }
                tailFiles.put(inode, tf);

            }

        }

        // cleanDeletedFile，防止 tailFiles 多线程访问异常(Done)
        cleanDeletedFile(existingInodes);

        return existingInodes;
    }

    private void cleanDeletedFile(List<Long> updatedInodes) {
        Iterator<Entry<Long, TailFile2>> it = tailFiles.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Long, TailFile2> entry = it.next();
            if (!updatedInodes.contains(entry.getKey())) {
                // 1. Close file
                TailFile2 tf = entry.getValue();
                if (tf != null && tf.getFileReader() != null) {
                    tf.close();
                    logger.info(String.format("close deleted file: %s", tf.uuid()));
                }

                // 2. remove
                it.remove();
                logger.info(String.format("remove inode:[%d] from tailFiles, path:[%s]", entry.getKey(),
                        tf.getPath()));
            }
        }
    }

    private List<File> getMatchFiles(File parentDir, final Pattern fileNamePattern) {

        FileFilter filter = new FileFilter() {
            public boolean accept(File f) {
                String fileName = f.getName();
                long fileLastModified = f.lastModified();
                if (f.isDirectory() || !fileNamePattern.matcher(fileName).matches() || !shouldHave(fileLastModified)) {
                    return false;
                }
                return true;
            }
        };
        File[] files = parentDir.listFiles(filter);
        ArrayList<File> result = Lists.newArrayList(files);
        Collections.sort(result, new TailFile2.CompareByLastModifiedTime());
        return result;
    }

    // [readStartTime, readEndTime)
    private boolean shouldHave(long fileLastModified) {
        boolean shouldHave = false;

        if (this.readStartTime == -1 && this.readEndTime == -1) {
            shouldHave = true;
        } else if (this.readStartTime != -1 && this.readEndTime == -1) {
            shouldHave = fileLastModified >= this.readStartTime;
        } else if (this.readStartTime == -1 && this.readEndTime != -1) {
            shouldHave = fileLastModified < this.readEndTime;
        } else {
            shouldHave = fileLastModified >= this.readStartTime && fileLastModified < this.readEndTime;
        }
        return shouldHave;
    }

    public static long getInode(File file) {
        long inode = -1;
        try {
            inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        } catch (IOException e) {
            logger.info("get file {} inode fail. ", file.getAbsolutePath());
        }
        return inode;
    }

    private TailFile2 openFile(File file, Map<String, String> headers, long inode, long pos, long bytes, long records,
                               long readerReadlineBatch,
                               long readerReadBatchSleep, boolean isBackTracking, int openType) {
        try {
            logger.info("openType: " + openType + ", Opening file: " + file + ", inode: " + inode
                    + ", records: " + records);
            return new TailFile2(file, headers, inode, pos, bytes, records, readerReadlineBatch, readerReadBatchSleep,
                    isBackTracking, logFileType);
        } catch (IOException e) {
            throw new FlumeException("Failed opening file: " + file, e);
        }
    }

    /**
     * Special builder class for ReliableTaildirEventReader
     */
    public static class Builder {
        private Map<String, String> filePaths;
        private Table<String, String, String> headerTable;
        private String positionFilePath;
        private boolean skipToEnd;
        private boolean addByteOffset;
        private boolean addInode;
        private boolean addFileName;
        private boolean timestampHeader;
        private Long readerReadlineBatch;
        private Long readerReadBatchSleep;
        private Long readStartTime;
        private Long readEndTime;
        private int fileIdleTimeout;
        private boolean isBackTracking;
        private String logFileType;

        public Builder filePaths(Map<String, String> filePaths) {
            this.filePaths = filePaths;
            return this;
        }

        public Builder headerTable(Table<String, String, String> headerTable) {
            this.headerTable = headerTable;
            return this;
        }

        public Builder positionFilePath(String positionFilePath) {
            this.positionFilePath = positionFilePath;
            return this;
        }

        public Builder skipToEnd(boolean skipToEnd) {
            this.skipToEnd = skipToEnd;
            return this;
        }

        public Builder addByteOffset(boolean addByteOffset) {
            this.addByteOffset = addByteOffset;
            return this;
        }

        public Builder addInode(boolean addInode) {
            this.addInode = addInode;
            return this;
        }

        public Builder addFileName(boolean addFileName) {
            this.addFileName = addFileName;
            return this;
        }

        public Builder addTimestampHeader(boolean timestampHeader) {
            this.timestampHeader = timestampHeader;
            return this;
        }

        public Builder addReaderReadlineBatch(Long readerReadlineBatch) {
            this.readerReadlineBatch = readerReadlineBatch;
            return this;
        }

        public Builder addReaderReadBatchSleep(Long readerReadBatchSleep) {
            this.readerReadBatchSleep = readerReadBatchSleep;
            return this;
        }

        public ReliableTaildirEventReader build() throws IOException {
            return new ReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd, addByteOffset,
                    addInode, addFileName, timestampHeader, readerReadlineBatch, readerReadBatchSleep, readStartTime,
                    readEndTime,
                    fileIdleTimeout,
                    isBackTracking,
                    logFileType);
        }

        public Builder addReadStartTime(Long readStartTime) {
            this.readStartTime = readStartTime;
            return this;
        }

        public Builder addReadEndTime(Long readEndTime) {
            this.readEndTime = readEndTime;
            return this;
        }

        public Builder fileIdleTimeout(int fileIdleTimeout) {
            this.fileIdleTimeout = fileIdleTimeout;
            return this;
        }

        public Builder addIsBackTracking(boolean isBackTracking) {
            this.isBackTracking = isBackTracking;
            return this;
        }

        public Builder setLogFileType(String logFileType) {
            this.logFileType = logFileType;
            return this;
        }
    }

}
