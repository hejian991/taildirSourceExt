/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildirExt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.interceptor.LimitInterceptor;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.apache.flume.source.PollableSourceExt;
import org.apache.flume.source.taildirExt.util.CheckpointUtil;
import org.apache.flume.source.taildirExt.util.FileDiff;
import org.apache.flume.source.taildirExt.util.SeparatorUtil;
import org.apache.flume.source.taildirExt.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.*;

public class TaildirSource extends AbstractSource implements
    PollableSourceExt, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private int batchSize;
    private String positionFilePath;
    private Object positionFileLock = new Object();
    private boolean skipToEnd;
    private boolean byteOffsetHeader;
    private boolean inodeHeader;
    private boolean fileNameHeader;

    private SourceCounter sourceCounter;
    private ReliableTaildirEventReader reader;
    private ScheduledExecutorService idleFileChecker;
    private ScheduledExecutorService positionWriter;
    private int retryInterval = 1000;
    private int maxRetryInterval = 5000;
    private int idleFileCheckInitDelay = 5000;
    private int idleTimeout;
    private int checkIdleInterval = 5000;
    private int writePosInitDelay = 5000;
    private int writePosInterval;

    // 必须用list，因为要保证 文件消费顺序
    // 匹配到的 文件：existingInodes
    // CopyOnWriteArrayList 保证了多线程访问安全
    private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
    private List<Long> idleInodes = new CopyOnWriteArrayList();

    // 匹配到的全部文件 - 正常收集完成的文件 = 未收集完的文件
    private List<Long> needProcessInodes = new ArrayList<Long>();

    private Long backoffSleepIncrement;
    private Long maxBackOffSleepInterval;

    private long limitRate = 500l;
    private LimitInterceptor limitInt;

    private boolean timestampHeader;
    private Long readerReadlineBatch;
    private Long readerReadBatchSleep;
    private Long startTime;
    private Long endTime;
    private JSONArray lastCheckpoint = new JSONArray();
    private boolean isBackTracking;
    private String logFileType;

    private AtomicBoolean shouldStop = new AtomicBoolean(false);

    @Override
    public synchronized void start() {
        logger.info("TaildirSource source name={} starting with directory: {}", getName(), filePaths);
        try {
            long t = TimeUtil.currentTimeMs();
            reader = new ReliableTaildirEventReader.Builder()
                    .filePaths(filePaths)
                    .headerTable(headerTable)
                    .positionFilePath(positionFilePath)
                    .skipToEnd(skipToEnd)
                    .addByteOffset(byteOffsetHeader)
                    .addInode(inodeHeader)
                    .addFileName(fileNameHeader)
                    .addTimestampHeader(timestampHeader)
                    .addIsBackTracking(isBackTracking)
                    .addReaderReadlineBatch(readerReadlineBatch)
                    .addReaderReadBatchSleep(readerReadBatchSleep)
                    .addReadStartTime(startTime)
                    .addReadEndTime(endTime)
                    .fileIdleTimeout(idleTimeout)
                    .setLogFileType(logFileType)
                    .build();
            logger.info(TimeUtil.recordTime("ReliableTaildirEventReader build time", t));

        } catch (IOException e) {
            throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
        }

        String idleFileCheckerName = "idleFileChecker-" + getClass().getSimpleName() + "-" + this.getName();
        idleFileChecker = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(idleFileCheckerName).build());
        idleFileChecker.scheduleWithFixedDelay(new IdleFileCheckerRunnable(),
                idleFileCheckInitDelay, checkIdleInterval, TimeUnit.MILLISECONDS);

        String positionWriterName = "positionWriter-" + getClass().getSimpleName() + "-" + this.getName();
        positionWriter = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(positionWriterName).build());
        positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
                writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

        super.start();
        logger.debug("TaildirSource started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        try {
            super.stop();
            closeIdleFiles();
            ExecutorService[] services = {idleFileChecker, positionWriter};
            for (ExecutorService service : services) {
                service.shutdown();
                if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            }
            // write the last position
            writePosition();
            reader.close();
        } catch (InterruptedException e) {
            logger.info("Interrupted while awaiting termination", e);
        } catch (IOException e) {
            logger.info("Failed: " + e.getMessage(), e);
        }
        sourceCounter.stop();
        logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    public String toString() {
        return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
                        + "byteOffsetHeader: %s, inodeHeader: %s, fileNameHeader: %s, idleTimeout: %s, "
                        + "writePosInterval: %s }",
                positionFilePath, skipToEnd, byteOffsetHeader, inodeHeader, fileNameHeader, idleTimeout,
                writePosInterval);
    }

    @Override
    public synchronized void configure(Context context) {
        String fileGroups = context.getString(FILE_GROUPS);
        Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

        filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX), fileGroups.split("\\s+"));
        Preconditions.checkState(!filePaths.isEmpty(),
                "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

        String homePath = System.getProperty("user.home").replace('\\', '/');
        positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
        headerTable = getTable(context, HEADERS_PREFIX);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
        byteOffsetHeader = context.getBoolean(LINE_OFFSET_HEADER, DEFAULT_LINE_OFFSET_HEADER);
        inodeHeader = context.getBoolean(INODE_HEADER, DEFAULT_INODE_HEADER);
        fileNameHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILENAME_HEADER);
        idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
        writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);

        backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
                PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
                PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);

        limitRate = context.getLong(LIMIT_RATE_HEADER, DEFAULT_RATE);
        limitInt = new LimitInterceptor(this.limitRate, 0l);

        timestampHeader = context.getBoolean(TIMESTAMP_HEADER, DEFAULT_TIMESTAMP_HEADER);
        isBackTracking = context.getBoolean(IS_BACK_TRACKING, DEFAULT_IS_BACK_TRACKING);

        // bufferedReader params
        readerReadlineBatch = context.getLong(READER_READLINE_BATCH, DEFAULT_READER_READLINE_BATCH);
        readerReadBatchSleep = context.getLong(READER_READ_BATCH_SLEEP, DEFAULT_READER_READ_BATCH_SLEEP);

        // timeRange params
        startTime = context.getLong(START_TIME, DEFAULT_START_TIME);
        endTime = context.getLong(END_TIME, DEFAULT_END_TIME);

        logFileType = context.getString(LOG_FILE_TYPE, DEFAULT_LOG_FILE_TYPE).toLowerCase();

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
        Map<String, String> result = Maps.newHashMap();
        for (String key : keys) {
            if (map.containsKey(key)) {
                result.put(key, map.get(key));
            }
        }
        return result;
    }

    private Table<String, String, String> getTable(Context context, String prefix) {
        Table<String, String, String> table = HashBasedTable.create();
        for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
            String[] parts = e.getKey().split("\\.", 2);
            table.put(parts[0], parts[1], e.getValue());
        }
        return table;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    @Override
    public Status process() {
        Status status = Status.READY;
        try {

            JSONArray newCheckpoint = new JSONArray();

            synchronized (positionFileLock) {
                try {
                    newCheckpoint =
                            JSONArray.fromObject(Files.readFirstLine(new File(positionFilePath), Charsets.UTF_8));
                } catch (FileNotFoundException e) {
                    logger.warn("process checkpointFile not found: " + positionFilePath
                            + ", will skip process once. first time is ok, otherwise error");
                }
                // 因为clear和addAll不是原子操作，因此可能写入checkpoint的是 [] 或 完整的checkpoint信息
                // 加 lock后，期望写入checkpoint 的总是 完整的checkpoint信息
                JSONObject checkpointMap = CheckpointUtil.transferToMap(newCheckpoint);
                List<Long> newExistingInodes = reader.updateTailFiles(Optional.of(checkpointMap));
                if (newExistingInodes.size() > 0) {
                    existingInodes.clear();
                    existingInodes.addAll(newExistingInodes);
                }
            }

            if (newCheckpoint.size() > 0) {
                lastCheckpoint = newCheckpoint;
            }
            boolean flag = lastCheckpoint.size() == 0 || newCheckpoint.size() > 0;

            if (flag) {
                needProcessInodes = FileDiff.calNeedProcessInodes(newCheckpoint, existingInodes, reader.getTailFiles());
                for (long inode : needProcessInodes) {
                    TailFile2 tf = reader.getTailFiles().get(inode);
                    if (tf.getFileReader() != null) {
                        tailFileHandle(tf, true);
                    } else {
                        logger.warn("needTailFile: {} is closed, cannot tail; next time to tail", tf.toString());
                    }
                }

            } else {
                logger.warn("lastCheckpoint has content, but newCheckpoint is empty, will skip process once");
            }

            // closeIdleFiles意义：只是释放 文件句柄，并不删除tailFiles里的kv对。
            closeIdleFiles();
            TimeUnit.MILLISECONDS.sleep(retryInterval);
        } catch (InterruptedException e) {
            logger.info("TaildirSource process interrupted. Exiting");
        } catch (Throwable t) {
            logger.error("Unable to tail files", t);
            t.printStackTrace();
            status = Status.BACKOFF;
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return backoffSleepIncrement;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return maxBackOffSleepInterval;
    }

    private void tailFileHandle(TailFile2 tf, boolean backoffWithoutNL)
            throws IOException, InterruptedException {
        List<Event> events = Lists.newArrayList();
        logger.info("start handle, set currentFile " + tf.uuid());
        reader.setCurrentFile(tf);
        while (!shouldStop.get()) {
            if (events.isEmpty()) {
                events = reader.readEvents(batchSize, backoffWithoutNL);
            }
            logger.info(String.format("handling, tf=%s", tf.toString()));
            limitInt.intercept(events);

            if (events.isEmpty()) {
                logger.info("end handle, currentFile " + tf.uuid());
                break;
            }
            sourceCounter.addToEventReceivedCount(events.size());
            sourceCounter.incrementAppendBatchReceivedCount();
            try {
                getChannelProcessor().processEventBatch(events);
                reader.commit();
            } catch (ChannelException ex) {
                logger.warn("The channel is full or unexpected failure. "
                        + "The source will try again after " + retryInterval + " ms");
                //                Thread.currentThread().interrupt();
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                retryInterval = retryInterval << 1;
                retryInterval = Math.min(retryInterval, maxRetryInterval);
                continue;
            }
            retryInterval = 1000;
//            sourceCounter.addToByteAcceptedCount(calEventsBytes(events));
            sourceCounter.addToEventAcceptedCount(events.size());
            sourceCounter.incrementAppendBatchAcceptedCount();

            events.clear();
        }

    }

    private long calEventsBytes(List<Event> events) {
        long result = 0l;
        for (Event event : events) {
            result += event.getBody().length + SeparatorUtil.LineSeparatorLength();
        }
        return result;
    }

    private void closeIdleFiles() throws IOException, InterruptedException {
        for (long inode : idleInodes) {
            TailFile2 tf = reader.getTailFiles().get(inode);
            if (tf != null && tf.getFileReader() != null) { // when file has not closed yet
                tf.close();
                logger.info("close idle file: {}", tf.toString());
            }
        }
        idleInodes.clear();
    }

    /**
     * Runnable class that checks whether there are files which should be closed.
     */
    private class IdleFileCheckerRunnable implements Runnable {
        @Override
        public void run() {
            try {
                logger.debug("IdleFileCheckerThread Running...");
                long now = System.currentTimeMillis();
                for (TailFile2 tf : reader.getTailFiles().values()) {
                    if (tf.getLastUpdated() + idleTimeout <= now && tf.getFileReader() != null) {
                        if (!idleInodes.contains(tf.getInode())) {
                            idleInodes.add(tf.getInode());
                            logger.info("add file {} to idleInodes.", tf.toString());
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("Uncaught exception in IdleFileChecker thread", t);
            }
        }
    }

    /**
     * Runnable class that writes a position file which has the last read position
     * of each file.
     */
    private class PositionWriterRunnable implements Runnable {
        @Override
        public void run() {
            writePosition();
        }
    }

    public void writePosition() {
        synchronized (positionFileLock) {

            File file = new File(positionFilePath);
            FileWriter writer = null;
            try {
                writer = new FileWriter(file);
                if (!existingInodes.isEmpty()) {
                    String json = toPosInfoJson();
                    logger.info("do write checkpoint.");
                    writer.write(json);
                    logger.debug("writePosition: " + json);

                } else {
                    logger.warn("Now existingInodes is empty, so write to checkpoint=[]");
                    writer.write("[]");
                }
            } catch (Throwable t) {
                logger.error("Failed writing positionFile", t);
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException e) {
                    logger.error("Error: " + e.getMessage(), e);
                }
            }
        }
    }

    // 每次把 existingInodes 存在的文件 的进度 覆盖写到 posFile中，这种做法不合理
    // 应该 记录 所有文件的进度。
    // 进一步查看发现，existingInodes.addAll(reader.updateTailFiles());就是 存在的所有文件
    // 因此这个做法没问题。
    private String toPosInfoJson() {
        @SuppressWarnings("rawtypes")
        List<Map> posInfos = Lists.newArrayList();
        for (Long inode : existingInodes) {
            TailFile2 tf = reader.getTailFiles().get(inode);
            if (tf != null) {
                posInfos.add(ImmutableMap.of(
                        "inode", inode,
                        "pos", tf.getPos(),
                        "bytes", tf.getBytes(),
                        "file", tf.getPath(),
                        "records", tf.getRecordNum())
                );
            }

        }
        return new Gson().toJson(posInfos);
    }

    @Override
    public void setShouldStop(boolean shouldStop) {
        this.shouldStop.set(shouldStop);
    }
}
