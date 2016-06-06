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

import java.util.regex.Pattern;

import org.joda.time.DateTime;

public class TaildirSourceConfigurationConstants {
    /**
     * Mapping for tailing file groups.
     */
    public static final String FILE_GROUPS = "filegroups";
    public static final String FILE_GROUPS_PREFIX = FILE_GROUPS + ".";

    /**
     * Mapping for putting headers to events grouped by file groups.
     */
    public static final String HEADERS_PREFIX = "headers.";

    /**
     * Path of position file.
     */
    public static final String POSITION_FILE = "positionFile";
    public static final String DEFAULT_POSITION_FILE = "/.flume/taildir_position.json";

    /**
     * What size to batch with before sending to ChannelProcessor.
     */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Whether to skip the position to EOF in the case of files not written on the position file.
     */
    public static final String SKIP_TO_END = "skipToEnd";
    public static final boolean DEFAULT_SKIP_TO_END = false;

    /**
     * Time (ms) to close idle files.
     * 改为1小时 idle 才会关闭文件，防止 bufferedReader经常关闭再打开后需要重新扫盘定位
     */
    public static final String IDLE_TIMEOUT = "idleTimeout";
    public static final int DEFAULT_IDLE_TIMEOUT = 1 * 3600 * 1000;

    /**
     * Interval time (ms) to write the last position of each file on the position file.
     */
    public static final String WRITE_POS_INTERVAL = "writePosInterval";
    public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;

    /**
     * Whether to add the line offset (lineNo) of a tailed line to the header
     */
    public static final String LINE_OFFSET_HEADER = "lineOffsetHeader";
    public static final String LINE_OFFSET_HEADER_KEY = "lineoffset";
    public static final boolean DEFAULT_LINE_OFFSET_HEADER = false;

    /**
     * public static final
     */
    public static final Pattern EMPTY_LINE_PATTERN = Pattern.compile("\\s+");

    public static final String INODE_HEADER = "inodeHeader";
    public static final String INODE_HEADER_KEY = "inode";
    public static final boolean DEFAULT_INODE_HEADER = false;

    public static final String FILENAME_HEADER = "fileNameHeader";
    public static final String FILENAME_HEADER_KEY = "filename";
    public static final boolean DEFAULT_FILENAME_HEADER = false;

    public static final String LIMIT_RATE_HEADER = "limitRate";
    public static long DEFAULT_RATE = 1024L;

    public static final String TIMESTAMP_HEADER = "timestampHeader";
    public static final String TIMESTAMP_KEY = "timestamp";
    public static final boolean DEFAULT_TIMESTAMP_HEADER = false;

    public static final String READER_READLINE_BATCH = "readerReadlineBatch";
    public static final long DEFAULT_READER_READLINE_BATCH = 1000;

    public static final String READER_READ_BATCH_SLEEP = "readerReadBatchSleep";
    public static final long DEFAULT_READER_READ_BATCH_SLEEP = 100;

    public static final String START_TIME = "startTime";

    public static long defaultStartTime() {

        DateTime now = new DateTime();
        //    DateTime nowToHour = new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(),
        // now.getHourOfDay(), 0);
        return now.minusMinutes(60).getMillis();
    }

    public static final long DEFAULT_START_TIME = -1;

    public static final String END_TIME = "endTime";
    public static final long DEFAULT_END_TIME = -1;


    /**
     * 是否 是回溯采集任务.
     */
    public static final String IS_BACK_TRACKING = "isBackTracking";
    public static final boolean DEFAULT_IS_BACK_TRACKING = false;


    /**
     * Path of position file.
     */
    public static final String LOG_FILE_TYPE = "logFileType";
    public static final String DEFAULT_LOG_FILE_TYPE = "text";

}
