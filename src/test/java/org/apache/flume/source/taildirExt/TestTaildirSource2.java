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

import static org.apache.flume.channel.SpillableMemoryChannel.MEMORY_CAPACITY;
import static org.apache.flume.channel.SpillableMemoryChannel.OVERFLOW_CAPACITY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.BATCH_SIZE;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.FILE_GROUPS;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.FILE_GROUPS_PREFIX;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.LIMIT_RATE_HEADER;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.POSITION_FILE;
import static org.apache.flume.source.taildirExt.util.TimeUtil.currentTimeMs;
import static org.apache.flume.source.taildirExt.util.TimeUtil.recordTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.channel.SpillableMemoryChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestTaildirSource2 {
    static TaildirSource source;
    static SpillableMemoryChannel channel;
    private File tmpDir;
    private String posFilePath;
    private ScheduledExecutorService positionWriter;

    @Before
    public void setUp() {
        source = new TaildirSource();
        channel = new SpillableMemoryChannel();

        Context context = new Context();
        context.put(MEMORY_CAPACITY, "1000000");
        context.put(OVERFLOW_CAPACITY, "100000000");
        context.put(FileChannelConfiguration.CHECKPOINT_DIR, "/data/web/storage/c1");
        context.put(FileChannelConfiguration.DATA_DIRS, "/data/web/storage/d1");
        Configurables.configure(channel, context);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        //    tmpDir = Files.createTempDir();
        tmpDir = new File("/Users/hj/temp");
        posFilePath = tmpDir.getAbsolutePath() + "/taildir_position_test.json";
    }

    //  @After
    //  public void tearDown() {
    //    for (File f : tmpDir.listFiles()) {
    //      f.delete();
    //    }
    //    tmpDir.delete();
    //  }

    // 测试 source的reader 处理完一个 大文件需要的时间
    @Test
    public void testSourceHandleTime() throws IOException {
        String file = "/Users/hj/temp/b.1m";

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", file);
        context.put(BATCH_SIZE, "1000");
        context.put(LIMIT_RATE_HEADER, "100000");

        Configurables.configure(source, context);

        long timeStart = currentTimeMs();

        source.start();
        source.process();
        System.out.println(recordTime("source把20M数据全部成功推到SpillableMemoryChannel耗时：", timeStart));
        // [Oper]:[source把20M数据全部成功推到SpillableMemoryChannel耗时：], [2003 ms]
        // [Oper]:[source把20M数据全部成功推到SpillableMemoryChannel耗时：], [2288 ms]
        source.stop();

        Transaction txn = channel.getTransaction();
        txn.begin();
        List<Event> out = Lists.newArrayList();
        Event e;
        while ((e = channel.take()) != null) {

            out.add(e);
        }
        txn.commit();
        txn.close();

        System.out.println("out.size: " + out.size());
        System.out.println(recordTime("从SpillableMemoryChannel 取出20M数据并加载到内存中耗时：", timeStart));
        // [Oper]:[从SpillableMemoryChannel 取出20M数据并加载到内存中耗时：], [5142 ms]
        //        assertEquals(4, out.size());

    }

    // ConfigureTwice
    // 复现bug：配置变化，而startTime 不改变
    @Test
    public void testConfigureTwice() throws IOException, InterruptedException {
        File f1 = new File(tmpDir, "notice.log.2015110806");
        File f2 = new File(tmpDir, "notice.log.2015110807");
        File f3 = new File(tmpDir, "notice.log.2015110808");

        Files.write("file1line1\nfile1line2\nfile1line3\n", f1, Charsets.UTF_8);
        Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
        Files.write("file3line1\n", f3, Charsets.UTF_8);

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", tmpDir + "/notice.log.([0-9]+)");
        context.put(BATCH_SIZE, "1000");
        context.put(LIMIT_RATE_HEADER, "100000");

        Configurables.configure(source, context);

        source.start();
        source.process();

        context.put(BATCH_SIZE, "500");

        Configurables.configure(source, context);

    }

    // bug1： 因为有两个 inode相同的文件，本质  因为inode相同而文件的path不同，导致 读文件流切换并从0行开始读
    // 复现bug 追上文件后，又重新开始读文件，并刷写到checkpoint文件中
    // log文件产生慢点

    // bug2: updateTailFiles 是否会发生 tf.getLastUpdated==f.lastModified  换到 TestBug里去测了，因为在unittest中线程池不阻塞
    @Test
    public void testBugWriteToPositionFile() throws IOException, InterruptedException {

        String file = "/Users/hj/temp/test-log";

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", file);
        context.put(BATCH_SIZE, "100");
        context.put(LIMIT_RATE_HEADER, "100000");

        Configurables.configure(source, context);

        source.start();

        new Thread(new CallProcessRunnable()).start();
        //    Thread.sleep(9000);

        new Thread(new Runnable2()).start();

        while (true) {
            Thread.sleep(1000);
        }

    }

    private class CallProcessRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                source.process();
            }
        }
    }

    private class Runnable2 implements Runnable {
        @Override
        public void run() {
            while (true) {
                fetch();
            }
        }

        public void fetch() {
            Transaction txn = channel.getTransaction();
            txn.begin();
            Event e;
            while ((e = channel.take()) != null) {
                String body = new String(e.getBody(), Charsets.UTF_8);
                System.out.println(body);
            }
            txn.commit();
            txn.close();
        }
    }

}
