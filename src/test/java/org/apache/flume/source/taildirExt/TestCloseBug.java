package org.apache.flume.source.taildirExt;

import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.channel.SpillableMemoryChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flume.channel.SpillableMemoryChannel.MEMORY_CAPACITY;
import static org.apache.flume.channel.SpillableMemoryChannel.OVERFLOW_CAPACITY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.*;

/**
 * Created by hj on 15/11/10.
 */
public class TestCloseBug {

    static TaildirSource source;
    static SpillableMemoryChannel channel;
    private File tmpDir;
    private String posFilePath;

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

    public void testCloseBug() throws IOException, InterruptedException {

        //        String file = "/Users/hj/temp/a.2016041817";
        //        String file = "/Users/hj/Downloads/sample_message.pb.log.201605112200";
        String file = "/Users/hj/code/project_baidu/avro-pb-test/php_seqfile_log";

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", file);
        context.put(BATCH_SIZE, "100");
        context.put(LIMIT_RATE_HEADER, "100000");
        context.put(IDLE_TIMEOUT, "5000000");
        context.put(LOG_FILE_TYPE, "seqfile");

        Configurables.configure(source, context);
        source.setName("hj-test-source");

        source.start();

        Thread process = new Thread(new CallProcessRunnable());
        process.setName("process-thread");
        process.start();

        Thread.sleep(3000);

        Thread fetchThread = new Thread(new Runnable2());
        fetchThread.setName("fetch-thread");
        fetchThread.start();

    }

    private class CallProcessRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                source.process();
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    System.out.println("do not need care.");
                }
            }
        }
    }

    private class Runnable2 implements Runnable {
        @Override
        public void run() {
            while (true) {
                fetch();
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    System.out.println("do not need care.");
                }
            }
        }

        public void fetch() {
            Transaction txn = channel.getTransaction();
            txn.begin();
            Event e;
            while ((e = channel.take()) != null) {
                System.out.println(e.toString());

//                Sample.SampleMessage sampleMessage = null;
//                try {
//                    sampleMessage = Sample.SampleMessage.parseFrom(e.getBody());
//                } catch (InvalidProtocolBufferException e1) {
//                    e1.printStackTrace();
//                }
//                System.out.println(sampleMessage);
            }
            txn.commit();
            txn.close();
        }
    }

    public void testIsBackTracking() throws IOException, InterruptedException {

        File f1 = new File(tmpDir, "notice.log.2015110806");
        File f2 = new File(tmpDir, "notice.log.2015110807");
        File f3 = new File(tmpDir, "notice.log.2015110808");

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", tmpDir + "/notice.log.([0-9]+)");
        context.put(BATCH_SIZE, "100");
        context.put(LIMIT_RATE_HEADER, "100000");
        context.put(IDLE_TIMEOUT, "5000000");
        context.put(IS_BACK_TRACKING, "true");
        context.put(TIMESTAMP_HEADER, "true");

        Configurables.configure(source, context);
        source.setName("hj-test-source");

        source.start();

        Thread process = new Thread(new CallProcessRunnable());
        process.setName("process-thread");
        process.start();
        //    Thread.sleep(9000);

        Thread fetchThread = new Thread(new Runnable2());
        fetchThread.setName("fetch-thread");
        fetchThread.start();

    }

    public void testIdleTimeout() throws IOException, InterruptedException {

        String file = "/Users/hj/temp/a.2016041817";

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", file);
        context.put(BATCH_SIZE, "100");
        context.put(IDLE_TIMEOUT, "5000");

        Configurables.configure(source, context);
        source.setName("hj-test-source");

        source.start();

        Thread process = new Thread(new CallProcessRunnable());
        process.setName("process-thread");
        process.start();
        //    Thread.sleep(9000);

        Thread fetchThread = new Thread(new Runnable2());
        fetchThread.setName("fetch-thread");
        fetchThread.start();

    }

    // bug: 文件句柄没有被释放
    // 原因查明：因为 closeIdleFile线程 的delay时间配置错误，导致并没有执行close操作
    // bug: deleted文件句柄没有释放  在cleanDeletedFile 方法中close之.
    public static void main(String[] args) throws IOException, InterruptedException {
        TestCloseBug tb = new TestCloseBug();
        tb.setUp();
        tb.testCloseBug();
        //        tb.testIsBackTracking();
        //        tb.testIdleTimeout();
    }
}
