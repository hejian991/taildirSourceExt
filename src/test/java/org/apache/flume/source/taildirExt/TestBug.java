package org.apache.flume.source.taildirExt;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.channel.SpillableMemoryChannel.MEMORY_CAPACITY;
import static org.apache.flume.channel.SpillableMemoryChannel.OVERFLOW_CAPACITY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.*;

/**
 * Created by hj on 15/11/10.
 */
public class TestBug {

    static TaildirSource source;
    static SpillableMemoryChannel channel;
    private File tmpDir;
    private String posFilePath;
    private ScheduledExecutorService positionWriter;

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

    // bug2: updateTailFiles 是否会发生 tf.getLastUpdated==f.lastModified
    public void testBug() throws IOException, InterruptedException {

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

        positionWriter = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
        positionWriter.scheduleWithFixedDelay(new Runnable3(),
                0, 1000, TimeUnit.MILLISECONDS);

    }

    private class CallProcessRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                source.process();
            }
        }
    }

    private class Runnable3 implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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

    // bug2: updateTailFiles 是否会发生 tf.getLastUpdated==f.lastModified
    public static void main(String[] args) throws IOException, InterruptedException {
        TestBug tb = new TestBug();
        tb.setUp();
        tb.testBug();
    }
}
