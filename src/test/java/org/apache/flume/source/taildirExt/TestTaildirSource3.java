
package org.apache.flume.source.taildirExt;

import com.google.common.base.Charsets;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.channel.SpillableMemoryChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flume.channel.SpillableMemoryChannel.MEMORY_CAPACITY;
import static org.apache.flume.channel.SpillableMemoryChannel.OVERFLOW_CAPACITY;
import static org.apache.flume.source.taildirExt.TaildirSourceConfigurationConstants.*;

public class TestTaildirSource3 {
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


    // feature test:
    //    1 starttime= -1 时， 时间为当前时间-1;
    //    2 checkpoint文件，新增 bytes，用-1 过渡；
    //    期望效果：更新版本前 checkpoint中的所有文件都不会被处理，刷新cp信息时仍会写bytes=-1，因此 reloadConf 也不会处理到这些文件。

    // 测试case：
    // 1 保留了 taildir_position_test.json.old.bak, 这个cp文件中没有bytes字段信息，
    //   [{"inode":35266138,"pos":0,"file":"/Users/hj/temp/test-log"}]
    //   改名，启动该unittest，会loadPositionFile；
    //   达到期望效果：[{"inode":35266138,"pos":0,"bytes":-1,"file":"/Users/hj/temp/test-log"}]

    // 2 配置变化，模拟触发 reloadConf，EventReader实例化会loadPositionFile，bytes=-1
    //   期望效果: 不处理bytes=-1的文件，刷写cp信息时仍会写bytes=-1
    //   达到效果：[{"inode":36960336,"pos":4,"bytes":-1,"file":"/Users/hj/temp/test-log2"}]

    // 3 starttime放开1小时的限制

    @Test
    public void testBugWriteToPositionFile() throws IOException, InterruptedException {

        String file = "/Users/hj/temp/test-log(.*)";

        Context context = new Context();
        context.put(POSITION_FILE, posFilePath);
        context.put(FILE_GROUPS, "f1");
        context.put(FILE_GROUPS_PREFIX + "f1", file);
        context.put(BATCH_SIZE, "100");
        context.put(LIMIT_RATE_HEADER, "100000");

        Configurables.configure(source, context);

        source.start();

        Thread channelConsumerThread = new Thread(new channelConsumerRunnable());
        channelConsumerThread.start();

        Thread sourceProcessThread = new Thread(new sourceProcessRunnable());
        sourceProcessThread.start();
        //    Thread.sleep(9000);


        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
        }

//        sourceProcessThread.stop();
//        channelConsumerThread.stop();

//        Configurables.configure(source, context);
//        source.start();
//        source.process();

//        source.stop();

        while (true) {
            Thread.sleep(1000);
        }

    }

    private class sourceProcessRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                source.process();
            }
        }
    }

    private class channelConsumerRunnable implements Runnable {
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
