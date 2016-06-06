package org.apache.flume.source.taildirExt.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flume.source.taildirExt.TailFile2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Created by hj on 15/11/26.
 */
public class FileDiff {

    private static final Logger logger = LoggerFactory.getLogger(FileDiff.class);

    public static List<Long> calNeedProcessInodes(JSONArray checkpoint, List<Long> existingInodes,
                                                  Map<Long, TailFile2> tailFiles) throws IOException {

        List<Long> needProcessInodes = Lists.newArrayList();
        List<Long> checkpointInodes = Lists.newArrayList();
        List<Long> newInodes = Lists.newArrayList();

        // long ei: existingInodes 在cp中，并且FileUtil.hasNext为true(旧文件，但有新数据)；
        // 或者ei不在cp中(新文件)；
        // 则 添加到needProcessInodes中

        for (int i = 0; i < checkpoint.size(); i++) {
            JSONObject jObj = checkpoint.getJSONObject(i);
            Long bytes = jObj.getLong("bytes");
            long pos = jObj.getLong("pos");
            long inode = jObj.getLong("inode");
            String file = jObj.getString("file");
            long records = jObj.getLong("records");

            checkpointInodes.add(inode);

            boolean hasNext = (new File(file)).length() > Long.valueOf(bytes + "");
            if (existingInodes.contains(inode) && hasNext) {
                needProcessInodes.add(inode);

                TailFile2 tf = tailFiles.get(inode);
                // 正常不该走到这里: oldFile hasNext, tf.getPos() != tf.position(), 那seek到pos处
                if (tf != null && tf.getFileReader() != null && tf.getPos() != tf.position()) {
                    logger.info(String.format("calNeedProcessInodes, oldFile hasNext: [inode=%d], [file=%s], " +
                            "[pos=%d], [bytes=%d B]", inode, file, pos, bytes));
                    tf.updatePos(pos);
                    tf.updateBytes(bytes);
                    tf.updateRecords(records);
                    tailFiles.put(inode, tf);

                }
            }
        }

        for (long ei : existingInodes) {
            if (!checkpointInodes.contains(ei)) {
                needProcessInodes.add(ei);
                newInodes.add(ei);
            }
        }

        logger.info(String.format(
                "needProcessInodes=%s, newInodes=%s, checkpointInodes=%s",
                needProcessInodes.toString(), newInodes.toString(), checkpointInodes.toString()));
        logger.debug(String.format(
                "checkpoint=%s, existingInodes=%s", checkpoint.toString(), existingInodes.toString()));

        return needProcessInodes;
    }


}
