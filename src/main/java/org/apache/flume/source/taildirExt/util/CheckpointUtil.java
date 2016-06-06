package org.apache.flume.source.taildirExt.util;

import java.io.File;
import java.util.List;

import org.apache.flume.source.taildirExt.ReliableTaildirEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Created by hj on 15/11/26.
 */
public class CheckpointUtil {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointUtil.class);

    public static boolean hasNext(File f, JSONObject checkpointMap) {
        boolean hasNext = false;

        String targetInode = ReliableTaildirEventReader.getInode(f) + "";
        if (checkpointMap.containsKey(targetInode)) {
            Long bytes = checkpointMap.getJSONObject(targetInode).getLong("bytes");
            hasNext = f.length() > bytes;
        }
        if (checkpointMap.keySet().size() == 0) {
            hasNext = true;
        }

        return hasNext;
    }

    public static JSONObject transferToMap(JSONArray checkpoint) {
        JSONObject result = new JSONObject();

        for (int i = 0; i < checkpoint.size(); i++) {
            JSONObject jObj = checkpoint.getJSONObject(i);
            long inode = jObj.getLong("inode");
            result.put(inode, jObj);
        }

        return result;
    }

    public static List<Long> getCheckpointInodes(JSONArray checkpoint) {
        List<Long> checkpointInodes = Lists.newArrayList();

        for (int i = 0; i < checkpoint.size(); i++) {
            JSONObject jObj = checkpoint.getJSONObject(i);
            checkpointInodes.add(jObj.getLong("inode"));
        }

        return checkpointInodes;
    }

}
