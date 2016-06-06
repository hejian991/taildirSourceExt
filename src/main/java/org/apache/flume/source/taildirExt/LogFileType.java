package org.apache.flume.source.taildirExt;

/**
 * Created by hj on 16/5/13.
 */
public enum LogFileType {
    TEXT("text"),
    SEQFILE("seqfile"),
    OTHER(null);

    private final String fileType;

    LogFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFileType() {
        return fileType;
    }
}


