package org.apache.flume.source.taildirExt.seqfile

import java.io.{EOFException, IOException, File}

import org.apache.flume.source.taildirExt.FileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, DataOutputBuffer}
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by hj on 16/5/13.
  */
class SeqfileFileReader(val file: File) extends FileReader {
  private val logger: Logger = LoggerFactory.getLogger(classOf[SeqfileFileReader])

  private val in: SequenceFile.Reader = {
    val conf = new Configuration(false)
    val path = new Path(file.getAbsolutePath)
    val fs = path.getFileSystem(conf)
    new SequenceFile.Reader(conf
      , SequenceFile.Reader.file(path)
      , SequenceFile.Reader.length(Long.MaxValue)
    )

  }

  private val buffer: DataOutputBuffer = new DataOutputBuffer
  private val vbytes: SequenceFile.ValueBytes = in.createValueBytes

  private var handledRecords: Long = 0L


  // read seqFile,
  // hasNext && key==1 (force transfer)
  @throws(classOf[IOException])
  override def readRecord: Array[Byte] = {
    val key = createKey
    val value = createValue

    try {
      if (next(key, value)) {
        handledRecords += 1
        value.copyBytes()
      }
      else null
    } catch {
      case eofException: EOFException =>
        null
    }
  }

  /**
    * Read raw bytes from a SequenceFile.
    */
  @throws(classOf[IOException])
  @throws(classOf[EOFException])
  def next(key: BytesWritable, value: BytesWritable): Boolean = {
    val eof: Boolean = (-1 == in.nextRawKey(buffer))
    val hasNext = !eof
    if (hasNext) {
      key.set(buffer.getData, 0, buffer.getLength)
      buffer.reset
      in.nextRawValue(vbytes)
      vbytes.writeUncompressedBytes(buffer)
      value.set(buffer.getData, 0, buffer.getLength)
      buffer.reset
    }

    hasNext
  }

  @throws(classOf[IOException])
  override def tell: Long = {
    in.getPosition
  }

  override def seek(position: Long): Unit = {
    in.seek(position)
  }

  override def getHandledBytes: Long = {
    in.getPosition
  }

  override def setHandledBytes(bytes: Long): Unit = {
    // don't need do anything, seek(position: Long) do.
  }

  @throws(classOf[IOException])
  def close {
    in.close
  }

  def createKey: BytesWritable = {
    return new BytesWritable
  }

  def createValue: BytesWritable = {
    return new BytesWritable
  }


  override def setHandledRecords(records: Long): Unit = {
    this.handledRecords = records
  }

  override def getHandledRecords: Long = {
    handledRecords
  }

}

object SeqfileFileReader
