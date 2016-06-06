package org.apache.flume.source.taildirExt

import java.io.IOException

/**
  * Created by hj on 16/5/13.
  */
trait FileReader extends SeekableInput {

  @throws(classOf[IOException])
  def readRecord: Array[Byte]

  @throws(classOf[IOException])
  def close


  def setHandledBytes(bytes: Long)

  def getHandledBytes: Long

  def setHandledRecords(records: Long)

  def getHandledRecords: Long

}
