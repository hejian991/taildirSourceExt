package org.apache.flume.source.taildirExt

import java.io.IOException

/**
  * Created by hj on 16/5/13.
  */
/** An InputStream that supports seek and tell. */
trait SeekableInput {
  /** Set the position for the next {@link java.io.InputStream#read(byte[],int,int) read()}. */
  @throws(classOf[IOException])
  def seek(position: Long)

  /** Return the position of the next {@link java.io.InputStream#read(byte[],int,int) read()}. */
  def tell: Long


}
