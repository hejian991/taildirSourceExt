package org.apache.flume.source.taildirExt.util

import org.joda.time.DateTime

object TimeUtils {


  def ms2String(ms: Long) = {
    val dt = new DateTime(ms)
    s"""${dt.toString("yyyy/MM/dd HH:mm:ss")}"""
  }

}
