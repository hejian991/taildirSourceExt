package org.apache.flume.source.taildirExt.util

import java.util.Properties

/**
  * Created by hj on 15/11/26.
  */
object SeparatorUtil {
  val prop = new Properties(System.getProperties)
  val getLineSeparator = prop.getProperty("line.separator")

  val LineSeparatorLength = getLineSeparator.length


}