package com.vc.config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {
  lazy val conf: Config = ConfigFactory.load
  lazy val getMaster: String = conf.getString("app.master")
  lazy val getAppName: String = conf.getString("app.name")
  lazy val getInputHDFSFile: String = conf.getString("hdfs.inputFilePath")
  lazy val getOutputHDFSFile: String = conf.getString("hdfs.outputFilePath")
}
