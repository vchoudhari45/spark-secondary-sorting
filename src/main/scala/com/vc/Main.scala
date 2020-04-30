package com.vc

import com.vc.config.ConfigLoader
import com.vc.model.FailSafeUserActivity._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.lead

object Main {

  def main(args: Array[String]): Unit = {
    /**
     *  For running this job on Spark Cluster with HDFS
     *  1. change app.master config in application.conf to yarn
     *  2. change hdfs.filePath config in application.conf to file in HDFS
     *  3. export HADOOP_CONF_DIR=/etc/hadoop/conf which should have core-site.xml, yarn-site.xml, hdfs-site.xml before submitting spark job
     **/
    val spark = SparkSession.builder().master(ConfigLoader.getMaster).appName(ConfigLoader.getAppName).getOrCreate()

    val userActivityDF = spark.read.csv(ConfigLoader.getInputHDFSFile)

    val failSafeUserActivityDS = userActivityDF
      .map(csvToUserActivity) //failSafe parsing
      .flatMap(_.row)

    val partitionedDF = failSafeUserActivityDS
      .withColumn("timeSpend", lead("visitedTimestamp", 1, 0)
        .over(partitionBy("userId").orderBy("visitedTimestamp"))
      ).map(userActivityToCsv)

    //partitionedDF.show(false)
    partitionedDF.write.mode("overwrite").text(ConfigLoader.getOutputHDFSFile)
  }

}
