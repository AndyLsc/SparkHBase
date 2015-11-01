package com.thomsonreuters.timeseries

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable


object SparkHBase {
  def main(args: Array[String]) {
    PropertyConfigurator.configure("log4j.properties")

    if(args.length != 3) {
      println("Usage: SparkHbase <HBase Resource list> <source table> <dest table>")
      System.exit(0)
    }
        
    println("Creating input hbase configuration")
    val conf = HBaseConfiguration.create()
    args(0).split(";").foreach { x => conf.addResource(new Path(x)) }
    
    conf.set(TableInputFormat.INPUT_TABLE, args(1))
    val job = Job.getInstance(conf)   

    println("Create spark conf")
    val sparkConf = new SparkConf()
    .setAppName("SparkHBase")
    val sc = new SparkContext(sparkConf)
    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    
    val cnt = hBaseRDD.count()
    println("Record count: " + cnt)
    
    println("Converting result RDD to cell RDD...")
    val outRDD = hBaseRDD.flatMapValues(result => result.rawCells()).mapValues(cell => new Put(cell.getRow).add(cell.getFamily, cell.getQualifier, cell.getTimestamp, cell.getValue))

    println("Writing to target hbase table")
    val outjob = new JobConf(conf, this.getClass)
    outjob.setOutputFormat(classOf[TableOutputFormat])
    outjob.set(TableOutputFormat.OUTPUT_TABLE, args(2))
    outjob.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    outjob.setMapOutputValueClass(classOf[Put])    
    outRDD.saveAsHadoopDataset(outjob)
    
  }
 
}