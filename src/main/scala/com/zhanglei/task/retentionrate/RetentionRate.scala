package com.zhanglei.task.retentionrate

import com.zhanglei.constants.LogConstants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/8.
  */
object RetentionRate {
//  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
//  //调整sparksql在进行shuffle时候的任务并行度
//  sparkConf.set("spark.sql.shuffle.partitions","100")
//  //配置job对象
//  val jobConf = new JobConf(new Configuration())
//  jobConf.set(TableInputFormat.INPUT_TABLE,LogConstants.LOG_HBASE_TABLE)
//  //创建sparkSession对象
//  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
//  val deAccTimeRdd = sparkSession.sparkContext.newAPIHadoopRDD(jobConf,classOf[TableOutputFormat],classOf[ImmutableBytesWritable],
//    classOf[Result]).map(_._2).map(result => {
//    //设备ID
//    val deviceId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes()))
//    //时间
//    val accessTime = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes())).toLong
//    (deviceId,accessTime)
//  })
}
