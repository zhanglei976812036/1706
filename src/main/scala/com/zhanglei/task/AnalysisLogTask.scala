package com.zhanglei.task

import java.io.IOException
import com.zhanglei.caseclass.IPRule
import com.zhanglei.common.LogAnalysis
import com.zhanglei.constants.{LogConstants, GlobalConstants}
import com.zhanglei.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/7.
  */
object AnalysisLogTask {

  def processArgs(sparkConf: SparkConf, args: Array[String]) = {
    if(args.length >=1 && Utils.validateDate(args(0))){
      sparkConf.set(GlobalConstants.TASK_RUN_DATE,args(0))
    }else{
      println(
        """
          |参数有误:
          |com.daoke360.kugou_music.task.analysislog.AnalysisLogTask 需要传入一个 "yyyy-MM-dd" 格式的时间参数
          |这个参数用于程序解析这个时间的日志
        """.stripMargin)
      System.exit(0)
    }
  }

  def processInputPath(sparkConf: SparkConf) = {
    val runDate = sparkConf.get(GlobalConstants.TASK_RUN_DATE)
    val datePath = Utils.formatDate(Utils.parseDateToLong(runDate,"yyyy-MM-dd"),"yyyy/MM/dd")
    val inputPath = new Path(GlobalConstants.HDFS_PATH_PREFIX.concat(datePath))
    var fs:FileSystem = null
    try{
      fs = FileSystem.newInstance(new Configuration)
      if(fs.exists(inputPath)){
        sparkConf.get(GlobalConstants.TASK_INPUT_PATH,inputPath.toString)
      }
    }catch{
      case e:IOException => e.printStackTrace()
    }finally{
      if(fs != null)
        fs.close()
    }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //验证参数
    processArgs(sparkConf,args)
    //验证输入路径
    processInputPath(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val ipRules:Array[IPRule] = sparkSession.read.textFile("/spark_sf_project/resource/ip.data").map(line => {
      val field = line.split("[|]")
      IPRule(field(2).toLong,field(3).toLong,field(5),field(6),field(7))
    }).collect()

    val ipRulesBroadCase = sparkSession.sparkContext.broadcast(ipRules)

    //累加器
    val inputAccumulator = sparkSession.sparkContext.longAccumulator("inputAccumulator")
    var filterAccumulator = sparkSession.sparkContext.longAccumulator("filterAccumulator")

    val logMapRdd = sparkSession.sparkContext.textFile(sparkConf.get(GlobalConstants.TASK_INPUT_PATH)).map(logText => {
      inputAccumulator.add(1)
      LogAnalysis.analysisLog(logText,ipRulesBroadCase.value)
    }).filter(logMap => {
      if(logMap == null){
        filterAccumulator.add(1)
        false
      }else{
        true
      }
    })
    val tupleRdd = logMapRdd.map(map => {
      val accessTime = map(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME)
      val ip = map(LogConstants.LOG_COLUMNS_NAME_IP)
      val rowKey = accessTime + "_" + ip.hashCode
      val put = new Put(rowKey.getBytes)
      map.foreach(t => {
        put.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes,t._1.getBytes(),t._2.getBytes())
      })
      (new ImmutableBytesWritable(),put)
    })

    val jobConf = new JobConf(new Configuration())
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,LogConstants.LOG_HBASE_TABLE)
    tupleRdd.saveAsHadoopDataset(jobConf)
    println(s"本次输入记录数：${inputAccumulator.value},过滤记录数:${filterAccumulator.value}")
    sparkSession.close()
  }
}
