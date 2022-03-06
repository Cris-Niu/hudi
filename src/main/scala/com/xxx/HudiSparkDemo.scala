package com.xxx

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @description: some desc
 * @author: 牛虎
 * @email: niuhu@zj.tech
 * @date: 2022/3/5 20:43
 */
object HudiSparkDemo {
  def main(args: Array[String]): Unit = {

    val sc: SparkSession = SparkSession
      .builder().appName("HudiSparkDemo")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //定义变量表名,  保存路径
    val tableName = "tbl_trips_cow"
    val tablePath = "/hudi-warehouse/tbl_trips_cow"
    val dataGenerator = new DataGenerator()
    // 构建数据生成器，模拟产生业务数据
    import  org.apache.hudi.QuickstartUtils._
    //insertData(sc, tableName, tablePath,dataGenerator)

    //快照查询数据
    //snapShotQueryData(sc,tablePath)

    //snapShotQueryDataByTime(sc,tablePath)

    //updateData(sc, tableName, tablePath,dataGenerator)
    incrementalQueryData(sc, tablePath)
    sc.stop()
  }
  // 任务一：模拟数据，插入Hudi表，采用COW模式
  def insertData(sc: SparkSession,tableName:String,tablePath: String,dataGenerator: DataGenerator): Unit={
    import sc.implicits._

    //1. 模拟乘车数据
    val inserts = convertToStringList(dataGenerator.generateInserts(100))

    import scala.collection.JavaConverters._
    val insertDF = sc.read.json(
      sc.sparkContext.parallelize(inserts.asScala, 2).toDS()
    )

    //2. 数据插入hudi表中
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    insertDF.write.mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // Hudi 表的属性值设置
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(tablePath)
  }

  //快照查询数据
  def snapShotQueryData(sc: SparkSession, path: String): Unit ={
    import sc.implicits._

    val df = sc.read.format("hudi").load(path)
   /* df.printSchema()
    df.show(10,truncate = false)*/

    df.filter($"fare" >= 20 && $"fare" <= 50)
      .select($"driver",$"end_lat",$"end_lon",$"fare",$"rider")
      .orderBy($"fare".desc,$"ts".desc)
      .show(20,false)
  }

  def snapShotQueryDataByTime(sc: SparkSession, path: String): Unit ={
    import org.apache.spark.sql.functions._

    //指定字符串格式 yyyyMMddHHmmss
    sc.read.format("hudi")
      .option("as.of.instant","20220305222012").load(path)
      .sort(col("fare").desc)
      .show(10,false)

  }

  def updateData(sc: SparkSession,tableName:String,tablePath: String,dataGenerator: DataGenerator): Unit={
    import sc.implicits._

    //1. 模拟乘车数据
    val updates = convertToStringList(dataGenerator.generateInserts(100))

    import scala.collection.JavaConverters._
    val insertDF = sc.read.json(
      sc.sparkContext.parallelize(updates.asScala, 2).toDS()
    )

    //2. 更新数据插入hudi表中
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    insertDF.write.mode(SaveMode.Append)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // Hudi 表的属性值设置
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .save(tablePath)
  }

  //快照查询数据
  def incrementalQueryData(sc: SparkSession, path: String): Unit ={
    import sc.implicits._

    sc.read.format("hudi").load(path)
      .createOrReplaceTempView("view_temp_hudi_trips")

    val commits = sc.sql(
      """
        |select
        |distinct(_hoodie_commit_time) as commitTime
        |from view_temp_hudi_trips
        |order by commitTime desc""".stripMargin).map(row => row.getString(0))
      .take(50)

    val beginTime = commits(commits.length - 1 )
    println("beginTime = " + beginTime)

    sc.read.format("hudi")
      .option(QUERY_TYPE.key,QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME.key(),beginTime)
      .load(path).createOrReplaceTempView("hudi_incremental")

    sc.sql("""
             |select
             |`_hoodie_commit_time`, fare, ts
             |from hudi_incremental
             |where fare > 20.0""".stripMargin).show(10,false)
  }

}
