// Databricks notebook source
// MAGIC %md ##Parametrized version of the TPC-* data generator to run as a job
// MAGIC 
// MAGIC ### Sample job parameters:
// MAGIC <pre>
// MAGIC {  
// MAGIC   "baseLocation": "/mnt/my_bucket/TPC",
// MAGIC   "fileFormat": "delta",
// MAGIC   "benchmarks": "TPCDS, TPCH",
// MAGIC   "scaleFactors": "1, 10"  
// MAGIC   "overwrite": "false",
// MAGIC   "createTableStats": "true",
// MAGIC   "partitionTables": "true",
// MAGIC }
// MAGIC </pre>  

// COMMAND ----------

// DBTITLE 1,For debugging parameters, uncomment as needed
//dbutils.widgets.text("TPCDSUseDoubleForDecimal", "true")
//dbutils.widgets.remove("benchmark")

// COMMAND ----------

// DBTITLE 1,Get the job parameters or use default values
// Multi TPC- H and DS generator and database importer using spark-sql-perf, typically to generate parquet files in S3/blobstore objects
def tryGetWidget[T](name: String): scala.util.Try[String] = scala.util.Try(dbutils.widgets.get(name))
def tryGetWidgetSet[T](name: String): scala.util.Try[Set[String]] = scala.util.Try(dbutils.widgets.get(name).split(",").map(_.trim).toSet)

/* for Shasta
val expectedWorkers = tryGetWidget("expectedWorkers").getOrElse("8")
spark.conf.set("spark.databricks.clusterUsageTags.clusterTargetWorkers", expectedWorkers) */

val benchmarks = tryGetWidgetSet("benchmarks").getOrElse(Set("TPCDS", "TPCH")).map(_.toUpperCase)
val scaleFactors = tryGetWidgetSet("scaleFactors").getOrElse(Set("1", "10", "100", "1000", "10000")).map(_.toInt).toSeq.sorted.map(_.toString) 

val baseLocation = tryGetWidget("baseLocation").getOrElse("/mnt/performance-datasets") // S3 bucket, blob, or local root path
val baseDatagenFolder = tryGetWidget("baseDatagenFolder").getOrElse("/local_disk0/tmp")  // usually /tmp if enough space is available for datagen files

// Output files
val overwrite = tryGetWidget("overwrite").map(_.toBoolean).getOrElse(false) //if to delete existing files (doesn't check if results are complete on no-overwrite)
val fileFormat = tryGetWidget("fileFormat").getOrElse("delta") // parquet, delta, orc, etc
val partitionTables = tryGetWidget("partitionTables").map(_.toBoolean).getOrElse(true) // if to partition tables
val distributeStrategy = tryGetWidget("distributeStrategy").getOrElse(
  if (fileFormat == "delta") "none" else "distributeBy") // experimental: none, distributeBy, clusterBy, packBy
val coalesceInto: Int = tryGetWidget("coalesceInto").map(_.toInt).getOrElse(1) // For non-delta, how many files for non-partitioned tables.  This determines parallelism in the writes



// Generate stats for CBO
val createTableStats = tryGetWidget("createTableStats").map(_.toBoolean).getOrElse(true)
val createColumnStats = tryGetWidget("createColumnStats").map(_.toBoolean).getOrElse(true)

val workers: Int = if (spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt > 0) spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt else 1 //number of nodes, assumes one executor per node.  
val cores: Int = Runtime.getRuntime.availableProcessors.toInt //number of CPU-cores for parallelization calculation

// Set only if creating multiple DBs or source file folders with different settings, use a leading _
var dbSuffix = tryGetWidget("dbSuffix").getOrElse("") 
if (!partitionTables) dbSuffix = "_nopartitions" + dbSuffix
if (!createTableStats) dbSuffix = "_nostats" + dbSuffix

// Set to generate file and schema naming and datatypes compatible with older results (legacy)
// as in: tpcds/sf1000-parquet/useDecimal=false,useDate=false,filterNull=false
val TPCDSUseLegacyOptions = tryGetWidget("TPCDSUseLegacyOptions").map(_.toBoolean).getOrElse(false) 

val TPCDSUseDoubleForDecimal = tryGetWidget("TPCDSUseDoubleForDecimal").map(_.toBoolean).getOrElse(false)
if (TPCDSUseDoubleForDecimal) dbSuffix = "_nodecimal" + dbSuffix

val onlyTextFiles = tryGetWidget("onlyTextFiles").map(_.toBoolean).getOrElse(false)
val textCompression = tryGetWidget("textCompression").getOrElse("none") //none, bzip2, gzip, lz4, snappy and deflate

val duplicateTPCDScolumn = tryGetWidget("duplicateTPCDScolumn").map(_.toBoolean).getOrElse(true)  // to accomodate both new and older spec version queries (ie., q30)

val skipOptimize = tryGetWidget("skipOptimize").map(_.toBoolean).getOrElse(false)  // to skip the delta optimize ie., for manual tests
val skipChecks = tryGetWidget("skipChecks").map(_.toBoolean).getOrElse(false)  // to skip some slow check ie., for running them in a separate job

//val parallelizeTables = tryGetWidget("parallelizeTables").map(_.toBoolean).getOrElse(false)

// COMMAND ----------

// DBTITLE 1,Get the core count from the executors and add one in case of odd numbers ES-38338
import scala.concurrent.duration._
var (_, coresStr) = sc.runOnEachExecutor[String](() => {
 Runtime.getRuntime.availableProcessors.toString
}, 5.seconds).mapValues[String](t => t.get).take(1).head

val cores = if (coresStr.toInt % 2 == 0) coresStr.toInt else coresStr.toInt +1

// COMMAND ----------

// DBTITLE 1,Imports
// Imports, fail fast if we are missing any library

// For datagens
import java.io._
import scala.sys.process._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Spark/Hadoop config
import org.apache.spark.deploy.SparkHadoopUtil

// COMMAND ----------

// DBTITLE 1,Spark settings for writing data
// Set Spark config to produce same and comparable source files across runs

spark.conf.set("spark.sql.shuffle.partitions", (cores * workers * 2).toString) // 2 writers per cluster cores

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.files.maxRecordsPerFile", "0")  // force larger files

spark.conf.set("spark.sql.legacy.charVarcharAsString", "true") // needed for 8.x+

// S3 
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "536870912")
spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")


if (Seq("delta", "tahoe").contains(fileFormat) && partitionTables) {
  spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
//   spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "4096")
//   spark.conf.set("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "5000000")
} else {
  // add parquet configs here
}

if (fileFormat == "orc") {
  spark.sqlContext.setConf("spark.sql.orc.impl", "native")
  spark.sqlContext.setConf("spark.sql.orc.enableVectorizedReader", "true") 
  spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
  spark.sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
  spark.sqlContext.setConf("spark.sql.orc.char.enabled", "true")
  spark.sqlContext.setConf("spark.sql.orc.compression.codec", "snappy")
}

// COMMAND ----------

// DBTITLE 1,Settings by scale factor
def setScaleConfig(scaleFactor: String): Unit = {
  if (scaleFactor.toInt >= 100000) { 
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.05")
  }   
  else if (scaleFactor.toInt >= 10000) {    
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
  } 
  else if (scaleFactor.toInt >= 1000) {
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.3")    
  }
  else { 
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.5")
  }
}

// COMMAND ----------

// DBTITLE 1,Logger and utilities
//import org.apache.log4j.Level

//@transient lazy val logger = org.apache.log4j.LogManager.getLogger(s"Notebook-logger")

def log(str: String) = {
  println(java.time.LocalDateTime.now + s"\t${str}")
  //logger.info(s"${str}")
}

// Time command helper
var timings = scala.collection.mutable.Map[String, Long]()
def time[R](blockName: String, block: => R): R = {  
    log(s"Starting '$blockName'...")
    val t0 = System.currentTimeMillis() //nanoTime()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis() //nanoTime()
    val elapsed = t1 -t0
    timings += (blockName -> elapsed)
    log(s"Elapsed time for '$blockName': $elapsed ms")
    result
}

// COMMAND ----------

// DBTITLE 1,Wait for all the workers to be ready, we need to install the tpc kits
// Checks that we have the correct number of worker nodes to start the data generation
// Make sure you have set the workers variable correctly, as the datagens binaries need to be present in all nodes
val targetWorkers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt
def numWorkers: Int = sc.getExecutorMemoryStatus.size - 1
def waitForWorkers(requiredWorkers: Int, tries: Int) : Unit = {
  for (i <- 0 to (tries-1)) {
    if (numWorkers == requiredWorkers) {
      log(s"All workers ready. Waited ${i}s. for $numWorkers workers to be ready.")
      return
    }
    if (i % 60 == 0) println(s"waiting ${i}s. for workers to be ready, got only $numWorkers workers")
    Thread sleep 1000
  }
  val failedMsg = s"Timed out waiting for workers to be ready after ${tries}s."
  log(failedMsg)
  throw new Exception(failedMsg)
}
waitForWorkers(targetWorkers, 3600) //wait up to an hour

// COMMAND ----------

// DBTITLE 1,[DEBUG] Get all worker hostname details for profiling 
// import scala.io.Source

// def getDetails = scala.collection.mutable.Map(  
//   "hostname" -> Source.fromURL("http://169.254.169.254/latest/meta-data/hostname").mkString,
//   "public-hostname" -> Source.fromURL("http://169.254.169.254/latest/meta-data/public-hostname").mkString,
//   "public-ipv4" -> Source.fromURL("http://169.254.169.254/latest/meta-data/public-ipv4").mkString,
//   "local-hostname" -> Source.fromURL("http://169.254.169.254/latest/meta-data/local-hostname").mkString,
//   "local-ipv4" -> Source.fromURL("http://169.254.169.254/latest/meta-data/local-ipv4").mkString,  
//   "internal-hostname" -> java.net.InetAddress.getLocalHost().getHostName(), 
//   "instance-id" -> Source.fromURL("http://169.254.169.254/latest/meta-data/instance-id").mkString,
//   "instance-type" -> Source.fromURL("http://169.254.169.254/latest/meta-data/instance-type").mkString
// )

// val driverDetails = getDetails
// val workerDetails = sc.parallelize(1 to 999).map(w=> getDetails).distinct.collect

// val allNodes = Array(driverDetails) ++ workerDetails

// allNodes.foreach{ n=>
//   n.foreach{ case(k,v) => {
//     println(s"$k\t\t-> $v")
//   }}
//   println
// }

// COMMAND ----------

// DBTITLE 1,Installer for TPC-H (dbgen)
// FOR INSTALLING TPCH DBGEN (with the sdtout patch)
def installDBGEN(url: String = "https://github.com/databricks/tpch-dbgen.git", useStdout: Boolean = true, baseFolder: String = "/tmp")(i: java.lang.Long): String = {
  // check if we want the revision which makes dbgen output to stdout
  val checkoutRevision: String = if (useStdout) "git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8 -- bm_utils.c" else ""
  Seq("mkdir", "-p", baseFolder).!
  val pw = new PrintWriter(new File(s"${baseFolder}/dbgen_$i.sh" ))
  pw.write(s"""
rm -rf ${baseFolder}/dbgen
rm -rf ${baseFolder}/dbgen_install_$i
mkdir ${baseFolder}/dbgen_install_$i
cd ${baseFolder}/dbgen_install_$i
git clone '$url'
cd tpch-dbgen
$checkoutRevision
make
ln -sf ${baseFolder}/dbgen_install_$i/tpch-dbgen ${baseFolder}/dbgen || echo "ln -sf failed"
test -e ${baseFolder}/dbgen/dbgen
echo "OK"
  """)
  pw.close
  Seq("chmod", "+x", s"${baseFolder}/dbgen_$i.sh").!
  Seq(s"${baseFolder}/dbgen_$i.sh").!!
}

// COMMAND ----------

// DBTITLE 1,Installer for TPC-DS (dsdgen)
// FOR INSTALLING TPCDS DSDGEN (with the sdtout patch) 
// Note: it assumes Debian/Ubuntu host, edit package manager if not
def installDSDGEN(url: String = "https://github.com/databricks/tpcds-kit.git", useStdout: Boolean = true, baseFolder: String = "/tmp")(i: java.lang.Long): String = {
  Seq("mkdir", "-p", baseFolder).!
  val pw = new PrintWriter(new File(s"${baseFolder}/dsdgen_$i.sh" ))
  pw.write(s"""
sudo apt-get update
sudo apt-get -y --force-yes install gcc make flex bison byacc git
rm -rf ${baseFolder}/dsdgen
rm -rf ${baseFolder}/dsdgen_install_$i
mkdir ${baseFolder}/dsdgen_install_$i
cd ${baseFolder}/dsdgen_install_$i
git clone '$url'
cd tpcds-kit/tools
make -f Makefile.suite
ln -sf ${baseFolder}/dsdgen_install_$i/tpcds-kit/tools ${baseFolder}/dsdgen || echo "ln -sf failed"
${baseFolder}/dsdgen/dsdgen -h
test -e ${baseFolder}/dsdgen/dsdgen
echo "OK"
  """)
  pw.close
  Seq("chmod", "+x", s"${baseFolder}/dsdgen_$i.sh").!
  Seq(s"${baseFolder}/dsdgen_$i.sh").!!
}

// COMMAND ----------

// DBTITLE 1,Build and install TPC binaries in executors
// install (build) the data generators in all nodes
val res = spark.range(0, workers, 1, workers).map(worker => benchmarks.map{
    case "TPCDS" => s"TPCDS worker $worker\n" + installDSDGEN(baseFolder = baseDatagenFolder)(worker)
    case "TPCH" => s"TPCH worker $worker\n" + installDBGEN(baseFolder = baseDatagenFolder)(worker)
  }).collect()

// COMMAND ----------

// DBTITLE 1,Set the schema name (ie., legacy configs), tables, and location for each benchmark
def getBenchmarkData(benchmark: String, scaleFactor: String) = benchmark match {    
  case "TPCH" => (
    s"tpch_sf${scaleFactor}_${fileFormat}${dbSuffix}",
    s"$baseLocation/tpch/tpch_sf${scaleFactor}_${fileFormat}${dbSuffix}")  
  
  case "TPCDS" if !TPCDSUseLegacyOptions && !TPCDSUseDoubleForDecimal  => (
    s"tpcds_sf${scaleFactor}_${fileFormat}${dbSuffix}",
    s"$baseLocation/tpcds-2.13/tpcds_sf${scaleFactor}_${fileFormat}${dbSuffix}")
  
  case "TPCDS" if TPCDSUseDoubleForDecimal => (
    s"tpcds_sf${scaleFactor}_${fileFormat}${dbSuffix}",
    s"$baseLocation/tpcds-2.13/tpcds_sf${scaleFactor}_${fileFormat}${dbSuffix}")  
  
  case "TPCDS" if TPCDSUseLegacyOptions => (
    if (Seq("delta", "tahoe").contains(fileFormat)) s"tpcds_sf${scaleFactor}_nodecimal_nodate_withnulls_delta${dbSuffix}" 
    else s"tpcds_sf${scaleFactor}_nodecimal_nodate_withnulls${dbSuffix}",
    s"$baseLocation/tpcds/tpcds_sf${scaleFactor}-${fileFormat}${dbSuffix}/useDecimal=false,useDate=false,filterNull=false")
}

// Namings for text files
def getTextData(benchmark: String, scaleFactor: String) = benchmark match {    
  case "TPCH" => (
    s"tpch_sf${scaleFactor}_text",
    s"$baseLocation/tpch/tpch_sf${scaleFactor}_text") 
  case "TPCDS" => (
    s"tpcds_sf${scaleFactor}_text",
    s"$baseLocation/tpcds-2.13/tpcds_sf${scaleFactor}_text")
}

def getNameLocation(benchmark: String, scaleFactor: String) =
  if (onlyTextFiles) getTextData(benchmark, scaleFactor) else getBenchmarkData(benchmark, scaleFactor)

// COMMAND ----------

// DBTITLE 1,Utility functions
def getTables(db: String) = {
  sql(s"use `$db`")
  sql(s"show tables").select("tableName")
    .collect().map(_.toString.drop(1).dropRight(1)) 
}

def getColumns(db: String, tableName: String) = table(s"`$db`.`$tableName`").columns

def partitionedCols(tableName: String): Set[String] = {
  try { 
    val df = sql(s"SHOW PARTITIONS $tableName")
    //df.map(_.getAs[String](0)).first.split('/').map(_.split("=")(0)).toSet
    df.columns.toSet
  } catch {
    case e: Throwable => Set.empty[String]
  }
}

def partitioningString(tableName: String): String = {
  val pCols = partitionedCols(tableName: String)
  if (!pCols.isEmpty) "PARTITIONED BY (" + pCols.mkString + ")"
  else ""
}

// COMMAND ----------

// DBTITLE 1,Expected tables for TPC- DS and H
val tableNamesTpcds = Seq(
  "inventory", "catalog_returns", "store_returns",  "web_returns", "web_sales",  "store_sales", // with partitions
  "call_center", "catalog_page", 
  "customer_address", "customer_demographics", "customer", 
  "date_dim", "household_demographics", "income_band",
  "item", "promotion", "reason", "ship_mode", 
  "store", "time_dim", 
  "warehouse", "web_page", 
  "web_site",
  "catalog_sales"
).sorted

// var tpcdsLargeTables = Set("customer_demographics","customer_address","customer","inventory","web_sales","store_sales","catalog_sales")
// var tpcdsSmallTables = Set("call_center","catalog_page","date_dim","household_demographics","income_band","item","promotion","reason","ship_mode","store","time_dim",
//                            "warehouse","web_page","web_site")

val tableNamesTpch = Seq(
  "customer", "lineitem", "nation", "orders", "part", 
  "region", "supplier", "partsupp"
).sorted

def getBenchmarkTables(benchmark: String) = benchmark match {
  case "TPCDS" => tableNamesTpcds
  case "TPCH" => tableNamesTpch
  case _ => throw new Exception(s"Invalid benchmark $benchmark")
}

// COMMAND ----------

// DBTITLE 1,For paralelizing table operations i.e., collect stats for manual
// import scala.collection.parallel._

// if (parallelizeTables) {
//   val tablesDS = tableNames.par
//   val tablesH = tableNamesTpch.par
//   val parLevel = 4
//   val forkJoinPool =  new scala.concurrent.forkjoin.ForkJoinPool // For DBR 6 use java.util.concurrent.ForkJoinPool(parLevel)
//   tables.tasksupport = new ForkJoinTaskSupport(forkJoinPool)  
// }

// COMMAND ----------

// val tableSchemas = Map(
// "call_center" -> """
// CREATE TABLE `call_center` (
//   `cc_call_center_sk` INT,
//   `cc_call_center_id` STRING,
//   `cc_rec_start_date` DATE,
//   `cc_rec_end_date` DATE,
//   `cc_closed_date_sk` INT,
//   `cc_open_date_sk` INT,
//   `cc_name` STRING,
//   `cc_class` STRING,
//   `cc_employees` INT,
//   `cc_sq_ft` INT,
//   `cc_hours` STRING,
//   `cc_manager` STRING,
//   `cc_mkt_id` INT,
//   `cc_mkt_class` STRING,
//   `cc_mkt_desc` STRING,
//   `cc_market_manager` STRING,
//   `cc_division` INT,
//   `cc_division_name` STRING,
//   `cc_company` INT,
//   `cc_company_name` STRING,
//   `cc_street_number` STRING,
//   `cc_street_name` STRING,
//   `cc_street_type` STRING,
//   `cc_suite_number` STRING,
//   `cc_city` STRING,
//   `cc_county` STRING,
//   `cc_state` STRING,
//   `cc_zip` STRING,
//   `cc_country` STRING,
//   `cc_gmt_offset` DECIMAL(5,2),
//   `cc_tax_percentage` DECIMAL(5,2))
// USING text
// LOCATION '###LOC###/call_center'""",
// "catalog_page" -> """
// CREATE TABLE `catalog_page` (
//   `cp_catalog_page_sk` INT,
//   `cp_catalog_page_id` STRING,
//   `cp_start_date_sk` INT,
//   `cp_end_date_sk` INT,
//   `cp_department` STRING,
//   `cp_catalog_number` INT,
//   `cp_catalog_page_number` INT,
//   `cp_description` STRING,
//   `cp_type` STRING)
// USING text
// LOCATION '###LOC###/catalog_page'""",
// "catalog_returns" -> """
// CREATE TABLE `catalog_returns` (
//   `cr_returned_date_sk` INT,
//   `cr_returned_time_sk` INT,
//   `cr_item_sk` INT,
//   `cr_refunded_customer_sk` INT,
//   `cr_refunded_cdemo_sk` INT,
//   `cr_refunded_hdemo_sk` INT,
//   `cr_refunded_addr_sk` INT,
//   `cr_returning_customer_sk` INT,
//   `cr_returning_cdemo_sk` INT,
//   `cr_returning_hdemo_sk` INT,
//   `cr_returning_addr_sk` INT,
//   `cr_call_center_sk` INT,
//   `cr_catalog_page_sk` INT,
//   `cr_ship_mode_sk` INT,
//   `cr_warehouse_sk` INT,
//   `cr_reason_sk` INT,
//   `cr_order_number` BIGINT,
//   `cr_return_quantity` INT,
//   `cr_return_amount` DECIMAL(7,2),
//   `cr_return_tax` DECIMAL(7,2),
//   `cr_return_amt_inc_tax` DECIMAL(7,2),
//   `cr_fee` DECIMAL(7,2),
//   `cr_return_ship_cost` DECIMAL(7,2),
//   `cr_refunded_cash` DECIMAL(7,2),
//   `cr_reversed_charge` DECIMAL(7,2),
//   `cr_store_credit` DECIMAL(7,2),
//   `cr_net_loss` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/catalog_returns'""",
// "catalog_sales" -> """
// CREATE TABLE `catalog_sales` (
//   `cs_sold_date_sk` INT,
//   `cs_sold_time_sk` INT,
//   `cs_ship_date_sk` INT,
//   `cs_bill_customer_sk` INT,
//   `cs_bill_cdemo_sk` INT,
//   `cs_bill_hdemo_sk` INT,
//   `cs_bill_addr_sk` INT,
//   `cs_ship_customer_sk` INT,
//   `cs_ship_cdemo_sk` INT,
//   `cs_ship_hdemo_sk` INT,
//   `cs_ship_addr_sk` INT,
//   `cs_call_center_sk` INT,
//   `cs_catalog_page_sk` INT,
//   `cs_ship_mode_sk` INT,
//   `cs_warehouse_sk` INT,
//   `cs_item_sk` INT,
//   `cs_promo_sk` INT,
//   `cs_order_number` BIGINT,
//   `cs_quantity` INT,
//   `cs_wholesale_cost` DECIMAL(7,2),
//   `cs_list_price` DECIMAL(7,2),
//   `cs_sales_price` DECIMAL(7,2),
//   `cs_ext_discount_amt` DECIMAL(7,2),
//   `cs_ext_sales_price` DECIMAL(7,2),
//   `cs_ext_wholesale_cost` DECIMAL(7,2),
//   `cs_ext_list_price` DECIMAL(7,2),
//   `cs_ext_tax` DECIMAL(7,2),
//   `cs_coupon_amt` DECIMAL(7,2),
//   `cs_ext_ship_cost` DECIMAL(7,2),
//   `cs_net_paid` DECIMAL(7,2),
//   `cs_net_paid_inc_tax` DECIMAL(7,2),
//   `cs_net_paid_inc_ship` DECIMAL(7,2),
//   `cs_net_paid_inc_ship_tax` DECIMAL(7,2),
//   `cs_net_profit` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/catalog_sales'""",
// "customer" -> """
// CREATE TABLE `customer` (
//   `c_customer_sk` INT,
//   `c_customer_id` STRING,
//   `c_current_cdemo_sk` INT,
//   `c_current_hdemo_sk` INT,
//   `c_current_addr_sk` INT,
//   `c_first_shipto_date_sk` INT,
//   `c_first_sales_date_sk` INT,
//   `c_salutation` STRING,
//   `c_first_name` STRING,
//   `c_last_name` STRING,
//   `c_preferred_cust_flag` STRING,
//   `c_birth_day` INT,
//   `c_birth_month` INT,
//   `c_birth_year` INT,
//   `c_birth_country` STRING,
//   `c_login` STRING,
//   `c_email_address` STRING,
//   `c_last_review_date_sk` INT)
// USING text
// LOCATION '###LOC###/customer'""",
// "customer_address" -> """
// CREATE TABLE `customer_address` (
//   `ca_address_sk` INT,
//   `ca_address_id` STRING,
//   `ca_street_number` STRING,
//   `ca_street_name` STRING,
//   `ca_street_type` STRING,
//   `ca_suite_number` STRING,
//   `ca_city` STRING,
//   `ca_county` STRING,
//   `ca_state` STRING,
//   `ca_zip` STRING,
//   `ca_country` STRING,
//   `ca_gmt_offset` DECIMAL(5,2),
//   `ca_location_type` STRING)
// USING text
// LOCATION '###LOC###/customer_address'""",
// "customer_demographics" -> """
// CREATE TABLE `customer_demographics` (
//   `cd_demo_sk` INT,
//   `cd_gender` STRING,
//   `cd_marital_status` STRING,
//   `cd_education_status` STRING,
//   `cd_purchase_estimate` INT,
//   `cd_credit_rating` STRING,
//   `cd_dep_count` INT,
//   `cd_dep_employed_count` INT,
//   `cd_dep_college_count` INT)
// USING text
// LOCATION '###LOC###/customer_demographics'""",
// "date_dim" -> """
// CREATE TABLE `date_dim` (
//   `d_date_sk` INT,
//   `d_date_id` STRING,
//   `d_date` DATE,
//   `d_month_seq` INT,
//   `d_week_seq` INT,
//   `d_quarter_seq` INT,
//   `d_year` INT,
//   `d_dow` INT,
//   `d_moy` INT,
//   `d_dom` INT,
//   `d_qoy` INT,
//   `d_fy_year` INT,
//   `d_fy_quarter_seq` INT,
//   `d_fy_week_seq` INT,
//   `d_day_name` STRING,
//   `d_quarter_name` STRING,
//   `d_holiday` STRING,
//   `d_weekend` STRING,
//   `d_following_holiday` STRING,
//   `d_first_dom` INT,
//   `d_last_dom` INT,
//   `d_same_day_ly` INT,
//   `d_same_day_lq` INT,
//   `d_current_day` STRING,
//   `d_current_week` STRING,
//   `d_current_month` STRING,
//   `d_current_quarter` STRING,
//   `d_current_year` STRING)
// USING text
// LOCATION '###LOC###/date_dim'""",
// "household_demographics" -> """
// CREATE TABLE `household_demographics` (
//   `hd_demo_sk` INT,
//   `hd_income_band_sk` INT,
//   `hd_buy_potential` STRING,
//   `hd_dep_count` INT,
//   `hd_vehicle_count` INT)
// USING text
// LOCATION '###LOC###/household_demographics'""",
// "income_band" -> """
// CREATE TABLE `income_band` (
//   `ib_income_band_sk` INT,
//   `ib_lower_bound` INT,
//   `ib_upper_bound` INT)
// USING text
// LOCATION '###LOC###/income_band'""",
// "inventory" -> """
//   `inv_date_sk` INT,
//   `inv_item_sk` INT,
//   `inv_warehouse_sk` INT,
//   `inv_quantity_on_hand` INT
// """,
// "item" -> """
// CREATE TABLE `item` (
//   `i_item_sk` INT,
//   `i_item_id` STRING,
//   `i_rec_start_date` DATE,
//   `i_rec_end_date` DATE,
//   `i_item_desc` STRING,
//   `i_current_price` DECIMAL(7,2),
//   `i_wholesale_cost` DECIMAL(7,2),
//   `i_brand_id` INT,
//   `i_brand` STRING,
//   `i_class_id` INT,
//   `i_class` STRING,
//   `i_category_id` INT,
//   `i_category` STRING,
//   `i_manufact_id` INT,
//   `i_manufact` STRING,
//   `i_size` STRING,
//   `i_formulation` STRING,
//   `i_color` STRING,
//   `i_units` STRING,
//   `i_container` STRING,
//   `i_manager_id` INT,
//   `i_product_name` STRING)
// USING text
// LOCATION '###LOC###/item'""",
// "promotion" -> """
// CREATE TABLE `promotion` (
//   `p_promo_sk` INT,
//   `p_promo_id` STRING,
//   `p_start_date_sk` INT,
//   `p_end_date_sk` INT,
//   `p_item_sk` INT,
//   `p_cost` DECIMAL(15,2),
//   `p_response_target` INT,
//   `p_promo_name` STRING,
//   `p_channel_dmail` STRING,
//   `p_channel_email` STRING,
//   `p_channel_catalog` STRING,
//   `p_channel_tv` STRING,
//   `p_channel_radio` STRING,
//   `p_channel_press` STRING,
//   `p_channel_event` STRING,
//   `p_channel_demo` STRING,
//   `p_channel_details` STRING,
//   `p_purpose` STRING,
//   `p_discount_active` STRING)
// USING text
// LOCATION '###LOC###/promotion'""",
// "reason" -> """
// REATE TABLE `reason` (
//   `r_reason_sk` INT,
//   `r_reason_id` STRING,
//   `r_reason_desc` STRING)
// USING text
// LOCATION '###LOC###/reason'""",
// "ship_mode" -> """
// CREATE TABLE `ship_mode` (
//   `sm_ship_mode_sk` INT,
//   `sm_ship_mode_id` STRING,
//   `sm_type` STRING,
//   `sm_code` STRING,
//   `sm_carrier` STRING,
//   `sm_contract` STRING)
// USING text
// LOCATION '###LOC###/ship_mode'""",
// "store" -> """
// CREATE TABLE `store` (
//   `s_store_sk` INT,
//   `s_store_id` STRING,
//   `s_rec_start_date` DATE,
//   `s_rec_end_date` DATE,
//   `s_closed_date_sk` INT,
//   `s_store_name` STRING,
//   `s_number_employees` INT,
//   `s_floor_space` INT,
//   `s_hours` STRING,
//   `s_manager` STRING,
//   `s_market_id` INT,
//   `s_geography_class` STRING,
//   `s_market_desc` STRING,
//   `s_market_manager` STRING,
//   `s_division_id` INT,
//   `s_division_name` STRING,
//   `s_company_id` INT,
//   `s_company_name` STRING,
//   `s_street_number` STRING,
//   `s_street_name` STRING,
//   `s_street_type` STRING,
//   `s_suite_number` STRING,
//   `s_city` STRING,
//   `s_county` STRING,
//   `s_state` STRING,
//   `s_zip` STRING,
//   `s_country` STRING,
//   `s_gmt_offset` DECIMAL(5,2),
//   `s_tax_precentage` DECIMAL(5,2))
// USING text
// LOCATION '###LOC###/store'""",
// "store_returns" -> """
// CREATE TABLE `store_returns` (
//   `sr_returned_date_sk` INT,
//   `sr_return_time_sk` INT,
//   `sr_item_sk` INT,
//   `sr_customer_sk` INT,
//   `sr_cdemo_sk` INT,
//   `sr_hdemo_sk` INT,
//   `sr_addr_sk` INT,
//   `sr_store_sk` INT,
//   `sr_reason_sk` INT,
//   `sr_ticket_number` BIGINT,
//   `sr_return_quantity` INT,
//   `sr_return_amt` DECIMAL(7,2),
//   `sr_return_tax` DECIMAL(7,2),
//   `sr_return_amt_inc_tax` DECIMAL(7,2),
//   `sr_fee` DECIMAL(7,2),
//   `sr_return_ship_cost` DECIMAL(7,2),
//   `sr_refunded_cash` DECIMAL(7,2),
//   `sr_reversed_charge` DECIMAL(7,2),
//   `sr_store_credit` DECIMAL(7,2),
//   `sr_net_loss` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/store_returns'""",
// "store_sales" -> """
// CREATE TABLE `store_sales` (
//   `ss_sold_date_sk` INT,
//   `ss_sold_time_sk` INT,
//   `ss_item_sk` INT,
//   `ss_customer_sk` INT,
//   `ss_cdemo_sk` INT,
//   `ss_hdemo_sk` INT,
//   `ss_addr_sk` INT,
//   `ss_store_sk` INT,
//   `ss_promo_sk` INT,
//   `ss_ticket_number` BIGINT,
//   `ss_quantity` INT,
//   `ss_wholesale_cost` DECIMAL(7,2),
//   `ss_list_price` DECIMAL(7,2),
//   `ss_sales_price` DECIMAL(7,2),
//   `ss_ext_discount_amt` DECIMAL(7,2),
//   `ss_ext_sales_price` DECIMAL(7,2),
//   `ss_ext_wholesale_cost` DECIMAL(7,2),
//   `ss_ext_list_price` DECIMAL(7,2),
//   `ss_ext_tax` DECIMAL(7,2),
//   `ss_coupon_amt` DECIMAL(7,2),
//   `ss_net_paid` DECIMAL(7,2),
//   `ss_net_paid_inc_tax` DECIMAL(7,2),
//   `ss_net_profit` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/store_sales'""",
// "time_dim" -> """
// CREATE TABLE `time_dim` (
//   `t_time_sk` INT,
//   `t_time_id` STRING,
//   `t_time` INT,
//   `t_hour` INT,
//   `t_minute` INT,
//   `t_second` INT,
//   `t_am_pm` STRING,
//   `t_shift` STRING,
//   `t_sub_shift` STRING,
//   `t_meal_time` STRING)
// USING text
// LOCATION '###LOC###/time_dim'""",
// "warehouse" -> """
// CREATE TABLE `warehouse` (
//   `w_warehouse_sk` INT,
//   `w_warehouse_id` STRING,
//   `w_warehouse_name` STRING,
//   `w_warehouse_sq_ft` INT,
//   `w_street_number` STRING,
//   `w_street_name` STRING,
//   `w_street_type` STRING,
//   `w_suite_number` STRING,
//   `w_city` STRING,
//   `w_county` STRING,
//   `w_state` STRING,
//   `w_zip` STRING,
//   `w_country` STRING,
//   `w_gmt_offset` DECIMAL(5,2))
// USING text
// LOCATION '###LOC###/warehouse'""",
// "web_page" -> """
// CREATE TABLE `web_page` (
//   `wp_web_page_sk` INT,
//   `wp_web_page_id` STRING,
//   `wp_rec_start_date` DATE,
//   `wp_rec_end_date` DATE,
//   `wp_creation_date_sk` INT,
//   `wp_access_date_sk` INT,
//   `wp_autogen_flag` STRING,
//   `wp_customer_sk` INT,
//   `wp_url` STRING,
//   `wp_type` STRING,
//   `wp_char_count` INT,
//   `wp_link_count` INT,
//   `wp_image_count` INT,
//   `wp_max_ad_count` INT)
// USING text
// LOCATION '###LOC###/web_page'""",
// "web_returns" -> """
// CREATE TABLE `web_returns` (
//   `wr_returned_date_sk` INT,
//   `wr_returned_time_sk` INT,
//   `wr_item_sk` INT,
//   `wr_refunded_customer_sk` INT,
//   `wr_refunded_cdemo_sk` INT,
//   `wr_refunded_hdemo_sk` INT,
//   `wr_refunded_addr_sk` INT,
//   `wr_returning_customer_sk` INT,
//   `wr_returning_cdemo_sk` INT,
//   `wr_returning_hdemo_sk` INT,
//   `wr_returning_addr_sk` INT,
//   `wr_web_page_sk` INT,
//   `wr_reason_sk` INT,
//   `wr_order_number` BIGINT,
//   `wr_return_quantity` INT,
//   `wr_return_amt` DECIMAL(7,2),
//   `wr_return_tax` DECIMAL(7,2),
//   `wr_return_amt_inc_tax` DECIMAL(7,2),
//   `wr_fee` DECIMAL(7,2),
//   `wr_return_ship_cost` DECIMAL(7,2),
//   `wr_refunded_cash` DECIMAL(7,2),
//   `wr_reversed_charge` DECIMAL(7,2),
//   `wr_account_credit` DECIMAL(7,2),
//   `wr_net_loss` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/web_returns'""",
// "web_sales" -> """
// CREATE TABLE `web_sales` (
//   `ws_sold_date_sk` INT,
//   `ws_sold_time_sk` INT,
//   `ws_ship_date_sk` INT,
//   `ws_item_sk` INT,
//   `ws_bill_customer_sk` INT,
//   `ws_bill_cdemo_sk` INT,
//   `ws_bill_hdemo_sk` INT,
//   `ws_bill_addr_sk` INT,
//   `ws_ship_customer_sk` INT,
//   `ws_ship_cdemo_sk` INT,
//   `ws_ship_hdemo_sk` INT,
//   `ws_ship_addr_sk` INT,
//   `ws_web_page_sk` INT,
//   `ws_web_site_sk` INT,
//   `ws_ship_mode_sk` INT,
//   `ws_warehouse_sk` INT,
//   `ws_promo_sk` INT,
//   `ws_order_number` BIGINT,
//   `ws_quantity` INT,
//   `ws_wholesale_cost` DECIMAL(7,2),
//   `ws_list_price` DECIMAL(7,2),
//   `ws_sales_price` DECIMAL(7,2),
//   `ws_ext_discount_amt` DECIMAL(7,2),
//   `ws_ext_sales_price` DECIMAL(7,2),
//   `ws_ext_wholesale_cost` DECIMAL(7,2),
//   `ws_ext_list_price` DECIMAL(7,2),
//   `ws_ext_tax` DECIMAL(7,2),
//   `ws_coupon_amt` DECIMAL(7,2),
//   `ws_ext_ship_cost` DECIMAL(7,2),
//   `ws_net_paid` DECIMAL(7,2),
//   `ws_net_paid_inc_tax` DECIMAL(7,2),
//   `ws_net_paid_inc_ship` DECIMAL(7,2),
//   `ws_net_paid_inc_ship_tax` DECIMAL(7,2),
//   `ws_net_profit` DECIMAL(7,2))
// USING text
// LOCATION '###LOC###/web_sales'""",
// "web_site" -> """
// CREATE TABLE `web_site` (
//   `web_site_sk` INT,
//   `web_site_id` STRING,
//   `web_rec_start_date` DATE,
//   `web_rec_end_date` DATE,
//   `web_name` STRING,
//   `web_open_date_sk` INT,
//   `web_close_date_sk` INT,
//   `web_class` STRING,
//   `web_manager` STRING,
//   `web_mkt_id` INT,
//   `web_mkt_class` STRING,
//   `web_mkt_desc` STRING,
//   `web_market_manager` STRING,
//   `web_company_id` INT,
//   `web_company_name` STRING,
//   `web_street_number` STRING,
//   `web_street_name` STRING,
//   `web_street_type` STRING,
//   `web_suite_number` STRING,
//   `web_city` STRING,
//   `web_county` STRING,
//   `web_state` STRING,
//   `web_zip` STRING,
//   `web_country` STRING,
//   `web_gmt_offset` DECIMAL(5,2),
//   `web_tax_percentage` DECIMAL(5,2))
// USING text
// LOCATION '###LOC###/web_site'"""
// )

// COMMAND ----------

// DBTITLE 1,Schemas for TPC- DS and H
val tableColumnSchemas = Map(
"dbgen_version" -> """
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            time                          ,
    dv_cmdline_args           varchar(200)                  
""",
"call_center" -> """
    cc_call_center_sk         integer               not null,
    cc_call_center_id         char(16)              not null,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
""",
"catalog_page" -> """
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
""",
"catalog_returns" -> """
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           bigint                not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  
""",
"catalog_sales" -> """
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           bigint                not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  
""",
"customer" -> """
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date_sk     integer                       
""",
"customer_address" -> """
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      
""",
"customer_demographics" -> """
    cd_demo_sk                integer               not null,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       
""",
"date_dim" -> """
    d_date_sk                 integer               not null,
    d_date_id                 char(16)              not null,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       
""",
"household_demographics" -> """
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
""",

"income_band" -> """
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
""",
"inventory" -> """
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       
""",
"item" -> """
    i_item_sk                 integer               not null,
    i_item_id                 char(16)              not null,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      
""",
"promotion" -> """
    p_promo_sk                integer               not null,
    p_promo_id                char(16)              not null,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       
""",
"reason" -> """
    r_reason_sk               integer               not null,
    r_reason_id               char(16)              not null,
    r_reason_desc             char(100)                     
""",
"ship_mode" -> """
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
""",
"store" -> """
    s_store_sk                integer               not null,
    s_store_id                char(16)              not null,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  
""",
"store_returns" -> """
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          bigint                not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  
""",

"store_sales" -> """
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          bigint                not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  
""",
"time_dim" -> """
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
""",
"warehouse" -> """
    w_warehouse_sk            integer               not null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
""",
"web_page" -> """
    wp_web_page_sk            integer               not null,
    wp_web_page_id            char(16)              not null,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       
""",
"web_returns" -> """
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           bigint                not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  
""",
"web_sales" -> """
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           bigint                not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  
""",
"web_site" -> """
    web_site_sk               integer               not null,
    web_site_id               char(16)              not null,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  
"""
)

//TPC-H
val tpchTableColumnSchemas = Map(
"customer" -> """
        c_custkey BIGINT,
        c_name VARCHAR(25),
        c_address VARCHAR(40),
        c_nationkey BIGINT,
        c_phone CHAR(15),
        c_acctbal DECIMAL(18,2),
        c_mktsegment CHAR(10),
        c_comment VARCHAR(117)
""",
"lineitem" -> """
        l_orderkey BIGINT,
        l_partkey BIGINT,
        l_suppkey BIGINT,
        l_linenumber INTEGER,
        l_quantity DECIMAL(18,2),
        l_extendedprice DECIMAL(18,2),
        l_discount DECIMAL(18,2),
        l_tax DECIMAL(18,2),
        l_returnflag CHAR(1),
        l_linestatus CHAR(1),
        l_shipdate DATE,
        l_commitdate DATE,
        l_receiptdate DATE,
        l_shipinstruct CHAR(25),
        l_shipmode CHAR(10),
        l_comment VARCHAR(44)
""",
"nation" -> """
        n_nationkey BIGINT,
        n_name CHAR(25),
        n_regionkey BIGINT,
        n_comment VARCHAR(152)
""",
"orders" -> """
        o_orderkey BIGINT,
        o_custkey BIGINT,
        o_orderstatus CHAR(1),
        o_totalprice DECIMAL(18,2),
        o_orderdate DATE,
        o_orderpriority CHAR(15),
        o_clerk CHAR(15),
        o_shippriority INTEGER,
        o_comment VARCHAR(79)
""",
"part" -> """
        p_partkey BIGINT,
        p_name VARCHAR(55),
        p_mfgr CHAR(25),
        p_brand CHAR(10),
        p_type VARCHAR(25),
        p_size INTEGER,
        p_container CHAR(10),
        p_retailprice DECIMAL(18,2),
        p_comment VARCHAR(23)
""",
"partsupp" -> """
        ps_partkey BIGINT,
        ps_suppkey BIGINT,
        ps_availqty INTEGER,
        ps_supplycost DECIMAL(18,2),
        ps_comment VARCHAR(199)
""",
"region" -> """
        r_regionkey BIGINT,
        r_name CHAR(25),
        r_comment VARCHAR(152)
""",
"supplier" -> """
        s_suppkey BIGINT,
        s_name CHAR(25),
        s_address VARCHAR(40),
        s_nationkey BIGINT,
        s_phone CHAR(15),
        s_acctbal DECIMAL(18,2),
        s_comment VARCHAR(101)
"""
)

def getBenchmarkColumns(benchmark: String) = benchmark match {
  case "TPCDS" => tableColumnSchemas
  case "TPCH" => tpchTableColumnSchemas
  case _ => throw new Exception(s"Invalid benchmark $benchmark")
}

// COMMAND ----------

// DBTITLE 1,Partition keys for TPC- DS and H
val tablePartitionKeys = Map(
    "dbgen_version" -> Seq(""),
    "call_center" -> Seq(""),
    "catalog_page" -> Seq(""),
    "catalog_returns" -> Seq("cr_returned_date_sk"),
    "catalog_sales" -> Seq("cs_sold_date_sk"),
    "customer" -> Seq(""),
    "customer_address" -> Seq(""),
    "customer_demographics" -> Seq(""),
    "date_dim" -> Seq(""),
    "household_demographics" -> Seq(""),
    "income_band" -> Seq(""),
    "inventory" -> Seq("inv_date_sk"),
    "item" -> Seq(""),
    "promotion" -> Seq(""),
    "reason" -> Seq(""),
    "ship_mode" -> Seq(""),
    "store" -> Seq(""),
    "store_returns" -> Seq("sr_returned_date_sk"),
    "store_sales" -> Seq("ss_sold_date_sk"),
    "time_dim" -> Seq(""),
    "warehouse" -> Seq(""),
    "web_page" -> Seq(""),
    "web_returns" -> Seq("wr_returned_date_sk"),
    "web_sales" -> Seq("ws_sold_date_sk"),
    "web_site" -> Seq("")
)

val tpchTablePartitionKeys = Map(
  "customer" -> Seq("c_mktsegment"),  
  "lineitem" -> Seq("l_shipdate"),
  "nation" -> Seq(""),
  "orders" -> Seq("o_orderdate"),
  "part" -> Seq(""),
  "partsupp" -> Seq(""),
  "region" -> Seq(""),
  "supplier" -> Seq("")
)

def getBenchmarkPartitions(benchmark: String) = benchmark match {
  case "TPCDS" => tablePartitionKeys
  case "TPCH" => tpchTablePartitionKeys
  case _ => throw new Exception(s"Invalid benchmark $benchmark")
}

// COMMAND ----------

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.immutable.Stream
import scala.sys.process._

/**
 * Using ProcessBuilder.lineStream produces a stream, that uses
 * a LinkedBlockingQueue with a default capacity of Integer.MAX_VALUE.
 *
 * This causes OOM if the consumer cannot keep up with the producer.
 *
 * See scala.sys.process.ProcessBuilderImpl.lineStream
 */
object BlockingLineStream {
  // See scala.sys.process.Streamed
  private final class BlockingStreamed[T](
    val process:   T => Unit,
    val    done: Int => Unit,
    val  stream:  () => Stream[T]
  )

  // See scala.sys.process.Streamed
  private object BlockingStreamed {
    // scala.process.sys.Streamed uses default of Integer.MAX_VALUE,
    // which causes OOMs if the consumer cannot keep up with producer.
    val maxQueueSize = 65536

    def apply[T](nonzeroException: Boolean): BlockingStreamed[T] = {
      val q = new LinkedBlockingQueue[Either[Int, T]](maxQueueSize)

      def next(): Stream[T] = q.take match {
        case Left(0) => Stream.empty
        case Left(code) =>
          if (nonzeroException) scala.sys.error("Nonzero exit code: " + code) else Stream.empty
        case Right(s) => Stream.cons(s, next())
      }

      new BlockingStreamed((s: T) => q put Right(s), code => q put Left(code), () => next())
    }
  }

  // See scala.sys.process.ProcessImpl.Spawn
  private object Spawn {
    def apply(f: => Unit): Thread = apply(f, daemon = false)
    def apply(f: => Unit, daemon: Boolean): Thread = {
      val thread = new Thread() { override def run() = { f } }
      thread.setDaemon(daemon)
      thread.start()
      thread
    }
  }

  def apply(command: Seq[String]): Stream[String] = {
    val streamed = BlockingStreamed[String](true)
    val process = command.run(BasicIO(false, streamed.process, None))
    Spawn(streamed.done(process.exitValue()))
    streamed.stream()
  }
}

// COMMAND ----------

// DBTITLE 1,[DEBUG] Uncoment for fast testing
// def getBenchmarkTables(benchmark: String) = benchmark match {
//  case "TPCDS" => Seq("reason") 
//  case "TPCH" => Seq("nation") 
// }

// COMMAND ----------

// DBTITLE 1,Settings for datagens and database creation
val partitions = workers * cores * 2
val dsdgen = s"${baseDatagenFolder}/dsdgen/dsdgen"
val dbgen = s"${baseDatagenFolder}/dbgen/dbgen"

//val convertToRows = false
//val convertToSchema = false

def createDatabase(databaseName: String, location: String) = {
  if (onlyTextFiles) {
    log(s"Only generating text files to $location/$databaseName")
  } else {
    log(s"Creating schema $databaseName at $location")            
    if(overwrite) time(s"drop-cascade_$databaseName", sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE"))
    sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION '$location'")
    sql(s"USE $databaseName")            
  }
}

def checkBin(fp: String) = {
  if (new java.io.File(fp).exists) {
    true
  } else {
    sys.error(s"Could not find the file at $fp. Check the tool builder above")
  }  
}

def tpcdsCmd(tableName: String, scaleFactor: String, part: Int) = {
  checkBin(dsdgen)
  val localToolsDir = s"$baseDatagenFolder/dsdgen"
  // Note: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100.
  val parallel = if (partitions > 1) s"-parallel $partitions -child $part" else ""
  val commands = Seq(
    "bash", "-c",
    s"cd $localToolsDir && ./dsdgen -table $tableName -filter Y -scale $scaleFactor -RNGSEED 100 $parallel")  
  commands
}

val smallTpchTables = Seq("nation", "region")

def tpchCmd(tableName: String, scaleFactor: String, part: Int) = {
  checkBin(dbgen)
  val localToolsDir = s"$baseDatagenFolder/dbgen"
  val shortTableNames = Map(
    "customer" -> "c",
    "lineitem" -> "L",
    "nation" -> "n",
    "orders" -> "O",
    "part" -> "P",
    "region" -> "r",
    "supplier" -> "s",
    "partsupp" -> "S"
  )
  val parallel = if (partitions > 1 && !smallTpchTables.contains(tableName)) s"-C $partitions -S $part" else ""
  val commands = Seq(
    "bash", "-c",
    s"cd $localToolsDir && ./dbgen -T ${shortTableNames(tableName)} -s $scaleFactor $parallel")
  commands
}

def tpcCmd(benchmark: String, tableName: String, scaleFactor: String, part: Int) = benchmark match {
  case "TPCDS" => tpcdsCmd(tableName, scaleFactor, part)
  case "TPCH" => tpchCmd(tableName, scaleFactor, part)
  case _ => throw new Exception(s"Benchmark $benchmark not supported.")  
}

// COMMAND ----------

// DBTITLE 1,File writers
import org.apache.spark.sql._

def writeText(df: DataFrame, tableName: String, location: String) = {
  df.write
    .mode(if (overwrite) "overwrite" else "ignore")
    .option("compression", textCompression) //none, bzip2, gzip, lz4, snappy and deflate
    .option("lineSep", "\n")
    .text(s"$location/$tableName")
}

// Duplicate TPCDS column to be compatible with older queries (pre v2.10)
def addExtraColumn(df: DataFrame, benchmark: String, tableName: String) = {
  if (benchmark == "TPCDS" && tableName == "customer")
    df.withColumn("c_last_review_date", $"c_last_review_date_sk")
  else
    df
}

def writeFormat(ds: Dataset[String], benchmark: String, tableName: String, location: String) = {
  val csvTable = spark.read
    .option("delimiter", "|")
    .option("sep", "|")
    .option("header", "false")
    .option("emptyValue", "")
    .option("charset", "iso-8859-1")
    .option("dateFormat", "yyyy-MM-dd")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss[.SSS]") // -- spec: yyyy-mm-dd hh:mm:ss.s
    .option("mode", "PERMISSIVE")
    .option("multiLine", "false")
    .option("locale", "en-US")
    .option("lineSep", "\n")        
    .schema(getBenchmarkColumns(benchmark)(tableName) + ", last_col string")
    .csv(ds)
    .drop("last_col")

  val csvTableOut = addExtraColumn(csvTable, benchmark, tableName)
  
  if (!partitionTables || getBenchmarkPartitions(benchmark)(tableName)(0).isEmpty) {
    // Unpartitioned delta tables
    if (Seq("delta", "tahoe").contains(fileFormat)) {
    csvTableOut.write
      .format(fileFormat)
      .mode(if (overwrite) "overwrite" else "ignore")
      .option("path", s"$location/$tableName")
      .option("overwriteSchema", "true")      
      .saveAsTable(tableName)      
    } else {
      // For other formats, coalesce to produce fewer files
      csvTableOut
        .coalesce(coalesceInto)
        .write
        .format(fileFormat)
        .mode(if (overwrite) "overwrite" else "ignore")
        .option("path", s"$location/$tableName")
        .option("overwriteSchema", "true")      
        .saveAsTable(tableName)       
    }
  } else if (distributeStrategy == "none") {
    // Delta tables uses the optimized writer  
    csvTableOut.write
      .format(fileFormat)
      .mode(if (overwrite) "overwrite" else "ignore")
      .option("path", s"$location/$tableName")
      .option("overwriteSchema", "true")
      .partitionBy(getBenchmarkPartitions(benchmark)(tableName):_*)      
      .saveAsTable(tableName)        
  } else {
    // Parquet, orc, etc need a repartition
    csvTableOut
      .repartition(col(getBenchmarkPartitions(benchmark)(tableName).head))
      .write
      .format(fileFormat)
      .mode(if (overwrite) "overwrite" else "ignore")
      .option("path", s"$location/$tableName")
      .option("overwriteSchema", "true")
      .partitionBy(getBenchmarkPartitions(benchmark)(tableName):_*)      
      .saveAsTable(tableName)
  }
}

def createTable(benchmark: String, tableName: String, path: String) = {
  val partitionedBy = 
    if (!partitionTables || getBenchmarkPartitions(benchmark)(tableName)(0).isEmpty) "" 
    else "PARTITIONED BY " + getBenchmarkPartitions(benchmark)(tableName).mkString(", ")
  // (${getBenchmarkColumns(benchmark)(tableName)})  
  val ct = s"""CREATE TABLE IF NOT EXISTS $tableName USING $fileFormat LOCATION '$path'"""
  println(ct)
  sql(ct)
  // recover the partitions if not delta and partitioned
  if (!Seq("delta", "tahoe").contains(fileFormat) && partitionTables && !getBenchmarkPartitions(benchmark)(tableName)(0).isEmpty)
    sql(s"MSCK REPAIR TABLE $tableName")
}

// Retry-once function to recover from file exists errros (ie., on don't overwirte)
// AnalysisException: Cannot create table t. The associated location l is not empty.;
def writeWithReties[R](fn: => R, retries: Int = 1, benchmark: String, tableName: String, path: String) = {
  var tries = retries
  while(tries >= 0) {
    try{
      fn
      tries = -1
    } catch {
      case e: AnalysisException if tries > 1 && overwrite == false => {        
        println(s"Failed writing into table. Retries available $tries. Error: " + e.getMessage)
        tries = tries - 1
        println(s"Attmpting to import table at: $path")
        createTable(benchmark, tableName, path)
      }      
      case e: AnalysisException if tries == 1 || overwrite == true => {        
        println(s"Failed writing into table. Retries available $tries. Error: " + e.getMessage)
        tries = tries - 1
        println(s"Forcing location delete: $path")
        dbutils.fs.rm(path, true)
      }
      case e: Throwable if tries < 1 => throw e
    }
  }  
}

// COMMAND ----------

// DBTITLE 1,Save text files using datafremes and text writer (if set)
import spark.implicits._ //needed for rdd to dataset conversion

scaleFactors.foreach { scaleFactor => {
  setScaleConfig(scaleFactor) // To prevent OOMs
  benchmarks.foreach { benchmark => { 
    val outputFormat = if (onlyTextFiles) "text" else fileFormat
    time(s"datagen_${benchmark}_${scaleFactor}_${outputFormat}", {
      val (databaseName, location) = getNameLocation(benchmark, scaleFactor)
      createDatabase(databaseName, location)
      for (tableName <- getBenchmarkTables(benchmark)) {        
        time(s"datagen_${benchmark}_${scaleFactor}_${outputFormat}_${tableName}", {
          val generatedData = {
            val parts = if (benchmark == "TPCH" && smallTpchTables.contains(tableName)) 1 else partitions
            sc.parallelize(1 to parts, parts).flatMap { part =>
              val commands = tpcCmd(benchmark, tableName, scaleFactor, part)
              println(commands)
              BlockingLineStream(commands)
            }
          }
          val jobName = s"$benchmark $tableName sf=$scaleFactor"
          generatedData.setName(jobName)
          sc.setJobGroup(jobName, "")
          
          if (onlyTextFiles) {
            writeText(generatedData.toDF, tableName, location)
          } else {
            writeWithReties(
              writeFormat(generatedData.toDS, benchmark, tableName, location), 
              2, 
              benchmark, tableName, s"$location/$tableName"
            )
          }
      })
    }
  })
}}
}}

// COMMAND ----------

// DBTITLE 1,Print delta table details (files and sizes)
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

if (!skipChecks) {
  var tableDetails = scala.collection.mutable.ListBuffer.empty[DataFrame]

  if (Seq("delta", "tahoe").contains(fileFormat)) {
    scaleFactors.foreach { scaleFactor =>
      benchmarks.foreach{ benchmark => {
        val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
        sql(s"use $dbname")
        println(s"Printing table information for $dbname SF $scaleFactor")
        val dbDetails = 
          sql(s"show tables").select("tableName").collect().map{ tableName =>        
            val name: String = tableName.toString().drop(1).dropRight(1)
            sql(s"describe detail $name")
              .withColumn("tableName", lit(name))
          }.reduce(_ union _)
          .drop("format", "id", "description", "properties", "createdAt", "lastModified",  "minReaderVersion", "minWriterVersion")
          .withColumn("scaleFactor", lit(scaleFactor))
          .withColumn("dbName", lit(dbname))
        dbDetails.show(999, false)
        tableDetails += dbDetails
        }
        println
      }
    }
    display(tableDetails.reduce(_ union _)
           .select("dbName", "tableName", "scaleFactor", "name", "location", "partitionColumns", "numFiles", "sizeInBytes")
    )
  }
}

// COMMAND ----------

// DBTITLE 1,Debug print files
scaleFactors.foreach { scaleFactor => {
  benchmarks.foreach { benchmark => { 
      val (databaseName, location) = getNameLocation(benchmark, scaleFactor)
    scala.util.Try(dbutils.fs.ls(location).foreach{ t => 
      println(t.name)
      if (t.name.takeRight(1) == "/") scala.util.Try(dbutils.fs.ls(t.path).foreach{ p => 
        println(s"\t${p.name}\t${p.size}")
        if (p.name.takeRight(1) == "/") scala.util.Try(dbutils.fs.ls(p.path).foreach{ f => 
          println(s"\t\t${f.name}\t${f.size}")
        })
      })
    })    
  }}
}}

// COMMAND ----------

// DBTITLE 1,Only delta_log
scaleFactors.foreach { scaleFactor => {
  benchmarks.foreach { benchmark => { 
    val (databaseName, location) = getNameLocation(benchmark, scaleFactor)
    scala.util.Try(dbutils.fs.ls(location).foreach{ t => 
      println(t.name)
      if (t.name.takeRight(1) == "/" || t.name == "_delta_log/") scala.util.Try(dbutils.fs.ls(t.path).foreach{ p => 
        println(s"\t${p.name}\t${p.size}")
        if (p.name.takeRight(1) == "/" && p.name == "_delta_log/") scala.util.Try(dbutils.fs.ls(p.path).foreach{ f => 
          println(s"\t\t${f.name}\t${f.size}")
        })
      })
    })    
  }}
}}

// COMMAND ----------

// DBTITLE 1,Stop here if only generating the source files
if (onlyTextFiles) dbutils.notebook.exit("Generating source text files into $location complete")

// COMMAND ----------

// DBTITLE 1,Row counts and partiton info
if (!skipChecks) {
  var tableStats = scala.collection.mutable.ListBuffer.empty[DataFrame]

  scaleFactors.foreach { scaleFactor =>
    benchmarks.foreach{ benchmark => {
      val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
      sql(s"use $dbname")
      println(s"Printing table details for $dbname SF $scaleFactor")
      val tableDetails = getTables(dbname).map{ tableName =>
        //println(s"Printing table information for table $tableName")
        val numRows: Long = sql(s"select count(*) as ${tableName}_count from $tableName").collect()(0).getLong(0)
        val numPartitions: Long = scala.util.Try(sql(s"SHOW PARTITIONS $tableName").count()).map(_.toLong).getOrElse(0L)
        println(s"Table $tableName has $numPartitions partitions and $numRows rows.")
        (tableName, numPartitions, numRows)
      }.toSeq.toDF("tableName", "numPartitions", "rowCount")
         .withColumn("dbName", lit(dbname))
         .withColumn("scaleFactor", lit(scaleFactor))
      //dbDetails.show(999, false)
      tableStats += tableDetails
      }
      println
    }
  }
  display(
    tableStats.reduce(_ union _)
      .select("dbName", "tableName", "scaleFactor", "numPartitions", "rowCount")
  )
}


// COMMAND ----------

// DBTITLE 1,Optimize delta tables
// Generate the data, import the tables, generate stats for selected benchmarks and scale factors
if (!skipOptimize && Seq("delta", "tahoe").contains(fileFormat)) {
  // when overwritting, force re-optimize  
  if (overwrite) spark.conf.set("spark.databricks.delta.optimize.zorder.mergeStrategy", "all")
  scaleFactors.foreach { scaleFactor => {  
  // First set some config settings affecting OOMs/performance
  setScaleConfig(scaleFactor)
  
  benchmarks.foreach{ benchmark => {
    
    val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
    println(s"\nDB $dbname from $location")
    sql(s"use $dbname")
    println(s"\nOptimizing DB $dbname")
    if (benchmark == "TPCDS") {
      if (partitionTables) {
        val queries = Array(
      "optimize call_center zorder by(cc_call_center_sk)",
      "optimize catalog_page zorder by(cp_catalog_page_sk)",
      "optimize catalog_returns zorder by(cr_item_sk)",
      "optimize customer zorder by(c_customer_sk)",
      "optimize customer_address zorder by(ca_address_sk)",
      "optimize customer_demographics zorder by(cd_demo_sk)",
      "optimize household_demographics zorder by(hd_demo_sk)",
      "optimize income_band zorder by(ib_income_band_sk)",
      "optimize inventory zorder by(inv_item_sk)",
      "optimize item zorder by(i_item_sk)",
      "optimize promotion zorder by(p_promo_sk)",
      "optimize reason zorder by(r_reason_sk)",
      "optimize store zorder by(s_store_sk)",
      "optimize store_returns zorder by(sr_item_sk)",
      "optimize store_sales zorder by(ss_item_sk)",
      "optimize time_dim zorder by(t_time_sk)",
      "optimize warehouse zorder by(w_warehouse_sk)",
      "optimize web_page zorder by(wp_web_page_sk)",
      "optimize web_returns zorder by(wr_item_sk)",
      "optimize web_sales zorder by(ws_item_sk)",
      "optimize catalog_sales zorder by(cs_item_sk)",        
      "optimize web_site zorder by(web_site_sk)"
          ).foreach { query =>
            println(s"Running $query")
            time(s"optimize_${benchmark}_${scaleFactor}_${fileFormat}_$query", sql(query)) 
          }
      } else {
        val queries = Array(
      "optimize call_center zorder by(cc_call_center_sk)",
      "optimize catalog_page zorder by(cp_catalog_page_sk)",
      "optimize catalog_returns zorder by(cr_returned_date_sk, cr_item_sk)",
      "optimize catalog_sales zorder by(cs_sold_date_sk, cs_item_sk)",
      "optimize customer zorder by(c_customer_sk)",
      "optimize customer_address zorder by(ca_address_sk)",
      "optimize customer_demographics zorder by(cd_demo_sk)",
      "optimize date_dim zorder by(d_date_sk)",
      "optimize household_demographics zorder by(hd_demo_sk)",
      "optimize income_band zorder by(ib_income_band_sk)",
      "optimize inventory zorder by(inv_date_sk, inv_item_sk)",
      "optimize item zorder by(i_item_sk)",
      "optimize promotion zorder by(p_promo_sk)",
      "optimize reason zorder by(r_reason_sk)",
      "optimize store zorder by(s_store_sk)",
      "optimize store_returns zorder by(sr_returned_date_sk, sr_item_sk)",
      "optimize store_sales zorder by(ss_sold_date_sk, ss_item_sk)",
      "optimize time_dim zorder by(t_time_sk)",
      "optimize warehouse zorder by(w_warehouse_sk)",
      "optimize web_page zorder by(wp_web_page_sk)",
      "optimize web_returns zorder by(wr_returned_date_sk, wr_item_sk)",
      "optimize web_sales zorder by(ws_sold_date_sk, ws_item_sk)",
      "optimize web_site zorder by(web_site_sk)"
          ).foreach { query =>
            println(s"Running $query")
            time(s"optimize_${benchmark}_${scaleFactor}_${fileFormat}_$query", sql(query).show(false)) 
          }
      }
     } else {  // TPCH 
      time(s"optimize_${benchmark}_${scaleFactor}_${fileFormat}", {
        sql(s"show tables").select("tableName").collect().foreach{ tableName =>        
          val name: String = tableName.toString().drop(1).dropRight(1)
          println(s"Optimizing table $name")
          scala.util.Try(sql(s"optimize $name").show(false))
        } 
      })              
     }
    }}
    }}
}


// COMMAND ----------

// DBTITLE 1,Create table and column stats (if set)
// Generate the data, import the tables, generate stats for selected benchmarks and scale factors
scaleFactors.foreach { scaleFactor => {
  
  // First set some config settings affecting OOMs/performance
  setScaleConfig(scaleFactor)
  
  benchmarks.foreach{ benchmark => {
    val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
     println(s"\nDB $dbname from $location")

    if (createTableStats) time(s"create-stats_${benchmark}_${scaleFactor}_${fileFormat}", {
      getTables(dbname).map{ tableName => 
       println(s"Creating table stats for $benchmark $scaleFactor table $tableName")
       time(s"create-table-stats_${benchmark}_${scaleFactor}_${fileFormat}_$tableName", sql(s"ANALYZE TABLE $dbname.$tableName COMPUTE STATISTICS"))
       println(s"Creating column stats for $benchmark $scaleFactor table $tableName")
       time(s"create-column-stats_${benchmark}_${scaleFactor}_${fileFormat}_$tableName", sql(s"ANALYZE TABLE $dbname.$tableName COMPUTE STATISTICS FOR ALL COLUMNS"))
      }
    })
  }}
}}

// COMMAND ----------

// DBTITLE 1,VACUUM
if (!skipChecks) {
  spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")  
  scaleFactors.foreach { scaleFactor =>
    benchmarks.foreach{ benchmark => {
      val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
      sql(s"use $dbname")
      time(s"vacuum_${benchmark}_${scaleFactor}_${fileFormat}", {
        sql(s"show tables").select("tableName").collect().foreach{ tableName =>        
          val name: String = tableName.toString().drop(1).dropRight(1)
          println(s"Vacuuming information for $benchmark SF $scaleFactor table $name at $location/$name")
          scala.util.Try(sql(s"VACUUM delta.`$location/$name` RETAIN 0 HOURS"))
        } 
      })
      println
    }}
  }
}

// COMMAND ----------

// DBTITLE 1,Checkpoining
import com.databricks.sql.transaction.tahoe._

if (!skipChecks) {
  if (Seq("delta", "tahoe").contains(fileFormat)) {
    scaleFactors.foreach { scaleFactor =>
      benchmarks.foreach{ benchmark => {
        val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
        sql(s"use $dbname")
        time(s"checkpointing_${benchmark}_${scaleFactor}_${fileFormat}", {
          sql(s"show tables").select("tableName").collect().foreach{ tableName =>        
            val name: String = tableName.toString().drop(1).dropRight(1)
            println(s"Checkpointing table $name at $location")
            DeltaLog.forTable(spark, s"$location/$name").checkpoint()
          } 
        })
        println
      }}
    }  
  }
}

// COMMAND ----------

// DBTITLE 1,Check column stats (Slow)
if (!skipChecks) {
  var columnStats = scala.collection.mutable.ListBuffer.empty[DataFrame]

  scaleFactors.foreach { scaleFactor =>
    benchmarks.foreach{ benchmark => {
      val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
      sql(s"use $dbname")
      println(s"Getting column details for $dbname SF $scaleFactor")
      val columnDetails = getTables(dbname).par.map{ tableName =>
        val tableStats = getColumns(dbname, tableName).par.map{ colName =>
          val describeColumn = sql(s"describe extended $tableName $colName")
            .groupBy().pivot($"info_name").agg(first($"info_value")) // traspose      
          //describeColumn.show
          describeColumn
        }.reduce(_ union _)
         .withColumn("tableName", lit(tableName))
        tableStats.show
        tableStats
      }.reduce(_ union _)
       .withColumn("dbName", lit(dbname))
       .withColumn("scaleFactor", lit(scaleFactor))
       .select("dbName", "tableName", "scaleFactor", "col_name", "data_type", "avg_col_len",
               "distinct_count", "max", "max_col_len", "min", "num_nulls", "histogram", "comment")
      //dbDetails.show(999, false)
      columnStats += columnDetails
      }
      println
    }
  }
  display(columnStats.reduce(_ union _))
}


// COMMAND ----------

// DBTITLE 1,Delta table history (only works for 1 type bechmark)
if (!skipChecks) {
  var tableDetails = scala.collection.mutable.ListBuffer.empty[DataFrame]

  if (Seq("delta", "tahoe").contains(fileFormat)) {
    scaleFactors.foreach { scaleFactor =>
      benchmarks.take(1).foreach{ benchmark => {
        val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
        sql(s"use $dbname")
        println(s"Printing table history for $dbname SF $scaleFactor")
        val dbDetails = 
          getTables(dbname).par.map{ tableName =>        
            sql(s"describe history $tableName")
              .withColumn("tableName", lit(tableName))
          }.reduce(_ union _)
          .withColumn("scaleFactor", lit(scaleFactor))
          .withColumn("dbName", lit(dbname))
        dbDetails.show(999, false)
        tableDetails += dbDetails
        }
        println
      }
    }

    display(tableDetails.reduce(_ union _)
           .select("dbName", "tableName", "scaleFactor", "version", "timestamp", "userId", "userName", "operation", 
                   "operationParameters", "job", "notebook", "clusterId", "readVersion", "isolationLevel", "isBlindAppend", "operationMetrics")
    )
  }
}

// COMMAND ----------

// DBTITLE 1,Debug print files
scaleFactors.foreach { scaleFactor => {
  benchmarks.foreach { benchmark => { 
    val (databaseName, location) = getNameLocation(benchmark, scaleFactor)
    scala.util.Try(dbutils.fs.ls(location).foreach{ t => 
      println(t.name)
      if (t.name.takeRight(1) == "/") scala.util.Try(dbutils.fs.ls(t.path).foreach{ p => 
        println(s"\t${p.name}\t${p.size}")
        if (p.name.takeRight(1) == "/") scala.util.Try(dbutils.fs.ls(p.path).foreach{ f => 
          println(s"\t\t${f.name}\t${f.size}")
        })
      })
    })    
  }}
}}

// COMMAND ----------

// DBTITLE 1,Only delta_log
scaleFactors.foreach { scaleFactor => {
  benchmarks.foreach { benchmark => { 
    val (databaseName, location) = getNameLocation(benchmark, scaleFactor)
    scala.util.Try(dbutils.fs.ls(location).foreach{ t => 
      println(t.name)
      if (t.name.takeRight(1) == "/" || t.name == "_delta_log/") scala.util.Try(dbutils.fs.ls(t.path).foreach{ p => 
        println(s"\t${p.name}\t${p.size}")
        if (p.name.takeRight(1) == "/" && p.name == "_delta_log/") scala.util.Try(dbutils.fs.ls(p.path).foreach{ f => 
          println(s"\t\t${f.name}\t${f.size}")
        })
      })
    })    
  }}
}}

// COMMAND ----------

// DBTITLE 1,Show only the last operation per table (to simplify verification)
// Dissabled as it can fail in spark 3 with: command-13849:3: error: missing argument list for method count in trait TraversableForwarder
// Unapplied methods are only converted to functions when a function type is expected.
// You can make this conversion explicit by writing `count _` or `count(_)` instead of `count`.
// if (Seq("delta", "tahoe").contains(fileFormat) && tableDetails.count > 0) {

// import org.apache.spark.sql.expressions.Window

// if (Seq("delta", "tahoe").contains(fileFormat) && tableDetails.count > 0) {
//   val tableDetailsJoined = tableDetails.reduce(_ union _)
//     .select("dbName", "tableName", "scaleFactor", "version", "timestamp", "userId", "userName", "operation", 
//             "operationParameters", "job", "notebook", "clusterId", "readVersion", "isolationLevel", "isBlindAppend", "operationMetrics")

//   val lastOperation = Window.partitionBy($"scaleFactor", $"dbName", $"tableName").orderBy(desc("timestamp"))

//   display(
//     tableDetailsJoined.withColumn("operationOrderDesc", row_number.over(lastOperation))
//       .filter($"operationOrderDesc".isin("1"))
//       .sort($"scaleFactor", $"dbName", $"tableName", $"operationOrderDesc")
//   )
// }

// COMMAND ----------

// DBTITLE 1,Print delta table details (files and sizes)
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._

if (!skipChecks) {
  var tableDetails = scala.collection.mutable.ListBuffer.empty[DataFrame]

  if (Seq("delta", "tahoe").contains(fileFormat)) {
    scaleFactors.foreach { scaleFactor =>
      benchmarks.foreach{ benchmark => {
        val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
        sql(s"use $dbname")
        println(s"Printing table information for $dbname SF $scaleFactor")
        val dbDetails = 
          sql(s"show tables").select("tableName").collect().map{ tableName =>        
            val name: String = tableName.toString().drop(1).dropRight(1)
            sql(s"describe detail $name")
              .withColumn("tableName", lit(name))
          }.reduce(_ union _)
          .drop("format", "id", "description", "properties", "createdAt", "lastModified",  "minReaderVersion", "minWriterVersion")
          .withColumn("scaleFactor", lit(scaleFactor))
          .withColumn("dbName", lit(dbname))
        dbDetails.show(999, false)
        tableDetails += dbDetails
        }
        println
      }
    }
    display(tableDetails.reduce(_ union _)
           .select("dbName", "tableName", "scaleFactor", "name", "location", "partitionColumns", "numFiles", "sizeInBytes")
    )
  }
}

// COMMAND ----------

// DBTITLE 1,Delta stats
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.expressions._
// import org.apache.spark.sql.types._

// import com.databricks.sql.transaction.tahoe._

// var tableDetails = scala.collection.mutable.ListBuffer.empty[DataFrame]

// if (Seq("delta", "tahoe").contains(fileFormat)) {
//   scaleFactors.foreach { scaleFactor =>
//     benchmarks.foreach{ benchmark => {
//       val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
//       sql(s"use $dbname")
//       println(s"Getting table information for $dbname SF $scaleFactor at $location")      
//       val dbDetails = 
//       getTables(dbname).map{ name =>        
//           com.databricks.sql.transaction.tahoe.DeltaLog.forTable(spark,  s"$location/$name")
//             .snapshot.withStats
//             .withColumn("partitionJSON", to_json($"partitionValues"))
//             .withColumn("statsJSON", to_json($"stats"))
//             .withColumn("tagsJSON", to_json($"tags"))            
//             .withColumn("tableName", lit(name))
//             .withColumn("fileNumRecords", $"stats".getItem("numRecords"))
//             .withColumn("minValuesCols", size(array($"stats.minValues.*")))
//             .withColumn("maxValuesCols", size(array($"stats.maxValues.*")))
//             .withColumn("nullValuesCols", size(array($"stats.nullCount.*")))        
//             .drop("partitionValues", "stats", "tags") // to prevent errors on different structs
//         }.reduce(_ unionByName _)
//         .withColumn("scaleFactor", lit(scaleFactor))
//         .withColumn("dbName", lit(dbname))

//       //dbDetails.show(5, false)
//       tableDetails += dbDetails
//       }
//       println
//     }
//   }
//   val results = tableDetails.reduce(_ union _)
//     .select($"dbName", $"tableName", $"scaleFactor", 
//             $"path", $"partitionJSON", $"size", 
//             $"modificationTime", $"dataChange", $"tagsJSON", 
//             $"fileNumRecords", $"minValuesCols", $"maxValuesCols",   $"nullValuesCols"            
//             //$"stats",            
//             //$"statsJSON"
//            )
  
//   //results.write.format("csv").option("header", "true").save("/mnt/performance-results/tmp/100TB_table_info.csv")
            
//   display(results)  
// }

// COMMAND ----------

// DBTITLE 1,Print full table details
if (!skipChecks) {
  // Print table structure for manual validation
  scaleFactors.foreach { scaleFactor =>
    benchmarks.foreach{ benchmark => {
      val (dbname, location) = getBenchmarkData(benchmark, scaleFactor)
      println(s"Printing table information for $dbname SF $scaleFactor at $location")
      sql(s"use $dbname")
      time(s"describe_${benchmark}_${scaleFactor}_${fileFormat}", {
        sql(s"show tables").select("tableName").collect().foreach{ tableName =>        
          val name: String = tableName.toString().drop(1).dropRight(1)
          val count = sql(s"select count(*) as ${name}_count from $name").collect()(0)(0)
          println(s"Table $name has " + scala.util.Try(sql(s"SHOW PARTITIONS $name").count() + " partitions").getOrElse(s"no partitions") + s" and $count rows.")
          sql(s"describe extended $name").show(999, false)
        } 
      })
      println
    }}
  }
}

// COMMAND ----------

// DBTITLE 1,Print out test times
val results = timings.toSeq.toDF("test", "time_ms")    
    .withColumn("category", split($"test", "_")(0))
    .select ($"test", $"category", $"time_ms")

display(results)

