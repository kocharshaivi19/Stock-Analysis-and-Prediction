package com.nyu.bigData
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Performance {

  def setConfig(): (SparkContext, SQLContext) = {
    val conf = new SparkConf().setAppName("StockPerformance").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    (sc, sqlContext)
  }
  def performanceMeasure(sc: SparkContext, sqlContext: SQLContext, name: String): Unit = {
    val filename = name.split(".").toSeq
    val business = sqlContext.read
      .format("csv")
      .option("header", "true")
      .load(System.getProperty("perf.dir") + "/" + filename(0) + "_fundamentals.csv")

    business.toDF().registerTempTable("new")

    val spark_Data = sqlContext.sql("Select `Cash and Cash Equivalents`/`Total Current Liabilities` As Cash_Ratio, `Total Current Assets`/`Total Current Liabilities` AS Current_ratio, `Long-Term Debt`/`Total Equity` As Debt_Ratio, `Net Cash Flow`/`Long-Term Debt` AS CashbyDebtRatio, `Ticker Symbol` from new ")

    spark_Data.toDF().registerTempTable("required_data")

    val bad_performance = sqlContext.sql("Select * from required_data WHERE Debt_Ratio >=2 AND Cash_Ratio < 1 AND Current_Ratio < 1 AND CashbyDebtRatio < 1")

    val good_performance = sqlContext.sql("Select * from required_data WHERE Debt_Ratio >=2 AND Cash_Ratio < 1 AND Current_Ratio < 1 AND CashbyDebtRatio < 1")

    bad_performance.coalesce(1).write.format("csv")
      .save(System.getProperty("perf.dir") + "/" + filename(0) + "_badPerfData.csv")

    good_performance.coalesce(1).write.format("csv")
      .save(System.getProperty("perf.dir") + "/" + filename(0) + "_goodPerfData.csv")

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("perf.dir", new java.io.File(".").getCanonicalPath + "/main/perf/")
    val (sc, sqlContext) = setConfig()
    val companycashtags = Seq("FB.csv", "AMZN.csv", "SWN.csv")
    for (ct <- companycashtags) {
      performanceMeasure(sc, sqlContext, ct)
    }
  }
}
