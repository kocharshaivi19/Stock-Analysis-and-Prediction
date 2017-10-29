package com.nyu.bigData
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark._
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.IndexedSeq


class OBV extends UserDefinedAggregateFunction  {
  // This is the input fields for your aggregate function.
  override def inputSchema: StructType = StructType(

    StructField("volume", LongType) ::
      StructField("close-yc", DoubleType) :: Nil
  )


  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(

    StructField("previous_obv", LongType) :: Nil
    //org.apache.spark.sql.types.StructField("product", org.apache.spark.sql.types.DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) =  0L // buffer.getAs[Long](0)
    print("buffer(0)", buffer(0))
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: sql.Row): Unit = {
    buffer(0) = if (input.getAs[Double](1) > 0) buffer.getAs[Long](0) + input.getAs[Long](0) else buffer.getAs[Long](0) - input.getAs[Long](0)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)

  }
  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Long](0)
  }
}

object prediction {
  def getYahooFinData(sc: SparkContext, sqlContext: SQLContext, name: String): Unit = {
    import sqlContext.implicits._
    val filename = name.split(".").toSeq
    val fin_data = sqlContext.read
      .format("csv")
      .option("header", "true")
      .load(System.getProperty("pred.dir") + "/" + name)

    val wSpec1 = Window.orderBy("Date")
    val fin_data1  =  fin_data.withColumn("YC",lag(fin_data("Close"), 1).over(wSpec1))
    val fin_data2  =  fin_data1.withColumn("TDAC", lag(fin_data1("Close"), 10).over(wSpec1))

    val wSpec2 = Window.orderBy("Date").rowsBetween(-4,0)
    val wSpec3 = Window.orderBy("Date").rowsBetween(-9,0)
    val fin_data3  =  fin_data2.withColumn( "Lowest Low", min(fin_data2("Low")).over(wSpec2))
    val fin_data4  =  fin_data3.withColumn( "Highest High", max(fin_data3("High")).over(wSpec2))
    val fin_data5  =  fin_data4.withColumn( "MA", avg(fin_data4("Close")).over(wSpec3))

    val filteredRdd1 = fin_data5.rdd.zipWithIndex().collect { case (r, i) if i >9   => r }

    val newDf1 = sqlContext.createDataFrame(filteredRdd1, fin_data5.schema)
    val filteredRdd = newDf1.rdd.zipWithIndex().collect { case (r, i) if i <400  => r }
    val newDf = sqlContext.createDataFrame(filteredRdd, fin_data5.schema)

    val fin_data6 = newDf.withColumn("Volume1", newDf("Volume").cast(org.apache.spark.sql.types.LongType))
      .drop("Volume")
      .withColumnRenamed("Volume1", "Volume")
    val fin_data7  =  fin_data6.withColumn( "Close-YC", newDf("Close")-newDf("YC"))

    val fin_data8 = fin_data7.withColumn("TC-YC bool", when($"Close-YC" > 0, 1).otherwise(0))
      .drop("Symbol")
      .drop("Open")
      .drop("Adj_Close")

    val fin_data9 = fin_data8.withColumn( "upcl",  when ($"TC-YC bool" < 1, 0 ).otherwise($"Close"))
    val fin_data10 = fin_data9.withColumn("dwcl", when ($"TC-YC bool" > 0, 0).otherwise($"Close"))
    val wSpec4 = Window.orderBy("Date").rowsBetween(-8, 0)
    val fin_data11 = fin_data10.withColumn( "upclose",sum(fin_data10("upcl")).over(wSpec4))
    val fin_data12 = fin_data11.withColumn( "downclose",sum(fin_data10("dwcl")).over(wSpec4))
    val fin_data13 = fin_data12.drop("Lowest Low").drop("Highest High")
    val filteredRdd2 = fin_data13.rdd.zipWithIndex().collect { case (r, i) if i >0   => r }

    val fin_data14 = sqlContext.createDataFrame(filteredRdd2, fin_data13.schema)
    val fin_data15  =  fin_data14.withColumn( "RSIration", fin_data14("upclose")/fin_data14("downclose"))

    val fin_data16 = fin_data15.withColumn("1+", fin_data15("YC") - fin_data15("YC") + 1)
    val fin_data17 = fin_data16.drop("High").drop("Low").drop("Close").drop("TDAC").drop("MA")
    val fin_data18 = fin_data17.withColumn("10+", fin_data17("1+")/(fin_data17("RSIration")+fin_data17("1+")))
    val fin_data19 = fin_data18.withColumn("100", fin_data15("YC") - fin_data15("YC") + 100)
    val fin_data20 = fin_data19.withColumn("RSI", fin_data19("100") - (fin_data19("100")*fin_data19("10+")))
    val fin_data21 = fin_data20.drop("RSIration").drop("1+").drop("10+").drop("100")
    val fin_data22 = fin_data21.withColumn("RSI_pred", when($"RSI" > 50, 1).otherwise(0)).drop("YC").drop("Volume")
      .drop("Close-YC").drop("TC-YC bool").drop("upcl").drop("dwcl").drop("upclose").drop("downclose").drop("50")

    val fin_data_PMO = fin_data2.drop("Adj_Close").drop("High").drop("Low").drop("Open").drop("Symbol").drop("Volume").drop("YC")

    val filteredRdd4 = fin_data_PMO.rdd.zipWithIndex().collect { case (r, i) if i >10 => r }

    val fin_data_PMO1 = sqlContext.createDataFrame(filteredRdd4, fin_data_PMO.schema)

    val filteredRdd5 = fin_data_PMO1.rdd.zipWithIndex().collect{case (r, i) if i <400 => r}
    val fin_data_PMO2 = sqlContext.createDataFrame(filteredRdd5, fin_data_PMO1.schema)
    val fin_data_PMO3 = fin_data_PMO2.withColumn("TC- TDAC", fin_data_PMO2("Close") - fin_data_PMO2("TDAC") )
    val fin_data_PMO4 = fin_data_PMO3.withColumn("PMO_pred", when($"TC- TDAC" > 0, 1).otherwise(0))

    val fin_data_k = newDf.withColumn("HN-LN", newDf("Highest High") - newDf("Lowest Low"))
    val fin_data_k1 = fin_data_k.withColumn("TC- LN", fin_data_k("Close") - fin_data_k("Lowest Low"))
      .drop("Adj_Close").drop("High").drop("Low").drop("Open").drop("Symbol").drop("Volume").drop("YC").drop("TDAC").drop("MA")
    val fin_data_k2 = fin_data_k1.withColumn("ratio", fin_data_k1("TC- LN")/ fin_data_k1("HN-LN"))
    val fin_data_k3 = fin_data_k2.withColumn("100", fin_data_k2("Close") - fin_data_k2("Close")+ 100)
    val fin_data_k4 = fin_data_k3.withColumn("K", fin_data_k3("ratio")*fin_data_k3("100"))
    val fin_data_k5 = fin_data_k4.withColumn("K_pred", when($"K"> 80, 1).otherwise(0)).drop("Close").drop("Lowest Low").drop("Highest High").drop("HN-LN").drop("TC- LN").drop("ratio").drop("100" )

    val OBV_ = new OBV()
    val winspec5 = Window.orderBy("Date")
    val fin_data_OBV = fin_data7.withColumn("OBV", OBV_(fin_data7.col("Volume"), fin_data7.col("Close-YC")).over(winspec5))
    val fin_data_OBV1  =  fin_data_OBV.withColumn("lagOBV",lag(fin_data_OBV("OBV").cast(org.apache.spark.sql.types.LongType), 1).over(wSpec1))
      .drop("Adj_Close").drop("High").drop("Low").drop("Open").drop("Symbol")

    val filteredRdd6 = fin_data_OBV1.rdd.zipWithIndex().collect { case (r, i) if i >0   => r }

    val fin_data_OBV2 = sqlContext.createDataFrame(filteredRdd6, fin_data_OBV1.schema)
    val fin_data_OBV3 = fin_data_OBV2.withColumn("delOBV", fin_data_OBV2("OBV")- fin_data_OBV2("lagOBV"))
    val fin_data_OBV4 = fin_data_OBV3.withColumn("OBV_pred", when($"delOBV"> 0, 1).otherwise(0))

    val fin_data_MA = fin_data_OBV.withColumn("lagOBV",lag(fin_data_OBV("OBV").cast(org.apache.spark.sql.types.LongType), 1).over(wSpec1))
    val fin_data_MA1 = fin_data_MA.drop("Adj_Close").drop("Close").drop("High").drop("Open")
      .drop("Low").drop("Symbol").drop("YC").drop("TDAC").drop("Lowest Low").drop("Highest High").drop("Volume").drop("Close-YC").drop("OBV").drop("lagOBV")
    val fin_data_MA2  =  fin_data_MA1.withColumn("lagMA",lag(fin_data_MA1("MA"), 1).over(wSpec1))

    val filteredRdd7 = fin_data_MA2.rdd.zipWithIndex().collect { case (r, i) if i >0   => r }

    val fin_data_MA3 = sqlContext.createDataFrame(filteredRdd7, fin_data_MA2.schema)
    val fin_data_MA4= fin_data_MA3.withColumn("delMA", fin_data_MA3("MA") - fin_data_MA3("lagMA"))
    val fin_data_MA5 = fin_data_MA4.withColumn("MA_pred", when($"delMA"> 0, 1).otherwise(0)).drop("lagMA").drop("delMA")

    val training_table1 = fin_data_OBV3.withColumn("OBV_pred", when($"delOBV"> 0, 1).otherwise(0))
    val training_table2 = training_table1.drop("TDAC").drop("Lowest Low").drop("Highest High")
      .drop("MA").drop("lagOBV").drop("delOBV").drop("Close")

    val df = training_table2.join(fin_data_PMO4, Seq("Date")).orderBy("Date")
    val df1 = df.join(fin_data22, Seq("Date")).orderBy("Date")
    val df2 = df1.join(fin_data_k5, Seq("Date")).orderBy("Date")
    val df3 = df2.join(fin_data_MA5, Seq("Date")).orderBy("Date").drop("TC- TDAC").drop("YC")

    val output1 = df3.withColumn("p", df3("OBV_pred")+df3("PMO_pred")+ df3("RSI_pred")+ df3("K_pred")+ df3("MA_pred"))
    val output2 = output1.withColumn("p_", when($"p" > 4, 1).otherwise(when($"p" < 2, -1).otherwise(0)))
    val output3 = output2.withColumn("label", when($"Close-YC">0 , 1).otherwise(-1))
    val output4 =  output3.withColumn("spike", output3("p_")*output3("label"))
    val output5 = output4.drop("Close-YC").drop("Volume").drop("RSI").drop("K").drop("MA")
    val output6 = output5.withColumn("correct", when($"spike"> 0, 1).otherwise(0))
    val output7 = output6.withColumn("error", when($"spike"<0, -1).otherwise(0))

    val train_data = fin_data_MA.drop("High").drop("Low").drop("Symbol").drop("Close-YC").drop("lagOBV")
    train_data.show()

    val output5 = output4.drop("Volume").drop("Close-YC").drop("OBV").drop("MA").drop("Close")
      .drop("TDAC").drop("PMO_pred").drop("K_pred").drop("MA_pred").drop("p").drop("p_").drop("label")
      .drop("spike").drop("RSI_pred").drop("OBV_pred")

    val train_data1 = train_data.join(output5, Seq("Date")).orderBy("Date")
    val filteredRdd7 = train_data1.rdd.zipWithIndex().collect { case (r, i) if i >5   => r }

    val train_data2 = sqlContext.createDataFrame(filteredRdd7, train_data1.schema)

    output7.coalesce(1).write.format("csv")
      .save(System.getProperty("pred.dir") + "/" + filename(0) + "_priceIndex.csv")

    train_data2.coalesce(1).write.format("csv")
      .save(System.getProperty("pred.dir") + "/" + filename(0) + "_trainingData.csv")
  }

  def trainLR(sc: SparkContext, sqlContext: SQLContext, name: String): Unit = {
    val filename = name.split(".").toSeq
    val data = sc.textFile( System.getProperty("pred.dir") + "/" + filename(0) + "_trainingData.csv/*.csv")
    val parsedData = data.map{ x =>
      val parts = x.split(",")
      LabeledPoint(parts(2).toDouble, Vectors.dense(parts(1).toDouble/10, parts(3).toDouble/10,
        parts(4).toDouble/10, parts(5).toDouble/10, parts(6).toDouble/10, parts(7).toDouble/10,
        parts(8).toDouble/10 ,parts(9).toDouble/1000000, parts(10).toDouble/10000000,
        parts(11).toDouble/10, parts(12).toDouble/10))
    }.cache()

    val numIterations = 10000000
    val stepSize = 0.01
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
    model.save(sc, System.getProperty("pred.dir") + "/" + filename(0) + "_scalaLinearRegressionWithSGDModel")
  }

  def testLR(sc: SparkContext, sqlContext: SQLContext, parsedData: RDD[LabeledPoint], name: String): Unit = {
    import sqlContext.implicits._
    val filename = name.split(".")
    val model = LinearRegressionModel.load(sc, System.getProperty("pred.dir") + "/" + filename(0) + "_scalaLinearRegressionWithSGDModel")
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    valuesAndPreds.take(10)
    valuesAndPreds.toDF().coalesce(1).write.format("csv")
      .save( System.getProperty("pred.dir") + "/" + filename(0) + "_predictions.csv")
  }

  def setConfig(): (SparkContext, SQLContext) = {
    val conf = new SparkConf().setAppName("StockPrediction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    (sc, sqlContext)
  }


  def main(args: Array[String]): Unit = {
    System.setProperty("pred.dir", new java.io.File(".").getCanonicalPath + "/main/pred/")
    val (sc, sqlContext) = setConfig()
    val companycashtags = Seq("FB.csv", "AMZN.csv", "SWN.csv")
    for (ct <- companycashtags){
      getYahooFinData(sc, sqlContext, ct)
      trainLR(sc, sqlContext, ct)
      testLR(sc, sqlContext, ct)
    }
  }
}
