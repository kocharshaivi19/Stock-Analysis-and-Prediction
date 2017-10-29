package com.nyu.bigData

/**
  * Created by shaivi on 23/4/17.
  */

import java.awt.Dimension
import java.io.File
import java.nio.charset.{Charset, CodingErrorAction}

import com.cybozu.labs.langdetect.DetectorFactory
import com.kennycason.kumo.bg.PixelBoundryBackground
import com.kennycason.kumo.{CollisionMode, PolarWordCloud}
import com.kennycason.kumo.font.{FontWeight, KumoFont}
import com.kennycason.kumo.font.scale.LinearFontScalar
import com.kennycason.kumo.nlp.FrequencyAnalyzer
import com.nyu.bigData.util.SentimentAnalysisUtils._
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.JsonDSL._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.unix_timestamp

import collection.mutable._
import collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

object TwitterSentimentAnalysis {

  def setConfig(ESresource: IndexedSeq[String]): (SparkContext, StreamingContext, SQLContext) = {
    val conf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local")

    conf.set("es.resource", ESresource(0))
    conf.set("es.nodes", ESresource(1))
    conf.set("es.port", ESresource(2))
    conf.set("es.index.auto.create","true")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))
    val sqlContext = new SQLContext(sc)

    (sc, ssc, sqlContext)
  }

  def grabTweets(sc: SparkContext, ssc: StreamingContext, sqlContext: SQLContext,
                 filters: IndexedSeq[String], ESresource: IndexedSeq[String], profileFilename: String,
                 configFilename: String){
    DetectorFactory.loadProfile(profileFilename)
    val twitterConfig = ConfigFactory.parseFile(new File(configFilename))
    val consumerKey = twitterConfig.getString("Twitter.secret.consumerKey")
    val consumerSecret = twitterConfig.getString("Twitter.secret.consumerSecret")
    val accessToken = twitterConfig.getString("Twitter.secret.accessToken")
    val accessTokenSecret = twitterConfig.getString("Twitter.secret.accessTokenSecret")

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // grab tweets
    val tweets = TwitterUtils.createStream(ssc, None, filters)

    val tweetMap = tweets.map(status => {
      val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      val tweetMap = ("user" -> status.getUser.getScreenName) ~
        ("createdAt" -> formatter.format(status.getCreatedAt.getTime)) ~ {
        if (status.getGeoLocation != null)
          ("latitude" -> status.getGeoLocation.getLatitude) ~ ("longitude" -> status.getGeoLocation.getLongitude)
        else
          ("latitude" -> "") ~ ("Longitude" -> "")
        } ~
        ("text" -> status.getText)
        ("retweet" -> status.getRetweetCount) ~
        ("sentiment" -> detectSentiment(status.getText).toString) ~
        ("hashtags" -> status.getHashtagEntities.map(_.getText).toList)

    })
    tweetMap.print
    tweetMap.map(s => List("Tweet Extracted")).print

    // Each batch is saved to Elasticsearch with StatusCreatedAt as the default time dimension
    tweetMap.foreachRDD { t => EsSpark.saveToEs(t, ESresource(0),
      Map("es.mapping.timestamp" -> "createdAt"))}
    ssc.start()
    ssc.awaitTermination()
  }

  def getElasticsearchData(sc: SparkContext, sqlContext: SQLContext,
                            find: String, ESresource: Seq[String], stopfile: String): DataFrame = {

    import sqlContext.implicits._
    val es_data = sqlContext.read.format("org.elasticsearch.spark.sql").load(ESresource(0))
    es_data.registerTempTable("es_data")

    val company = sqlContext.sql(s"""SELECT * FROM es_data where filter = "$find"""")
    company.registerTempTable("company")

    val ts = unix_timestamp($"created_at","EEE MMM dd HH:mm:ss ZZZ yyyy").cast("timestamp")
    val time_grasp = company.withColumn("timestamp", ts)
    time_grasp.registerTempTable("time_grasp")

    val time_interval = sqlContext.sql(s"""Select created_at, title, retweet_count, sentiment, from_unixtime(floor(unix_timestamp(timestamp)/3600)*3600) as timeslice from time_grasp where timestamp>=cast('2017-05-01' as date) and timestamp<cast('2017-05-11' as date)""")

    time_interval.toDF()
  }

  def detectLanguage(text: String) : String = {
    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

  }

  def plotSentiments(sqlContext: SQLContext, time_interval: DataFrame, find:String): Unit ={
    time_interval.registerTempTable("time_interval")
    val path = System.getProperty("analysis.dir")
    val sentiment_count = sqlContext.sql("select sentiment, count(*) as sentimentcount, timeslice from time_interval group by sentiment, timeslice")
    sentiment_count.map(x => x.mkString(",")).coalesce(1).saveAsTextFile(path + find + "_sentiment.csv")
  }

  def plotWordCloud(sqlContext: SQLContext, time_interval: DataFrame, find: String, stopfile: String): Unit ={
    time_interval.registerTempTable("time_interval")

    val getPosText = sqlContext.sql(s"""select title from time_interval where sentiment="positive"""")

    val getNegText = sqlContext.sql(s"""select title from time_interval where sentiment="negative"""")

    val getNeuText = sqlContext.sql(s"""select title from time_interval where sentiment="neutral"""")

    val negative = getNegText.select("title").rdd.flatMap(r => r(0).asInstanceOf[String].split(" ")).collect().toList
    val positive = getPosText.select("title").rdd.flatMap(r => r(0).asInstanceOf[String].split(" ")).collect().toList
    val neutral = getNeuText.select("title").rdd.flatMap(r => r(0).asInstanceOf[String].split(" ")).collect().toList

    if (negative.take(1).length == 1 && positive.take(1).length == 1)
      return

    val decoder = Charset.forName("UTF-8").newDecoder()

    decoder.onMalformedInput(CodingErrorAction.IGNORE)

    val pos = positive.map(x => x.replaceAll("\\p{C}", "?"))
    val neg = negative.map(x => x.replaceAll("\\p{C}", "?"))
    val neu = neutral.map(x => x.replaceAll("\\p{C}", "?"))

    plotWhale(pos, neg, find, stopfile)
  }

  def plotWhale(pos: List[String], neg: List[String], find:String, stopfile: String): Unit = {
    val path = System.getProperty("analysis.dir")
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val listOfLines = Source.fromFile(stopfile)(decoder).getLines.toSet
    val frequencyAnalyzer = new FrequencyAnalyzer
    frequencyAnalyzer.setWordFrequenciesToReturn(600)
    frequencyAnalyzer.setMinWordLength(4)
    frequencyAnalyzer.setStopWords(listOfLines.asJava)

    val wordFrequencies = frequencyAnalyzer.load(pos.asJava)
    val wordFrequencies2 = frequencyAnalyzer.load(neg.asJava)

    val dimension = new Dimension(1600, 924)
    val wordCloud = new PolarWordCloud(dimension, CollisionMode.PIXEL_PERFECT)
    wordCloud.setPadding(2)

    wordCloud.setBackgroundColor(java.awt.Color.WHITE)
    wordCloud.setKumoFont(new KumoFont("Impact", FontWeight.PLAIN))
    wordCloud.setBackground(new PixelBoundryBackground(path + "background.jpg"))
    wordCloud.setFontScalar(new LinearFontScalar(30, 80))
    wordCloud.build(wordFrequencies, wordFrequencies2)
    wordCloud.writeToFile(path + find + "_wordcloud.png")
  }

  def main(args: Array[String]): Unit = {
    // Replace ESresource with below line if you need Localhost Elasticsearch
    // val ESresource = Seq("sparksender/tweets", "localhost", "9200")
    val ESresource = IndexedSeq("sparksender/cashtags_filter", "AWS_ES_PATH", "443")

    System.setProperty("resource.dir", new java.io.File(".").getCanonicalPath + "/main/resource/")
    System.setProperty("analysis.dir", new java.io.File(".").getCanonicalPath + "/main/analysis/")

    val profileFilename = System.getProperty("resource.dir") + "profiles"
    val configFilename = System.getProperty("resource.dir") + "twitter_cred.conf"
    val stopFile = System.getProperty("resource.dir") + "stopwords.txt"

    val filters = IndexedSeq("$SNAP", "$AAPL", "$FB", "$CERN", "$SPX")
    val (sc, ssc, sqlContext) = setConfig(ESresource)

    grabTweets(sc, ssc, sqlContext, filters, ESresource, profileFilename, configFilename)

    for (fil <- filters) {
      val time_interval_data = getElasticsearchData(sc, sqlContext, fil, ESresource, stopFile)
      plotSentiments(sqlContext, time_interval_data, fil)
      plotWordCloud(sqlContext, time_interval_data, fil, stopFile)
    }
  }
}
