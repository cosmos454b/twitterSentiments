package com.yogini.twitter.analyzer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.SparkContext._
import edu.stanford.nlp.ling.Sentence
import edu.stanford.nlp.ling.TaggedWord
import edu.stanford.nlp.ling.HasWord
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import scala.collection.JavaConverters._

class Analyzer extends java.io.Serializable {
  
  object Tagger{
     val tagger = new MaxentTagger("data/models/english-bidirectional-distsim.tagger");
      val posTag = {
          (sentence: String) =>
        tagger.tagString(sentence)
     }
  }
  

  def calculate(args: List[Tweet], exclusion: Array[String]): Array[PolarizedTweet] = {

    val conf = new SparkConf()
      .setAppName("Twitter sentiment analysis")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");


    def normalize(tweet: Tweet): Array[Tuple2[String, String]] = {
      val normal = Tagger.posTag(tweet.content.toLowerCase()).split("\\s")
        .filter({ word: String => word.endsWith("_JJ") })
        .map({ word: String =>
          (word.replace("_JJ", "")
            .replace("'", "")
            .replace(".", "")
            .replace(":", "")
            .replace("!", "")
            .replace("`", "")
            .replace(";", "")
            .replace("*", ""), tweet.tweetId)
        });
      return normal;
    }

    def readTwitterData(args: List[Tweet]): rdd.RDD[Tweet] = {
      return sc.makeRDD(args);
    }

    def readSentinialDict(): rdd.RDD[Dictionary] = {
      return sc.textFile("data/SentiWordNet_Processed.txt").map({ line: String => line.split(",") }).map({ terms: Array[String] => new Dictionary(terms(0), terms(1).toFloat) });
    }

    val data: rdd.RDD[Tweet] = readTwitterData(args);
    
    
    val processed = data.map(normalize).map({ arr => arr.toList }).flatMap(identity);
    val dict: rdd.RDD[Dictionary] = readSentinialDict();

    val dictGrped = dict.keyBy { _.term };

    val joined: rdd.RDD[(String, Float)] = dictGrped.join(processed).map({ case (term: String, mapped: Tuple2[Dictionary, String]) => (mapped._2, mapped._1.netScore) });
    //joined.foreach(println)

    val netScore = joined.reduceByKey(_ + _);

    val joinBackToTweet = data.keyBy { _.tweetId }.join(netScore);

    val finalRDD = joinBackToTweet.map({ case (id, record: Tuple2[Tweet, Float]) => new PolarizedTweet(record._1.timestamp, record._1.content, record._2) });
    val result = finalRDD.collect();
    sc.stop();
    return result;
  }

  def aggregatePolarity(tweets: Array[PolarizedTweet]): Tuple4[Float, Float, Float, Float] = {
    val excited = tweets.filter { tweet => tweet.polarity >= 1 }.map({ tweet => tweet.polarity }).foldLeft(0.0F)(_ + _);
    val happy = tweets.filter { tweet => tweet.polarity >= 0 && tweet.polarity < 1 }.map({ tweet => (tweet.polarity) }).foldLeft(0.0F)(_ + _);
    val upset = tweets.filter { tweet => tweet.polarity < 0 && tweet.polarity >= -1 }.map({ tweet => (tweet.polarity.abs) }).foldLeft(0.0F)(_ + _);
    val angry = tweets.filter { tweet => tweet.polarity < -1 }.map({ tweet => (tweet.polarity.abs) }).foldLeft(0.0F)(_ + _);
    println(excited, happy, upset, angry);
    val total = excited + happy + upset + angry;
    return (excited / total, happy / total, upset / total, angry / total);
  }

  def totalPolarity(tweets: Array[PolarizedTweet]): Tuple2[Float, Float] = {
    val positive = tweets.filter { tweet => tweet.polarity > 0 }.map({ tweet => tweet.polarity }).foldLeft(0.0F)(_ + _);
    val negative = tweets.filter { tweet => tweet.polarity < 0 }.map({ tweet => tweet.polarity.abs }).foldLeft(0.0F)(_ + _);
    return (positive, negative);
  }

}