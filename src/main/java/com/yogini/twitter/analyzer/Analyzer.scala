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
class Analyzer {
   val conf = new SparkConf()
      .setAppName("Twitter sentiment analysis")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")

    
    def calculate(args : List[Tweet], exclusion: Array[String]):Array[PolarizedTweet]= {
         
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
   
    def readTwitterData(args : List[Tweet]):rdd.RDD[Tweet]={       
         return sc.makeRDD(args); 
    }
     
    val someFunc = {(sentence: String) =>   val tagger = new MaxentTagger("data/models/english-bidirectional-distsim.tagger");tagger.tagString(sentence)}
    
    def readSentinialDict():rdd.RDD[Dictionary]={
        return sc.textFile("data/SentiWordNet_Processed.txt").map({line:String => line.split(",")}).map({terms:Array[String]=>new Dictionary(terms(0),terms(1).toFloat)});
    }
    
    
    val data:rdd.RDD[Tweet]= readTwitterData(args);
    val dict:rdd.RDD[Dictionary] = readSentinialDict();
    data.map { sentence => someFunc(sentence.content)}.foreach {println};
    val wordsMapper = data.map({tweet:Tweet=>tweet.content.split("\\s").map({charArr:String=>(charArr,tweet.tweetId)}).toList}).flatMap(identity);
    
    val filtered = wordsMapper.filter({case(word:String,id:String) => !word.startsWith("http")}).filter({case(word:String,id:String) => !exclusion.contains(word.toLowerCase())});
    val processed = filtered.map({case(word:String,id:String) => (word.replace("'",""),id)}).map({case(word:String,id:String) => (word.replace(".",""),id)}).map({case(word:String,id:String) => (word.replace(":",""),id)}).map({case(word:String,id:String) => (word.replace("!",""),id)}).map({case(word:String,id:String) => (word.toLowerCase(),id)});
    val processed_distinct = processed.distinct();
    
    val dictGrped = dict.keyBy {_.term};
    val joined:rdd.RDD[(String, Float)] = dictGrped.join(processed_distinct).map({case(term:String,mapped:Tuple2[Dictionary,String])=>(mapped._2,mapped._1.netScore)});
    joined.foreach(println)
    val netScore = joined.reduceByKey(_+_);
   
    val joinBackToTweet = data.keyBy {_.tweetId}.join(netScore);
    val finalRDD = joinBackToTweet.map({case(id,record:Tuple2[Tweet,Float])=>new PolarizedTweet(record._1.timestamp,record._1.content,record._2)});
    val result = finalRDD.collect();
    sc.stop();
    return result;
  }
    
    def aggregatePolarity(tweets : Array[PolarizedTweet]):Tuple4[Float,Float,Float,Float] = {
      val excited = tweets.filter { tweet => tweet.polarity >= 1 }.map ({ tweet => tweet.polarity}).foldLeft(0.0F)(_+_);
      val happy = tweets.filter { tweet => tweet.polarity >= 0 && tweet.polarity < 1}.map ({ tweet => (tweet.polarity)}).foldLeft(0.0F)(_+_);
      val upset = tweets.filter { tweet => tweet.polarity < 0 && tweet.polarity >= -1 }.map ({ tweet => (tweet.polarity.abs)}).foldLeft(0.0F)(_+_);
      val angry = tweets.filter { tweet => tweet.polarity < -1 }.map ({ tweet => (tweet.polarity.abs)}).foldLeft(0.0F)(_+_);
      println(excited,happy,upset,angry);
      val total=excited+happy+upset+angry;
      return (excited/total,happy/total,upset/total,angry/total);
    }
    
     def totalPolarity(tweets : Array[PolarizedTweet]):Tuple2[Float,Float] = {
      val positive = tweets.filter { tweet => tweet.polarity >0 }.map ({ tweet => tweet.polarity}).foldLeft(0.0F)(_+_);
      val negative = tweets.filter { tweet => tweet.polarity <0 }.map ({ tweet => tweet.polarity.abs}).foldLeft(0.0F)(_+_);
      return (positive,negative);
    }
  
}