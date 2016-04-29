package com.yogini.twitter.analyzer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd
import org.apache.spark.SparkContext._

object SentinetProcessor {
  
  val conf = new SparkConf()
      .setAppName("Twitter sentiment analysis")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")

      
    def calculate()= {
         
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");

    def average( ts: Iterable[(String,Float)]):Float = {
       println(ts.toList);
       val sum:Float = ts.filter({case(term,score)=>score!=0}).map({case(term,score)=>score}).foldLeft(0.0F)(_+_);
       val fin = sum /ts.size.toFloat;
       if(fin>=0.5){
         return 1;
       }else if(fin>0 && fin<0.5){
         return 0.5F;
       }else if(fin<0 && fin > -0.5){
         return -0.5F;
       }else if(fin <= -0.5){
         return -1;
       }
       return 0;
    }
    
    val regex = "[0-9]".r  
    val data = sc.textFile("data/SentiWordNet.txt")
                     .map {line:String => line.split("\t")}
                     .map({terms:Array[String]=>(terms(0),terms(1).toFloat,terms(2).toFloat,terms(3))})
                     .map({case(id:String,ps:Float,ns:Float,terms:String)=>terms.split("#").map({alias:String=>if (alias.length()>2) Some(id,ps,ns,regex.replaceAllIn(alias,"").trim()) else None})});
                
    val data_filtered = data.map({rec:Array[Option[Tuple4[String,Float,Float,String]]]=>rec.toList.filter( _.isDefined ).flatten}).flatMap(identity);
    val grped= data_filtered.map({case(id:String,ps:Float,ns:Float,term:String)=>(term,ps-ns)}).groupBy(_._1).map(x => (x._1, average(x._2)))
    grped.filter({case(term:String,score:Float)=>score!=0}).filter({case(term:String,score:Float)=>term.length()>1}).sortBy(_._1).map({case(term:String,score:Float)=>term+","+score}).saveAsTextFile("data/SentiWordNet_Processed.txt");
   }
  
   def main(args : Array[String]) {
    calculate();
   }
}