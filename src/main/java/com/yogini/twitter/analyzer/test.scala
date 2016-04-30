package com.yogini.twitter.analyzer

object test {
   def main(args : Array[String]) {
     var tweet = new Tweet("1","Apr-20","The only thing keeping me going right now is the fact I get to see the Jungle Book tomorrow. ONLY THING.");
     val a = new Analyzer();
     val t: List[Tweet] = List(tweet)
     val res = a.calculate(t,"jungle book".split("\\s"));
     println(res(0).content,res(0).polarity);
     
     
     /*val c = new TweeterConnection();
     c.search("Narendra Modi").foreach(println);*/
   }
}