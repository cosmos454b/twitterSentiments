package com.yogini.twitter.analyzer

object test {
   def main(args : Array[String]) {
     var tweet = new Tweet("1","Apr-20","the jungle book was Very Good. The little kid was not a good actor i was just there for billy murry and, scarlett the johanson as a snek");
     val a = new Analyzer();
     val t: List[Tweet] = List(tweet)
     val res = a.calculate(t,"jungle book".split("\\s"));
     println(res(0).content,res(0).polarity);
     
     
     /*val c = new TweeterConnection();
     c.search("Narendra Modi").foreach(println);*/
   }
}