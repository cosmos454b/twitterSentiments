package com.yogini.twitter.analyzer

import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken
import twitter4j.Trends
import twitter4j.Trend
import twitter4j.Query
import twitter4j.QueryResult
import twitter4j.Status
import twitter4j.Place
import scala.collection.JavaConversions._

class TweeterConnection{
  
  val access_key="2808413395-NPd9YmV7PfSacGTfFZIB0uJjCF6spKar7Rfh4Z9";
  val access_token="FajJVYUd3oTLdOeTD14tb6zawJcERGwIIC15Nlkzp6aJ1";
  val consumer_key="7StWFQlriMB0PWrp7MhrrYiid";
  val consumer_token="5FPZAdE7xCsO0jHzBBpgJOX5GeqQvhmGyUwFc8zX1iTlKflJDd";
  
  val us_woeid=23424977;
  val uk_woeid=23424975;
  val india=23424848;

  val woeid_maps = Map("US"->23424977,"uk"->23424975,"india"->23424848);
  
  
  def main(args : Array[String]) {
  val twitter = new TwitterFactory().getInstance();
 
  twitter.setOAuthConsumer(consumer_key,consumer_token);
  twitter.setOAuthAccessToken(new AccessToken(access_key,access_token));
  
  val statuses = twitter.getPlaceTrends(23424977);
  statuses.getTrends.foreach { trend:Trend => println(trend.toString())};
 
  /*for (Status status : result.getTweets()) {
        System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
  }*/
  }
  
  def search(word:String):List[Tweet] = {
     val format = new java.text.SimpleDateFormat("MMM-d")
     val twitter = new TwitterFactory().getInstance();
     twitter.setOAuthConsumer(consumer_key,consumer_token);
     twitter.setOAuthAccessToken(new AccessToken(access_key,access_token));
     val query = new Query(word+ " -filter:retweets");
     query.setCount(100);
   
     val result = twitter.search(query);
     val tweets: java.util.List[Status] = result.getTweets();
     val resultSet= tweets.map { tweet => new Tweet(String.valueOf(tweet.getId),format.format(tweet.getCreatedAt),tweet.getText())}.toList;
     return resultSet;
  }
  
  
}