package com.yogini.twitter.analyzer

import org.scalatra._
import java.net.URL
import org.scalatra.scalate.ScalateSupport


class Servlet extends ScalatraServlet with ScalateSupport {

  get("/") {
     var searchWord = request.getParameter("word");
     if(searchWord==null || searchWord.length()<2){
       searchWord = "India";
     }
     val conn = new TweeterConnection();
     val tweets:List[Tweet] = conn.search(searchWord);
     val analyzer = new Analyzer();
     val result = analyzer.calculate(tweets,searchWord.toLowerCase().split("\\s"));
     val polarityCounts = analyzer.aggregatePolarity(result);
     val positivePolarity =  (polarityCounts._1+polarityCounts._2)*100
     contentType="text/html"
     ssp("views/index.ssp","result"->result,"searchWord"->searchWord,"polarityCounts"->polarityCounts,"positivePolarity"->positivePolarity)
  }
  
}

