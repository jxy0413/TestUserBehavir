package com.atguigu.gmall0105.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
  * Created by jxy on 2020/12/14 0014 18:24
  */
object MyEsUtil {
    //相当于连接池
    var factory : JestClientFactory=null;

    def getClient:JestClient={
      if(factory==null)build();
         factory.getObject
    }

    def  build(): Unit ={
     factory=new JestClientFactory
     factory.setHttpClientConfig(new HttpClientConfig.Builder("http://Worker1:9200" )
       .multiThreaded(true)
       .maxTotalConnection(20)
       .connTimeout(10000).readTimeout(1000).build())
    }

    def addDoc():Unit = {
       val jest = getClient
       //builder设计模式
       //可转化为json对象
       //HashMap
       val index = new Index.Builder(Movie0105("0104","大话西游","西游记")).
         index("movie_test_20200619").`type`("_doc").id("0104").build()
       val message = jest.execute(index).getErrorMessage
       if(message!=null){
         println(message)
       }
       jest.close()
    }

    def bulkDoc(sourceList:List[Any],indexName:String):Unit = {
     // println(sourceList.size+"kk")
     if(sourceList!=null&&sourceList.size>0){
       val jest = getClient
       val bulkBuilder = new Bulk.Builder
       //jest.execute(bulk)
       for(source<- sourceList){
         val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
         bulkBuilder.addAction(index)
       }
       val result = jest.execute(bulkBuilder.build())
       val items = result.getItems
       println("保存到ES: "+items.size()+"条数")
       jest.close()
      }
   }

     //查询
     def queryDoc:Unit = {
        val jest = getClient
        val searchSourceBuilder = new SearchSourceBuilder()
        val booleanQueryBuilder = new BoolQueryBuilder
        booleanQueryBuilder.must(new MatchQueryBuilder("name","red"))
        booleanQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword","zhang han yu"))
        searchSourceBuilder.query(booleanQueryBuilder)
        searchSourceBuilder.from(0).size(20)
        searchSourceBuilder.sort("doubleScore",SortOrder.DESC)
        searchSourceBuilder.highlight(new HighlightBuilder().field("name"))
        val query = searchSourceBuilder.toString
        println(query)
     }

    def main(args: Array[String]): Unit = {
     //addDoc()
       queryDoc
   }
    case class Movie0105(id:String,movie_name:String,name:String)
}
