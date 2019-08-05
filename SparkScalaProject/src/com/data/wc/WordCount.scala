package com.data.wc

import scala.math.random

import org.apache.spark.sql.SparkSession

object Wordcount {
	def main(args: Array[String]) {
	  
		if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
      System.exit(1);
    }
		
		val spark = SparkSession
				.builder
				.appName("Wordcount")
				.getOrCreate()

    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.flatMap { line => {
      line.split(" ")
      
      /*
       * DataFlair is the leading Training Provider of Big Data Technologies 
       * DataFlair 
       * is 
       * the 
       * leading 
       * Training 
       * Provider 
       * of 
       * Big 
       * Data 
       * Technologies 
       */
    } }
    .map { words => (words, 1)
      /*
       * DataFlair, 1 
       * is, 1
       * the, 1
       * leading, 1 
       * Training, 1 
       * Provider, 1 
       * of, 1 
       * Big, 1 
       * Data, 1 
       * Technologies, 1 
       */

      }
    .reduceByKey(_+_)
    /*
     * DataFlair	[1 1 1 1 1 1..........]		=>		DataFlair, 369
     * Training		[1 1 1 1 1 1..........]		=>		Training, 760
     */
    
    result.saveAsTextFile(args(1))
    
    spark.stop
	}
}
//bin/spark-submit --class com.dataflair.wc.Wordcount ../sparkWcJob.jar ../about-dataflair.txt wc-out-001