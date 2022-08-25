// scalastyle:off println


package org.apache.spark.examples.ml

import org.apache.spark.sql.SparkSession

object ScalaSentimentAnalysis {

//  function to transform <username, tweet> pairs to <username,sentiment score>
  //  pairs. It uses word list to check which words are positive, negative and negation.
//  def generateGraph: Seq[(Int, Int)] = {
//    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
//
//  }

  def sentimentScore(wordList: Map[String, Int],
                     username: String, tweet: String): (String, Int) = {
    var sentiment = 0
    var negationExists = false
    var negationIndex = -1
    var currIndex = -1

    val words = tweet.split(" ")
    var count = 0
    for (word <- words) {
      currIndex = count
      if (wordList.contains(word.toLowerCase())) {
        if (wordList.getOrElse(word.toLowerCase(), 2) == 0) {
          negationExists = true
          negationIndex = count
        }
        else if (wordList.getOrElse(word.toLowerCase(), 2) == 1) {
          sentiment += 1
          if (negationExists && (Math.abs(currIndex - negationIndex)) <= 3) {
            sentiment -= 2
          }
        }
        else if (wordList.getOrElse(word.toLowerCase(), 2) == -1) {
          sentiment -= 1
          if (negationExists && (Math.abs(currIndex - negationIndex) <= 3)) {
            sentiment += 2
          }
        }
      }
      count += 1
    }
    new Tuple2[String, Int](username, sentiment)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Not enough arguments")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("ScalaSentimentAnalysis")
      .getOrCreate()

//    read word list from file
    val list = spark.read.csv(args(1)).rdd
//    val list = spark.sparkContext.textFile(args(1))
//    wordList is a RDD[Array[String]] here

//    val wordList: Map[String, Integer] = list.map(f => {
//      f.split(",")
//    }).collect().toMap: Map[String, Integer]

//    val wordList = Map(list.map(f => {
//      f.split(",")
//    }).collect())

    val wordList = list.map(f => {
      (f.getString(0), f.getString(1).toInt)
    }).collect().toMap
    //    read dataset from file
    var df = spark.read.csv(args(0))
    //    transform it to <username,tweet>
    df = df.drop("_c0", "_c1", "_c2", "_c3")

//    call sentimentScore function on each pair of <username,tweet> to
    //    convert it to <username, sentiment score>

    val rdd = df.rdd
    val underscorePairs = rdd.map(row =>
        sentimentScore(wordList, row.getString(0), row.getString(1)))

    val counts = underscorePairs.reduceByKey(_ + _)
    counts.saveAsTextFile("outputScala")
    spark.stop()

  }
}
