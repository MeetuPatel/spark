package org.apache.spark.examples.ml;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Map;
import java.util.StringTokenizer;

public class SentimentAnalysis {
    public static Tuple2<String, Integer> sentimentScore(Map<String, Integer> wordList, String username, String tweet) {
        int sentiment;
        sentiment = 0;
        boolean negationExists = false;
        int negationIndex = -1;
        int currIndex = -1;

        StringTokenizer words = new StringTokenizer(tweet);
        while(words.hasMoreTokens()){
            String word = words.nextToken();
            currIndex = words.countTokens() + 1;

            if(wordList.containsKey(word.toLowerCase())) { //if the wordlist contains a word from the tweet
                if (wordList.get(word.toLowerCase()) == 0) { //checking if it's a negation word
                    negationExists = true;
                    negationIndex = words.countTokens() + 1;
                }
                else if (wordList.get(word.toLowerCase()) == 1) {
                    sentiment += 1; //increasing the score for a positive word
                    if(negationExists && (Math.abs(currIndex - negationIndex) <= 3)){ //checking if negation word present and if its 3 positions before or after a word
                        sentiment -= 2; //if negation exists then its a negative sentiment so decreasing twice, once for the previous increase in score and one for the negative sentiment
                    }
                }
                else if(wordList.get(word.toLowerCase()) == -1){
                    sentiment -= 1; //decreasing the score for a negative word
                    if(negationExists && (Math.abs(currIndex - negationIndex) <= 3)){ //checking if negation word exists and if its 3 positions before or after a word
                        sentiment += 2; //if negation exists then its a positive sentiment so increasing twice, once for the previous decrease in score and one for the positive sentiment
                    }
                }
            }
        }

        return new Tuple2<>(username, sentiment);

    }

    public static void main(String[] args) throws Exception {

//        args[0] - file path to dataset and args[1] - file path to word list
        if (args.length < 2) {
            System.err.println("Not enough arguments");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SentimentAnalysis")
                .getOrCreate();

//        creating word list from file
        JavaRDD<Row> list = spark.read().csv(args[1]).javaRDD();  //optimisation - can you directly create a paired rdd from file or should you use javaSparkContext.textFile?
        Map<String, Integer> wordList = list.mapToPair(row -> new Tuple2<>(row.getString(0), Integer.parseInt(row.getString(1)))).collectAsMap();

        Dataset<Row> df = spark.read().csv(args[0]); // add max split

//        drop columns not required. just keep username and tweet
        String[] colNames;
        colNames = new String[]{"_c0", "_c1", "_c2", "_c3"};
        df = df.drop(colNames);

//        using mapToPair function to transform each row from <username, tweet> to <username, sentiment score> by applying the sentimentScore function to each row
        JavaRDD<Row> rdd = df.javaRDD();
        JavaPairRDD<String, Integer> userScorePairs = rdd.mapToPair(row -> sentimentScore(wordList, row.getString(0),row.getString(1)));

//        using reduce by key to sum up the sentiment scores for each unique username
        JavaPairRDD<String, Integer> counts = userScorePairs.reduceByKey((i1, i2) -> i1 + i2);
//        List<Tuple2<String, Integer>> output = counts.collect();
        counts.saveAsTextFile("outputJava"); //make this an input parameter
//        System.out.println("Output here");
//        System.out.println(output);
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }

        spark.stop();




    }




}
