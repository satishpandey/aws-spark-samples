package com.spark.aws.samples.s3;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Sample program showing embedded AWS credentials to fetch data from S3 bucket.
 * 
 * @author Satish Pandey
 *
 */
public class SparkS3EmbeddedCredentials {

	private final static Logger logger = LogManager.getLogger(SparkS3EmbeddedCredentials.class);
	private final static String APPLICATION_NAME = logger.getName();

	public static void main(String[] args) throws Exception {
		logger.debug(String.format("Starting %s application.....", APPLICATION_NAME));
		// Taking input/output files from command line
		String s3InputFile = args[0];
		String s3OutputFile = args[1];
		logger.debug(String.format("Input file: %s, Output file: %s", s3InputFile, s3OutputFile));
		// Spark configuration
		SparkConf conf = new SparkConf().setAppName(APPLICATION_NAME);
		JavaSparkContext sc = new JavaSparkContext(conf);
		// AWS credentials configuration
		Configuration configuration = sc.hadoopConfiguration();
		configuration.set("fs.s3a.access.key", "<AWS-ACCESS-KEY>");
		configuration.set("fs.s3a.secret.key", "<AWS-SECRET-KEY>");
		// Fetching input data from S3 bucket
		JavaRDD<String> s3InputRDD = sc.textFile(s3InputFile);
		logger.info(String.format("Total number of lines to process: %d", s3InputRDD.count()));
		// Preparing words RDD
		JavaRDD<String> words = s3InputRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		logger.info(String.format("Total words : %d", words.count()));
		// Preparing words count RDD
		JavaPairRDD<String, Integer> wordsCount = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2<String, Integer>(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		logger.info(String.format("Total unique words : %d", wordsCount.count()));
		// Storing output data back to S3 bucket
		wordsCount.saveAsTextFile(s3OutputFile);
		logger.debug(String.format("Exiting %s application....", APPLICATION_NAME));
		sc.close();
		System.exit(0);
	}
}