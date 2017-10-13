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

import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;

/**
 * Sample program showing AWS Instance Profile based temporary credentials to
 * access S3 filesystem.
 * 
 * @author Satish Pandey
 *
 */
public class SparkS3InstanceProfile {

	private final static Logger logger = LogManager.getLogger(SparkS3InstanceProfile.class);
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
		logger.trace("Loading Instance profile credentials from Instance MetaData....");
		InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
		configuration
				.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
		BasicSessionCredentials credentials = configureKeySecret(configuration, credentialsProvider);
		logger.trace("Fetching input data from S3 bucket");
		JavaRDD<String> s3InputRDD = sc.textFile(s3InputFile);
		logger.info(String.format("Total number of lines to process: %d", s3InputRDD.count()));
		logger.trace("Preparing words RDD");
		JavaRDD<String> words = s3InputRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		logger.info(String.format("Total words : %d", words.count()));
		logger.trace("Preparing words count RDD");
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
		logger.trace("Storing output data back to S3 bucket");
		if (credentialsProvider.getCredentials().getAWSAccessKeyId().equals(credentials.getAWSAccessKeyId())) {
			configureKeySecret(configuration, credentialsProvider);
		}
		wordsCount.saveAsTextFile(s3OutputFile);
		logger.debug(String.format("Exiting %s application....", APPLICATION_NAME));
		sc.close();
		System.exit(0);
	}

	private static BasicSessionCredentials configureKeySecret(Configuration configuration,
			InstanceProfileCredentialsProvider credentialsProvider) {
		BasicSessionCredentials credentials = (BasicSessionCredentials) credentialsProvider.getCredentials();
		configuration.set("fs.s3a.access.key", credentials.getAWSAccessKeyId());
		configuration.set("fs.s3a.secret.key", credentials.getAWSSecretKey());
		configuration.set("fs.s3a.session.token", credentials.getSessionToken());
		return credentials;
	}
}