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

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;

/**
 * Sample program showing AWS STS based temporary credentials to access S3
 * filesystem.
 * 
 * @author Satish Pandey
 *
 */
public class SparkS3STSAssumeRole {

	private final static Logger logger = LogManager.getLogger(SparkS3STSAssumeRole.class);
	private final static String APPLICATION_NAME = logger.getName();
	private final static String accountId = EC2MetadataUtils.getInstanceInfo().getAccountId();
	// RoleArn template
	private final static String roleArnTemplate = String.format("arn:aws:iam::%s:role/%s", accountId, "%s");

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
		logger.trace("Loading STS Temporary credentials....");
		String roleSessionName = EC2MetadataUtils.getInstanceId() + "Session";
		String roleArnRead = String.format(roleArnTemplate, "S3ReadOnlyCredentialsSession");
		InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
		AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.standard()
				.withCredentials(credentialsProvider).build();
		STSAssumeRoleSessionCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArnRead,
				roleSessionName).withStsClient(sts).build();
		configuration.set("fs.s3a.access.key", provider.getCredentials().getAWSAccessKeyId());
		configuration.set("fs.s3a.secret.key", provider.getCredentials().getAWSSecretKey());
		configuration.set("fs.s3a.session.token", provider.getCredentials().getSessionToken());
		configuration
				.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
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
		String roleArnWrite = String.format(roleArnTemplate, "S3FullAccessCredentials");
		STSAssumeRoleSessionCredentialsProvider provider2 = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArnWrite,
				roleSessionName).withStsClient(sts).build();
		configuration.set("fs.s3a.access.key", provider2.getCredentials().getAWSAccessKeyId());
		configuration.set("fs.s3a.secret.key", provider2.getCredentials().getAWSSecretKey());
		configuration.set("fs.s3a.session.token", provider2.getCredentials().getSessionToken());
		logger.trace("Storing output data back to S3 bucket");
		wordsCount.saveAsTextFile(s3OutputFile);
		logger.debug(String.format("Exiting %s application....", APPLICATION_NAME));
		sc.close();
		System.exit(0);
	}	
}