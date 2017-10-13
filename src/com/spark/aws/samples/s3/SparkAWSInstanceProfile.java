package com.spark.aws.samples.s3;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.scheduler.ApplicationEventListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

import scala.Tuple2;

import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.json.Jackson;

/**
 * Sample program showing AWS Instance Profile based temporary credentials to
 * access AWS services like: S3, SQS, SNS.
 * 
 * @author Satish Pandey
 *
 */
public class SparkAWSInstanceProfile {

	private final static Logger logger = LogManager.getLogger(SparkAWSInstanceProfile.class);
	private final static String accountId = EC2MetadataUtils.getInstanceInfo().getAccountId();
	private final static String APPLICATION_NAME = logger.getName();
	private final static String topicName = "SPARK-TOPIC";

	public static void main(String[] args) throws Exception {
		logger.debug(String.format("Starting %s application.....", APPLICATION_NAME));
		// Taking input/output files from command line
		String s3InputFile = args[0];
		String s3OutputFile = args[1];
		logger.debug(String.format("Input file: %s, Output file: %s", s3InputFile, s3OutputFile));
		InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(true);
		AmazonSNS sns = AmazonSNSClientBuilder.standard().withCredentials(credentialsProvider).build();
		// Spark configuration
		SparkConf conf = new SparkConf().setAppName(APPLICATION_NAME);
		SparkContext context = new SparkContext(conf);
		context.addSparkListener(new ApplicationEventListener() {
			@Override
			public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
				String message = "Application end event captured : time - " + applicationEnd.time();
				logger.trace(message);
				sns.publish("arn:aws:sns:us-east-2:" + accountId + ":" + topicName, message);
			}

			@Override
			public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
				String message = "Application start event captured : time - " + applicationStart.time();
				logger.trace(message);
				sns.publish("arn:aws:sns:us-east-2:" + accountId + ":" + topicName, message);
			}
		});
		JavaSparkContext sc = new JavaSparkContext(context);
		// AWS credentials configuration
		Configuration configuration = sc.hadoopConfiguration();
		logger.trace("Loading Instance profile credentials from Instance MetaData....");
		configuration
				.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
		BasicSessionCredentials credentials = configureKeySecret(configuration, credentialsProvider);
		logger.trace("Fetching input data from S3 bucket");
		JavaRDD<String> s3InputRDD = sc.textFile(s3InputFile);
		logger.info(String.format("Total number of lines to process: %d", s3InputRDD.count()));
		JavaRDD<String> input = s3InputRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 3006184056940059269L;

			@Override
			public Iterator<String> call(Iterator<String> input) throws Exception {
				InstanceProfileCredentialsProvider provider = new InstanceProfileCredentialsProvider(true);
				AmazonSQS sqs = AmazonSQSClientBuilder.standard().withCredentials(provider).build();
				ExecutorInfo executorInfo = new ExecutorInfo(EC2MetadataUtils.getInstanceId(), EC2MetadataUtils
						.getPrivateIpAddress(), EC2MetadataUtils.getLocalHostName(), EC2MetadataUtils.getMacAddress(),
						System.getenv().get("SPARK_EXECUTOR_DIRS"), System.getProperty("user.dir"), System.getenv()
								.get("SPARK_LOG_URL_STDOUT"), System.getenv().get("SPARK_LOG_URL_STDERR"));
				String sqsMessage = Jackson.toJsonString(executorInfo);
				logger.info("Sending data to SQS queue : " + sqsMessage);
				sqs.sendMessage("SPARK-TEST-QUEUE", sqsMessage);
				return input;
			}
		});
		logger.trace("Preparing words RDD");
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
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

	static class ExecutorInfo {
		private Date createdDateTime;
		private String instanceId;
		private String privateIp;
		private String localAddress;
		private String macAddress;
		private String executorDir;
		private String userDir;
		private String logUrlStdout;
		private String logUrlStderr;

		public ExecutorInfo() {
			createdDateTime = new Date();
		}

		public ExecutorInfo(String instanceId, String privateIp, String localAddress, String macAddress,
				String executorDir, String userDir, String logUrlStdout, String logUrlStderr) {
			this();
			this.instanceId = instanceId;
			this.privateIp = privateIp;
			this.localAddress = localAddress;
			this.macAddress = macAddress;
			this.executorDir = executorDir;
			this.userDir = userDir;
			this.logUrlStdout = logUrlStdout;
			this.logUrlStderr = logUrlStderr;
		}

		public String getInstanceId() {
			return instanceId;
		}

		public void setInstanceId(String instanceId) {
			this.instanceId = instanceId;
		}

		public String getPrivateIp() {
			return privateIp;
		}

		public void setPrivateIp(String privateIp) {
			this.privateIp = privateIp;
		}

		public String getLocalAddress() {
			return localAddress;
		}

		public void setLocalAddress(String localAddress) {
			this.localAddress = localAddress;
		}

		public String getMacAddress() {
			return macAddress;
		}

		public void setMacAddress(String macAddress) {
			this.macAddress = macAddress;
		}

		public String getExecutorDir() {
			return executorDir;
		}

		public void setExecutorDir(String executorDir) {
			this.executorDir = executorDir;
		}

		public String getUserDir() {
			return userDir;
		}

		public void setUserDir(String userDir) {
			this.userDir = userDir;
		}

		public String getLogUrlStdout() {
			return logUrlStdout;
		}

		public void setLogUrlStdout(String logUrlStdout) {
			this.logUrlStdout = logUrlStdout;
		}

		public String getLogUrlStderr() {
			return logUrlStderr;
		}

		public void setLogUrlStderr(String logUrlStderr) {
			this.logUrlStderr = logUrlStderr;
		}

		public Date getCreatedDateTime() {
			return createdDateTime;
		}

	}

}