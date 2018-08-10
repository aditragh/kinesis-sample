package com.aws.kinesis.sample;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest.Builder;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.adapter.StringToSdkBytesAdapter;

public class AmazonKinesisRecordProducerSample {

	/*
	 * Before running the code: Fill in your AWS access credentials in the
	 * provided credentials file template, and be sure to move the file to the
	 * default location (~/.aws/credentials) where the sample code will load the
	 * credentials from.
	 * https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */
	public static final String SAMPLE_APPLICATION_STREAM_NAME = "2.xTest";

	private static final Region REGION = Region.US_WEST_2;

	private static KinesisAsyncClient kinesis;
	
	private static DefaultCredentialsProvider credentialsProvider;

	private static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		credentialsProvider = DefaultCredentialsProvider.create();
		try {
			credentialsProvider.resolveCredentials();
		} catch (Exception e) {
			throw new Exception("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}

		kinesis = KinesisAsyncClient.builder().region(REGION)
				.credentialsProvider(credentialsProvider).build();
	}

	public static void main(String[] args) throws Exception {
		init();

		final String myStreamName = SAMPLE_APPLICATION_STREAM_NAME;
		final Integer myStreamSize = 1;
		System.out.println(System.getProperty("StreamName"));

		// Describe the stream and check if it exists.
		DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder().streamName(SAMPLE_APPLICATION_STREAM_NAME).build();
		try {
			
			StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).get().streamDescription();
			StreamStatus status = streamDescription.streamStatus();
			System.out.printf("Stream %s has a status of %s.\n", myStreamName, status);

			if (StreamStatus.DELETING.equals(status)) {
				System.out.println("Stream is being deleted. This sample will now exit.");
				System.exit(0);
			}

			// Wait for the stream to become active if it is not yet ACTIVE.
			if (!StreamStatus.ACTIVE.equals(status)) {
				waitForStreamToBecomeAvailable(myStreamName);
			}
		} catch (ResourceNotFoundException ex) {
			System.out.printf("Stream %s does not exist. Creating it now.\n", myStreamName);

			// Create a stream. The number of shards determines the provisioned
			// throughput.
			CreateStreamRequest createStreamRequest = CreateStreamRequest.builder().streamName(myStreamName).shardCount(myStreamSize).build();
			kinesis.createStream(createStreamRequest).get();
			// The stream is now being created. Wait for it to become active.
			waitForStreamToBecomeAvailable(myStreamName);
		}

		// List all of my streams.
		
		ListStreamsRequest listStreamsRequest = ListStreamsRequest.builder().limit(10).build();
		ListStreamsResponse listStreamsResult = kinesis.listStreams(listStreamsRequest).get();
		List<String> streamNames = listStreamsResult.streamNames();
		while (listStreamsResult.hasMoreStreams()) {
			if (streamNames.size() > 0) {				
				listStreamsRequest = ListStreamsRequest.builder().limit(10).exclusiveStartStreamName(streamNames.get(streamNames.size() - 1)).build();
			}
			listStreamsResult = kinesis.listStreams(listStreamsRequest).get();
			streamNames.addAll(listStreamsResult.streamNames());
		}
		// Print all of my streams.
		System.out.println("List of my streams: ");
		for (int i = 0; i < streamNames.size(); i++) {
			System.out.println("\t- " + streamNames.get(i));
		}

		System.out.printf("Putting records in stream : %s until this application is stopped...\n", myStreamName);
		System.out.println("Press CTRL-C to stop.");
		// Write records to the stream until this program is aborted.
		while (true) {
			long createTime = System.currentTimeMillis();
			Builder putRecordRequestBuilder = PutRecordRequest.builder().data(new StringToSdkBytesAdapter()
							.adapt(String.format("testData-" + LocalDateTime.now().toString())))
					.streamName(myStreamName).partitionKey(String.format("partitionKey-%d", createTime));

			PutRecordRequest putRecordRequest = putRecordRequestBuilder.build();
			PutRecordResponse putRecordResult = kinesis.putRecord(putRecordRequest).get();
			System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
					putRecordRequest.partitionKey(), putRecordResult.shardId(),
					putRecordResult.sequenceNumber());
			Thread.sleep(500);
		}
	}

	private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
		System.out.printf("Waiting for %s to become ACTIVE...\n", myStreamName);

		long startTime = System.currentTimeMillis();
		long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
		while (System.currentTimeMillis() < endTime) {
			Thread.sleep(TimeUnit.SECONDS.toMillis(20));
			DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder().build();
			StreamDescription streamDescription = null;
			try {
				streamDescription = kinesis.describeStream(describeStreamRequest).get().streamDescription();

			} catch (ResourceNotFoundException ex) {
				// ResourceNotFound means the stream doesn't exist yet,
				// so ignore this error and just keep polling.
			} catch (AwsServiceException ase) {
				throw ase;
			} catch (Exception e) {
				System.out.println("There was an unhandled exception");
			}
			StreamStatus status = streamDescription.streamStatus();
			System.out.printf("Stream %s has a status of %s.\n", myStreamName, status);

			// Wait for the stream to become active if it is not yet ACTIVE.
			if (!StreamStatus.ACTIVE.equals(status)) {
				return;
			}
		}
		throw new RuntimeException(String.format("Stream %s never became active", myStreamName));
	}
}