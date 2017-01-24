package com.chebotarskyi.dm.AWS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

/**
 * Created by dima on 23.01.17.
 */
public class SQSReceiver {

    private static AWSCredentials credentials = null;
    private final static String sqsURL = "https://sqs.us-west-2.amazonaws.com/983680736795/ChebotarskyiSQS";
    private final static String bucketName = "lab4-weeia";

    public static void main(String[] args){
        System.out.println("Hello world!");
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);

        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setRegion(usWest2);

        List<String> filesList = getMessages(sqs);

        for (String file : filesList) {
            String[] files = file.split(",");

            for (int i=0; i<files.length; ++i) {
                try {
                    processFile(files[i], s3);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        System.out.println();

    }

    private static List<String> getMessages(AmazonSQS sqs) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsURL);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

        List<String> filesToProcess = new ArrayList<String>();

        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue().getStringValue());
                filesToProcess.add(entry.getValue().getStringValue());
            }
            System.out.println("Deleting a message.\n");
            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(sqsURL, messageReceiptHandle));
        }

        return filesToProcess;
    }

    private static void processFile(String key, AmazonS3 s3) throws IOException {

        System.out.println("Downloading an object");
        S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        //displayTextInputStream(object.getObjectContent());

        System.out.println("Deleting an object\n");
        s3.deleteObject(bucketName, key);

        System.out.println("Processing...");

        System.out.println("Uploading a new object to S3 from a file\n");
        s3.putObject(new PutObjectRequest(bucketName, key + "_1", object.getObjectContent(), object.getObjectMetadata()));
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
