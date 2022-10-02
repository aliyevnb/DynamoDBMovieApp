package com.pilimit;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.IOException;


public class DdbDemoApp {
    public static void main(String[] args) {


        final String usage = "\n" +
                "App creates Demo DynamoDB table with 10 WCU/RCU with Movies DB\n\n" +
                "Usage:\n" +
                "    <tableName> <filePath> <region>\n\n" +
                "Where:\n" +
                "    tableName - The DynamoDB table to create (for example, Music3).\n" +
                "    filePath - Source JSON file \n" +
                "    region - The AWS region for the DynamoDB table (for example, us-east-1)\n\n ";

        if (args.length != 3) {
            System.err.println(usage);
            System.exit(1);
        }

        String tableName = args[0];
        String filePath = args[1];
        String tableRegion = args[2];

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.of(tableRegion);

        DynamoDbClient client = DynamoDbClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

        if(DescribeTable.getTableInfo(client, tableName)) {
            System.out.println("Table exists...\n");
            try {
                LoadData.loadData(client, tableName, filePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("Creating a new DynamoDB table "+tableName
                    + " in " + tableRegion);

            String result = CreateTable.createTable(client, tableName);
            System.out.println("New table is " + result);
            try {
                LoadData.loadData(client, tableName, filePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        client.close();
    }
}