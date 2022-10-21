package com.pilimit;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.IOException;


public class DdbDemoApp {
    public static void main(String[] args) throws IOException {


        final String usage = "\n" +
                "App creates Demo DynamoDB table with 10 WCU/RCU with Movies DB\n\n" +
                "Usage:\n" +
                "    create <tableName> <filePath> <region>\n" +
                "    delete <tableName> <region>\n\n" +
                "Where:\n" +
                "    tableName - The DynamoDB table to create (for example, Music3).\n" +
                "    filePath - Source JSON file \n" +
                "    region - The AWS region for the DynamoDB table (for example, us-east-1)\n\n ";


        if (args.length >= 3) {
            System.err.println(usage);
            System.exit(1);
        }

        /*
        TODO: Table command to create/delete/ (possiblly) update table parameters
         */
        String tableName = args[0];
        String filePath = args[1];
        String tableRegion = args[2];

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.of(tableRegion);

        DynamoDbClient ddbClient = DynamoDbClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

/*        if(DescribeTable.getTableInfo(client, tableName)) {
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
        }*/

        System.out.println("Createing new DynamoDB table " + tableName);
        String result = CreateTable.createTable(ddbClient, tableName);
        System.out.println(result);

        LoadData.loadData(ddbClient, tableName, filePath);

        ddbClient.close();
    }
}