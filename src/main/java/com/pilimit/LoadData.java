package com.pilimit;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import java.io.IOException;

public class LoadData {

    public static void loadData(DynamoDbClient ddb, String tablaName, String fileName) throws IOException {

        DynamoDbEnhancedClient enhansedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();
        PutRecord.putRecord(enhansedClient, tablaName, fileName);
    }
}
