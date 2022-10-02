package com.pilimit;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.ArrayList;

public class CreateTable {

    public static String createTable(DynamoDbClient ddb, String tableName) {

        String tablePriKey = "year";
        String tableRngKey = "title";

        DynamoDbWaiter dbWaiter = ddb.waiter();

        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();

        attributeDefinitions.add(AttributeDefinition.builder()
                .attributeName(tablePriKey)
                .attributeType(ScalarAttributeType.N)
                .build());

        attributeDefinitions.add((AttributeDefinition.builder()
                .attributeName(tableRngKey)
                .attributeType(ScalarAttributeType.S)
                .build()));

        ArrayList<KeySchemaElement> keySchemaElements = new ArrayList<>();

        KeySchemaElement key = KeySchemaElement.builder()
                .attributeName(tablePriKey)
                .keyType(KeyType.HASH)
                .build();

        KeySchemaElement key2 = KeySchemaElement.builder()
                .attributeName(tableRngKey)
                .keyType(KeyType.RANGE)
                .build();

        keySchemaElements.add(key);
        keySchemaElements.add(key2);

        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(attributeDefinitions)
                .keySchema(keySchemaElements)
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(Long.valueOf(10))
                        .writeCapacityUnits(Long.valueOf(10))
                        .build())
                .tableName(tableName)
                .build();

        String newTable = "";

        try {
            CreateTableResponse response = ddb.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();

            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);
            newTable = response.tableDescription().tableName();
            return newTable;
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }
}
