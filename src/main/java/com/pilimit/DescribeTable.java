package com.pilimit;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DescribeTable {

    public static boolean getTableInfo(DynamoDbClient ddb, String tableName) {
        DescribeTableRequest request = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();

        boolean tableExists = false;

        try {
            TableDescription tableInfo = ddb.describeTable(request).table();

            if(tableInfo != null) {
                System.out.println("Table " + tableInfo.tableArn() + " exists");
                tableExists = true;
            } else {
                System.out.println("Table does not exist");
                tableExists = false;
            }
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return tableExists;
    }

}
