package com.pilimit;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public class Movies {
    private String title;
    private int year;
    private String info;

    @DynamoDbSecondaryPartitionKey(indexNames = { "year-index"})
    @DynamoDbPartitionKey
    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @DynamoDbSortKey
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }
}
