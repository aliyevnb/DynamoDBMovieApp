package com.pilimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.thirdparty.jackson.core.JsonFactory;
import software.amazon.awssdk.thirdparty.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class PutRecord {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static void putRecord(DynamoDbEnhancedClient enhancedClient, String tableName, String fileName) throws IOException {
        try {
            DynamoDbTable<Movies> mappedTable = enhancedClient.table(tableName, TableSchema.fromBean(Movies.class));
            /*JsonParser parser = new JsonFactory().createParser(new File(fileName));
            JsonNode rootNode = new ObjectMapper().readTree(parser); */
            JsonNode rootNode = mapper.readTree(fileName);
            Iterator<JsonNode> iter = rootNode.iterator();
            ObjectNode currentNode;

            int t = 0;
            while (iter.hasNext()) {
                if(t == 200)
                    break;

                currentNode = (ObjectNode) iter.next();
                int year = currentNode.path("year").asInt();
                String title = currentNode.path("title").asText();
                String info = currentNode.path("info").toString();

                Movies movie = new Movies();
                movie.setYear(year);
                movie.setTitle(title);
                movie.setInfo(info);

                mappedTable.putItem(movie);
                t++;
            }
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
