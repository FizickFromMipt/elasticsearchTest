package ru.pevnenko;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchExample {

    public static void main(String[] args) {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

            System.setProperty("log4j.configurationFile","./path_to_the_log4j2_config_file/log4j2.xml");

            Logger log = LogManager.getLogger(ElasticsearchExample.class.getName());

            // Создание индекса
            createIndex(client, "my_index2");

            // Добавление документа
            addDocument(client, "my_index2", "1", "name", "John Doe", "age", 30);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createIndex(RestHighLevelClient client, String indexName) throws IOException {
        // Создание запроса для создания индекса
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    private static void addDocument(RestHighLevelClient client, String index, String id, String field1, String value1, String field2, int value2) throws IOException {
        // Создание запроса для добавления документа
        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(createSource(field1, value1, field2, value2), XContentType.JSON);

        // Отправка запроса и получение ответа
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // Обработка ответа
        System.out.println("Document added. Index: " + response.getIndex() + ", ID: " + response.getId());
    }

    private static Map<String, Object> createSource(String field1, String value1, String field2, int value2) {
        Map<String, Object> source = new HashMap<>();
        source.put(field1, value1);
        source.put(field2, value2);
        return source;
    }
}
