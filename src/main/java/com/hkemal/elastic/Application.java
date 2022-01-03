package com.hkemal.elastic;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

public class Application {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();


        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        //List<DiscoveryNode> discoveryNodes = client.listedNodes();
        //discoveryNodes.stream().forEach(System.out::println);

        //Index Api
        /*
        Map<String, Object> json1 = new HashMap<String, Object>();
        json1.put("name", "Asus Vivobook");
        json1.put("detail", "Intel Core i5, 8GB Ram, 1TB HDD");
        json1.put("price", 5000);
        json1.put("currency", "Turkish Liras");
        json1.put("provider", "Asus TR");

        Map<String, Object> json2 = new HashMap<String, Object>();
        json2.put("name", "Dell Insprion");
        json2.put("detail", "Intel Core i7, 16GB Ram, 256GB SDD");
        json2.put("price", 7000);
        json2.put("currency", "Turkish Liras");
        json2.put("provider", "Dell TR");

        Map<String, Object> json3 = new HashMap<String, Object>();
        json3.put("name", "Apple MacBook Air");
        json3.put("detail", "M1 , 16GB Ram, 512GB SDD");
        json3.put("price", 10000);
        json3.put("currency", "Turkish Liras");
        json3.put("provider", "Apple TR");

        IndexResponse indexResponse1 = client.prepareIndex("product", "_doc", "1")
                .setSource(json1, XContentType.JSON).get();
        IndexResponse indexResponse2 = client.prepareIndex("product", "_doc", "2")
                .setSource(json2, XContentType.JSON).get();
        IndexResponse indexResponse3 = client.prepareIndex("product", "_doc", "3")
                .setSource(json3, XContentType.JSON).get();

        System.out.println(indexResponse1.getId());
        System.out.println(indexResponse2.getId());
        System.out.println(indexResponse3.getId());
         */

        //Get Api
        /*
        GetResponse response = client.prepareGet("product", "_doc", "1").get();
        Map<String, Object> source = response.getSource();
        source.forEach((key, value) -> System.out.println(key + " : " + value));
        */

        //Search Api
        /*
        SearchResponse response1 = client.prepareSearch("product").setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("name", "Asus")).get();
        SearchResponse response2 = client.prepareSearch("product").setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("provider", "TR")).get();
        SearchHit[] hits1 = response1.getHits().getHits();
        Arrays.stream(hits1).forEach(item -> {
            Map<String, Object> sourceAsMap = item.getSourceAsMap();
            System.out.println(sourceAsMap);
        });
        System.out.println("*************************");
        SearchHit[] hits2 = response2.getHits().getHits();
        Arrays.stream(hits2).forEach(item -> {
            Map<String, Object> sourceAsMap = item.getSourceAsMap();
            System.out.println(sourceAsMap);
        });
        */
        //DeleteResponse response = client.prepareDelete("product", "_doc", "1").get();

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("name", "Apple"))
                .source("product")
                .get();
        System.out.println(response.getDeleted());

    }
}
