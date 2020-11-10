package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author yymstart
 * @create 2020-11-09 18:15
 */
public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {

        //创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置连接
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(clientConfig);

        //连接客户端
        JestClient jestClient = jestClientFactory.getObject();

        //准备数据
        Movie movie1 = new Movie("1004", "11");
        Movie movie2 = new Movie("1005", "22");

        Index index1 = new Index.Builder(movie1).build();
        Index index2 = new Index.Builder(movie2).build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_test2")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        jestClient.execute(bulk);
        jestClient.shutdownClient();


    }
}
