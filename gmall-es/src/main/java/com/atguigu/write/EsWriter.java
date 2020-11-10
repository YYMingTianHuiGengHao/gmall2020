package com.atguigu.write;
import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {

    public static void main(String[] args) throws IOException {


        //创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(clientConfig);

        //获取客户端连接对象
        JestClient jestClient = jestClientFactory.getObject();

        //准备数据
        Movie movie = new Movie("1010", "当幸福来敲门");
        Index index = new Index.Builder(movie)
                .index("movie_test2")
                .type("_doc")
                .id("1003")
                .build();

        jestClient.execute(index);

        jestClient.shutdownClient();

    }
}
