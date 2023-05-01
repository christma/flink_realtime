package com.cn.pvuv.function;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.cn.pvuv.entity.RealStatResult;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 构造ESSinkFunction
 */
public class EsSinkFunctionFactory {

    public static final Logger logger =  LoggerFactory.getLogger(EsSinkFunctionFactory.class) ;

    /**
     * 获取具体的SinkFunction
     * @param hostList es连接地址
     * @param userName es连接用户名
     * @param password es连接密码
     * @param indexPrefix 写入的索引前缀 （按天区分索引）
     * @return
     */
    public static ElasticsearchSink<RealStatResult> getElasticsearchSinkFunction(List<HttpHost> hostList,
                                                                                 String userName,
                                                                                 String password,
                                                                                 String indexPrefix) {

        ElasticsearchSink.Builder<RealStatResult> builder = new ElasticsearchSink.Builder<>(hostList, new ElasticsearchSinkFunction<RealStatResult>() {
            @Override
            public void process(RealStatResult realStatResult, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(buildRequest(realStatResult));
            }

            private IndexRequest buildRequest(RealStatResult realStatResult) {
                Map<String, Object> sourceMap = new HashMap<>();
                sourceMap.put("statisticsTime", realStatResult.getStatisticsTime());
                sourceMap.put("count", realStatResult.getCount());
                sourceMap.put("statType", realStatResult.getStatType());
                sourceMap.put("dataType", realStatResult.getDataType());
                sourceMap.put("step", realStatResult.getStep());

                String indexName = indexPrefix + realStatResult.getStatisticsTime().split(" ")[0].replaceAll("-", "");

                return Requests.indexRequest()
                        .index(indexName)
                        .type("_doc")
                        .source(sourceMap);
            }
        });

        // 配置异常处理器
        builder.setFailureHandler(new ActionRequestFailureHandler() {
            @Override
            public void onFailure(ActionRequest actionRequest, Throwable throwable, int restCode, RequestIndexer indexer) throws Throwable {
                if(ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()) {
                    indexer.add((IndexRequest) actionRequest);
                }else if(ExceptionUtils.findThrowable(throwable, InterruptedException.class).isPresent()) {
                    indexer.add((IndexRequest) actionRequest);
                }else if(ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()) {
                    indexer.add((IndexRequest) actionRequest);
                }else {
                    logger.error("sink to es exception, exceptionData: {}， exceptionStackTrace: {}",
                            actionRequest.toString(),
                            org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(throwable));
                    throw throwable ;
                }
            }
        });
        // 刷新 bulk  缓冲区中的重试次数
        builder.setBulkFlushBackoffRetries(5);
        // 重试间隔时间 ms
        builder.setBulkFlushBackoffDelay(5000);

        builder.setRestClientFactory(restClientBuilder -> {
            restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                    CredentialsProvider credentialProvider = new BasicCredentialsProvider() ;
                    credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
                    return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider) ;
                }
            }) ;

            restClientBuilder.setMaxRetryTimeoutMillis(5 * 60 * 1000) ;

            restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
                    builder.setConnectTimeout(5000) ;
                    builder.setSocketTimeout(60000) ;
                    builder.setConnectionRequestTimeout(10000) ;

                    return builder ;
                }
            }) ;
        });

        return builder.build();
    }
}
