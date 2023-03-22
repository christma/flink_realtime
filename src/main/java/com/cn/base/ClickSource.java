package com.cn.base;

import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();
        String[] users = {"Alice", "Bob", "Tom", "Jack"};
        String[] urls = {"./home", "./cart", "./fav", "./product?id=100", "./product?id=100"};


        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long ts = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, ts));
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
