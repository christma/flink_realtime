package com.cn.base;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource2 implements SourceFunction<Event> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();
        String[] users = {"Alice", "Bob"};
        String[] urls = {"./home2", "./cart2", "./fav2", "./product?id=1002", "./product?id=1002"};


        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long ts = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, ts));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
