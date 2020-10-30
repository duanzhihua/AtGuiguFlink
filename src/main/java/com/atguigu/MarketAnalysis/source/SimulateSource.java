package com.atguigu.MarketAnalysis.source;

import com.atguigu.MarketAnalysis.domain.MarketUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @description:
 * @author: liyang
 * @date: 2020/10/30 21:22
 */
public class SimulateSource extends RichSourceFunction<MarketUserBehavior> {
    private boolean running = true;
    private String[] behaviorSet = {"view","download","install","uninstall"};
    private String[] channelSet = {"appstore","weibo","wechat","tieba"};

    @Override
    public void run(SourceContext<MarketUserBehavior> ctx) throws Exception {
        Long maxCounts = Long.MAX_VALUE;
        Long count = 0L;

        while (running&& count<maxCounts){
            String id = UUID.randomUUID().toString();
            Random r1 = new Random();
            Random r2 = new Random();
            String behavior = behaviorSet[r1.nextInt(behaviorSet.length)];
            String channel = channelSet[r2.nextInt(channelSet.length)];
            Long ts = System.currentTimeMillis();
            ctx.collect(new MarketUserBehavior(id,behavior,channel,ts));
            count++;
            Thread.sleep(50L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
