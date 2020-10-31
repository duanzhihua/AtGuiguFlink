package com.atguigu.networkFlowAnalysis.process;

import com.atguigu.networkFlowAnalysis.domain.UvCount;
import com.atguigu.networkFlowAnalysis.serializable.Bloom;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.Iterator;

/**
 * @description: // 实现自定义的窗口处理函数
 * @author: liyang
 * @date: 2020/10/31 22:17
 */
public class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String,Long>, UvCount,String, TimeWindow> {
    // 定义redis连接以及布隆过滤器
    private transient Jedis jedis = new Jedis("192.168.136.12",6379);
    private transient Bloom bloomFilter = new Bloom((long) (1<<29));  // 位的个数：2^6(64) * 2^20(1M) * 2^3(8bit) ,64MB
    @Override
    public void process(String key, Context ctx, Iterable<Tuple2<String, Long>> input, Collector<UvCount> out) throws Exception {
        // 先定义redis中存储位图的key
        String storedBitMapKey = ctx.window().toString();
        // 另外将当前窗口的uv count值，作为状态保存到redis里，用一个叫做uvcount的hash表来保存（windowEnd，count）
        String uvCountMap = "uvcount";
        String currentKey = currentKey = String.valueOf(ctx.window().getEnd());
        Long count = 0L;
        // 从redis中取出当前窗口的uv count值
        if (jedis.hget(uvCountMap, currentKey) != null)
            count = Long.valueOf(jedis.hget(uvCountMap, currentKey));

        // 去重：判断当前userId的hash值对应的位图位置，是否为0
        Iterator<Tuple2<String, Long>> iterator = input.iterator();
        Tuple2<String, Long> last = null;
        while (iterator.hasNext())
            last = iterator.next();
        String userId = last.f1.toString();
        // 计算hash值，就对应着位图中的偏移量
        long offset = bloomFilter.hash(userId, 61);
        // 用redis的位操作命令，取bitmap中对应位的值
        boolean isExist = jedis.getbit(storedBitMapKey, offset);
        if (!isExist) {
            // 如果不存在，那么位图对应位置置1，并且将count值加1
            jedis.setbit(storedBitMapKey, offset, true);
            jedis.hset(uvCountMap, currentKey, String.valueOf(count + 1));
        }
    }
}
