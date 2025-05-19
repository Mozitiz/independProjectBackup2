package com.example.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class RevenueAggregationFunction extends KeyedProcessFunction<String, Tuple5<Long, String, Integer, Double, Boolean>, Tuple4<Long, String, Integer, Double>> {
    
    // 使用Tuple3<Long, String, Integer>作为key，即l_orderkey, o_orderdate, o_shippriority
    // 使用Double作为value，即revenue
    private MapState<Tuple3<Long, String, Integer>, Double> revenueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化revenue状态
        MapStateDescriptor<Tuple3<Long, String, Integer>, Double> revenueDescriptor = 
            new MapStateDescriptor<>(
                "revenueState",
                Types.TUPLE(Types.LONG, Types.STRING, Types.INT),
                Types.DOUBLE
            );
        revenueState = getRuntimeContext().getMapState(revenueDescriptor);
    }

    @Override
    public void processElement(Tuple5<Long, String, Integer, Double, Boolean> value, Context ctx, Collector<Tuple4<Long, String, Integer, Double>> out) throws Exception {
        // 创建复合键：l_orderkey, o_orderdate, o_shippriority
        Tuple3<Long, String, Integer> key = new Tuple3<>(value.f0, value.f1, value.f2);
        
        // 获取当前的revenue
        Double currentRevenue = revenueState.get(key);
        
        if (currentRevenue == null) {
            // 如果不存在，则添加新的revenue
            currentRevenue = 0.0; 
        } 

        if (value.f4) {
            // 如果是添加操作，则累加revenue
            currentRevenue += value.f3;
        } else {
            // 如果是删除操作，则从状态中移除
            currentRevenue -= value.f3;
        }

        // 更新状态
        revenueState.put(key, currentRevenue);
        
        // 输出结果（只包含4个字段）
        out.collect(new Tuple4<>(value.f0, value.f1, value.f2, currentRevenue));
    }
} 