package com.example.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.example.model.Customer;

public class CustomerProcessFunction extends KeyedProcessFunction<Long, Customer, Tuple2<Long, Boolean>> {

    @Override
    public void processElement(Customer customer, Context ctx, Collector<Tuple2<Long, Boolean>> out) throws Exception {
        // 只处理market segment为AUTOMOBILE的记录
        if ("AUTOMOBILE".equals(customer.getMktSegment())) {
            // 输出custKey和isAlive状态
            out.collect(new Tuple2<>(customer.getCustKey(), customer.isAlive()));
        }
    }
} 