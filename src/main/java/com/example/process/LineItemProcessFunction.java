package com.example.process;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.example.model.LineItem;

public class LineItemProcessFunction extends KeyedCoProcessFunction<Long, Tuple4<Long, String, Integer, Boolean>, LineItem, Tuple5<Long, String, Integer, Double, Boolean>> {
    
    private static final String FILTER_DATE = "1995-03-13";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private ValueState<Tuple3<Long, String, Integer>> orderState;
    private MapState<Long, List<Tuple2<Integer, Double>>> lineItemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple3<Long, String, Integer>> orderDescriptor = 
            new ValueStateDescriptor<>("orderState", Types.TUPLE(Types.LONG, Types.STRING, Types.INT));
        orderState = getRuntimeContext().getState(orderDescriptor);

        MapStateDescriptor<Long, List<Tuple2<Integer, Double>>> lineItemDescriptor = 
            new MapStateDescriptor<>("lineItemState", 
                Types.LONG,
                Types.LIST(Types.TUPLE(Types.INT, Types.DOUBLE)));
        lineItemState = getRuntimeContext().getMapState(lineItemDescriptor);
    }

    @Override
    public void processElement1(Tuple4<Long, String, Integer, Boolean> order, Context ctx, Collector<Tuple5<Long, String, Integer, Double, Boolean>> out) throws Exception {
        // 获取当前的order信息
        Tuple3<Long, String, Integer> currentOrder = orderState.value();
        
        // 根据isAlive状态来更新或删除orderState
        if (order.f3) {  // isAlive = true
            // 添加操作：检查orderKey是否已存在
            if (currentOrder == null || !currentOrder.f0.equals(order.f0)) {
                // 如果不存在，则添加新的order信息
                orderState.update(new Tuple3<>(order.f0, order.f1, order.f2));
            }
        } else {  // isAlive = false
            // 删除操作：检查orderKey是否存在
            if (currentOrder != null && currentOrder.f0.equals(order.f0)) {
                // 如果存在，则删除order信息
                orderState.clear();
            }
        }
        
        // 检查lineItemState中是否存在对应的l_orderKey
        Long orderKey = order.f0;  // o_orderKey
        if (lineItemState.contains(orderKey)) {
            // 获取该orderKey对应的所有LineItem信息
            List<Tuple2<Integer, Double>> lineItems = lineItemState.get(orderKey);
            
            // 输出所有LineItem信息
            for (Tuple2<Integer, Double> item : lineItems) {
                // 输出格式：l_orderKey, o_orderDate, o_shippriority, revenue, o_isAlive
                out.collect(new Tuple5<>(
                    orderKey,           // l_orderKey
                    order.f1,           // o_orderDate
                    order.f2,           // o_shippriority
                    item.f1,            // revenue
                    order.f3            // o_isAlive
                ));
            }
        }
    }

    @Override
    public void processElement2(LineItem lineItem, Context ctx, Collector<Tuple5<Long, String, Integer, Double, Boolean>> out) throws Exception {
        // 获取订单信息
        Tuple3<Long, String, Integer> order = orderState.value();
        
        // // 如果订单信息不存在，则直接返回
        // if (order == null) {
        //     return;
        // }
        
        // 检查shipdate
        String shipDate = lineItem.getShipDate();
        if (shipDate != null) {
            // 解析shipDate并比较
            LocalDate shipLocalDate = LocalDate.parse(shipDate, DATE_FORMATTER);
            LocalDate filterLocalDate = LocalDate.parse(FILTER_DATE, DATE_FORMATTER);
            
            if (shipLocalDate.isAfter(filterLocalDate)) {
                // 根据isAlive状态来更新或删除lineItemState
                if (lineItem.isAlive()) {
                    // 添加操作
                    List<Tuple2<Integer, Double>> lineItems = lineItemState.get(lineItem.getOrderKey());
                    if (lineItems == null) {
                        lineItems = new ArrayList<>();
                    }
                    
                    // 检查是否已存在相同的lineNumber
                    boolean exists = false;
                    for (Tuple2<Integer, Double> item : lineItems) {
                        if (item.f0.equals(lineItem.getLineNumber())) {
                            exists = true;
                            break;
                        }
                    }
                    
                    // 如果不存在相同的lineNumber，则添加到列表中
                    if (!exists) {
                        // 计算revenue
                        double revenue = lineItem.getExtendedPrice() * (1 - lineItem.getDiscount());
                        
                        lineItems.add(new Tuple2<>(
                            lineItem.getLineNumber(),   // lineNumber
                            revenue                     // revenue
                        ));
                        lineItemState.put(lineItem.getOrderKey(), lineItems);
                    }
                } else {
                    // 删除操作
                    List<Tuple2<Integer, Double>> lineItems = lineItemState.get(lineItem.getOrderKey());
                    if (lineItems != null) {
                        // 查找并删除对应的LineItem
                        lineItems.removeIf(item -> item.f0.equals(lineItem.getLineNumber()));                  
                        // 如果列表为空，则删除整个订单的LineItem信息
                        if (lineItems.isEmpty()) {
                            lineItemState.remove(lineItem.getOrderKey());
                        } else {
                            lineItemState.put(lineItem.getOrderKey(), lineItems);
                        }
                    }
                }

                // 检查lineItem的orderKey是否与orderState中的orderKey匹配
                if (order != null && order.f0.equals(lineItem.getOrderKey())) {
                    // 计算revenue
                    double revenue = lineItem.getExtendedPrice() * (1 - lineItem.getDiscount());
                    
                    // 流出数据：l_orderKey, o_orderDate, o_shippriority, revenue, isAlive
                    out.collect(new Tuple5<>(
                        lineItem.getOrderKey(),    // l_orderKey
                        order.f1,                  // o_orderDate
                        order.f2,                  // o_shippriority
                        revenue,                   // revenue
                        lineItem.isAlive()         // isAlive
                    ));
                }
            }
        }
    }
}