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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.example.model.Order;

public class OrderProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, Boolean>, Order, Tuple4<Long, String, Integer, Boolean>> {
    
    private ValueState<Tuple4<Long, Long, String, Integer>> orderState;
    private ValueState<List<Long>> customerState;  // 使用 List 存储客户 custKey
    private MapState<Long, List<Tuple3<Long, String, Integer>>> customerOrderMapState;
    private static final LocalDate CUTOFF_DATE = LocalDate.parse("1995-03-13");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void open(Configuration parameters) throws Exception {


        // 客户状态 - 使用 List 存储 custKey
        ValueStateDescriptor<List<Long>> customerDescriptor = 
            new ValueStateDescriptor<>(
                "customerState",
                org.apache.flink.api.common.typeinfo.Types.LIST(
                    org.apache.flink.api.common.typeinfo.Types.LONG
                )
            );
        customerState = getRuntimeContext().getState(customerDescriptor);

        // 客户订单映射状态
        MapStateDescriptor<Long, List<Tuple3<Long, String, Integer>>> customerOrderMapDescriptor = 
            new MapStateDescriptor<>("customerOrderMapState",
                Types.LONG,
                Types.LIST(Types.TUPLE(Types.LONG, Types.STRING, Types.INT)));
        customerOrderMapState = getRuntimeContext().getMapState(customerOrderMapDescriptor);
    }

    @Override
    public void processElement1(
        Tuple2<Long, Boolean> customer,
        Context ctx,
        Collector<Tuple4<Long, String, Integer, Boolean>> out) throws Exception {
        
        // 获取当前客户列表
        List<Long> currentCustomers = customerState.value();
        if (currentCustomers == null) {
            currentCustomers = new ArrayList<>();
        }
        
        // 检查是否是添加或删除操作
        if (!customer.f1) {
            // 删除操作：检查 customerOrderMapState 中是否存在该客户
            if (customerOrderMapState.contains(customer.f0)) {
                // 如果存在，则从 currentCustomers 中删除
                if (currentCustomers.remove(customer.f0)) {
                    customerState.update(currentCustomers);
                }
                customerOrderMapState.remove(customer.f0);
            }
        } else {
            // 添加操作：检查 custKey 是否已存在，如果不存在则添加
            if (!currentCustomers.contains(customer.f0)) {
                currentCustomers.add(customer.f0);
                customerState.update(currentCustomers);
            }
        }

        // 获取该客户的所有订单并输出
        List<Tuple3<Long, String, Integer>> orderInfos = customerOrderMapState.get(customer.f0);
        if (orderInfos != null && !orderInfos.isEmpty()) {
            for (Tuple3<Long, String, Integer> orderInfo : orderInfos) {
                out.collect(new Tuple4<>(
                    orderInfo.f0,
                    orderInfo.f1,
                    orderInfo.f2,
                    customer.f1
                ));
            }
        }
    }

    @Override
    public void processElement2(
        Order order,
        Context ctx,
        Collector<Tuple4<Long, String, Integer, Boolean>> out) throws Exception {
        
        // 解析订单日期并检查是否在截止日期之前
        LocalDate orderDate = LocalDate.parse(order.getOrderDate(), DATE_FORMATTER);
        if (orderDate.isBefore(CUTOFF_DATE)) {
            // 过滤 o_orderdate < date '1995-03-13' 的数据
            
            // 检查是否是添加或删除操作
            if (!order.isAlive()) {
                // 删除操作：检查 customerOrderMapState 中是否存在该订单
                List<Tuple3<Long, String, Integer>> orderInfos = customerOrderMapState.get(order.getCustKey());
                if (orderInfos != null) {
                    // 如果存在，则从 orderInfos 中删除
                    if (orderInfos.remove(new Tuple3<>(
                        order.getOrderKey(),
                        order.getOrderDate(),
                        order.getShippriority()
                    ))) {
                        if (orderInfos.isEmpty()) {
                            customerOrderMapState.remove(order.getCustKey());
                        } else {
                            customerOrderMapState.put(order.getCustKey(), orderInfos);
                        }
                    }
                }
            } else {
                // 添加操作：检查 orderKey 是否已存在，如果不存在则添加
                List<Tuple3<Long, String, Integer>> orderInfos = customerOrderMapState.get(order.getCustKey());
                if (orderInfos == null) {
                    orderInfos = new ArrayList<>();
                }
                
                boolean exists = false;
                for (Tuple3<Long, String, Integer> info : orderInfos) {
                    if (info.f0.equals(order.getOrderKey())) {
                        exists = true;
                        break;
                    }
                }
                
                if (!exists) {
                    orderInfos.add(new Tuple3<>(
                        order.getOrderKey(),
                        order.getOrderDate(),
                        order.getShippriority()
                    ));
                    customerOrderMapState.put(order.getCustKey(), orderInfos);
                }
            }

             // 使用 customerOrderMapState 检查客户订单关系
             List<Long> customers = customerState.value();
             if (customers != null && customers.contains(order.getCustKey())) {
                 out.collect(new Tuple4<>(
                     order.getOrderKey(),
                     order.getOrderDate(),
                     order.getShippriority(),
                     order.isAlive()
                 ));
             }
        }
    }
} 
