package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.model.Customer;
import com.example.model.LineItem;
import com.example.model.Order;
import com.example.process.CustomerProcessFunction;
import com.example.process.LineItemProcessFunction;
import com.example.process.OrderProcessFunction;
import com.example.process.ResultRevenueSink;
import com.example.process.RevenueAggregationFunction;

public class TPCHQuery3 {
    public static void main(String[] args) throws Exception {
        // build the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the data

        // DataStream<String> inputStream = env.readTextFile("input_data_all.csv");

        DataStream<String> inputStream = env.readTextFile("input_data_all.csv")
            .setParallelism(1);

        // DataStream<String> inputStream = env.readTextFile("/Users/mozitizi/Documents/independentProject/input_data_all1G.csv")
        //     .setParallelism(8);

        // get the customer data stream
        DataStream<Customer> customerStream = inputStream
            .filter(value -> value.startsWith("+CU") || value.startsWith("-CU"))
            .map(value -> Customer.fromString(value));

        // get the order data stream
        DataStream<Order> orderStream = inputStream
            .filter(value -> value.startsWith("+OR") || value.startsWith("-OR"))
            .map(value -> Order.fromString(value));

        // get the lineItem data stream
        DataStream<LineItem> lineItemStream = inputStream
            .filter(value -> value.startsWith("+LI") || value.startsWith("-LI"))
            .map(value -> LineItem.fromString(value));
            

        // process the customer data stream, only keep the records of AUTOMOBILE, and output custKey and isAlive status
        DataStream<Tuple2<Long, Boolean>> custProcessStream = customerStream
            .keyBy(Customer::getCustKey)  // group by custKey
            .process(new CustomerProcessFunction())
            .name("Customer Process Function"); 

        // process the order data stream, output orderKey, orderDate and shippriority
        DataStream<Tuple4<Long, String, Integer, Boolean>> orderProcessStream = custProcessStream
            .connect(orderStream)  // connect the processed customer stream
            .keyBy(
                tuple -> tuple.f0,    // the key selector of the customer stream (the first element of the Tuple2 is custKey)
                Order::getCustKey     // the key selector of the order stream, use custKey to partition
            )
            .process(new OrderProcessFunction())
            .name("Order Process Function");
        // process the lineItem data stream
        DataStream<Tuple5<Long, String, Integer, Double, Boolean>> lineitemProcessStream = orderProcessStream
            .connect(lineItemStream)    // connect the lineItem stream
            .keyBy(
                tuple -> tuple.f0,                    // the key selector of the order stream (the first element of the Tuple4 is orderKey)
                LineItem::getOrderKey                 // the key selector of the lineItem stream
            )
            .process(new LineItemProcessFunction())
            .name("LineItem Process Function");

        // group by l_orderkey, o_orderdate, o_shippriority and calculate the total revenue
        DataStream<Tuple4<Long, String, Integer, Double>> aggregationStream = lineitemProcessStream
            .keyBy(value -> value.f0 + "_" + value.f1 + "_" + value.f2)
            .process(new RevenueAggregationFunction())
            .name("Aggregation Process Function");

        DataStream<Tuple4<Long, String, Integer, Double>> rebalancedStream = aggregationStream
            .rebalance();

        // use ResultRevenueSink to output the result
        rebalancedStream
            .addSink(new ResultRevenueSink())
            .name("Data Sink")
            .setParallelism(1);

        // System.out.println("Transport mode revenue intermediate results:");
        // resultStream.print();

        System.out.println("Starting Flink job...");
        env.execute("TPC-H Query 3");
    }
} 