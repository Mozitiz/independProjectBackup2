package com.example.process;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class ResultRevenueSink extends RichSinkFunction<Tuple4<Long, String, Integer, Double>> {
    private static final String OUTPUT_FILE = "query_result.csv";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // 用于存储每个key的最新结果
    private Map<String, Tuple4<Long, String, Integer, Double>> resultCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        resultCache = new HashMap<>();
    }

    @Override
    public void invoke(Tuple4<Long, String, Integer, Double> value, Context context) throws Exception {
        // 生成复合键
        String compositeKey = value.f0 + "_" + value.f1 + "_" + value.f2;
        
        // 更新缓存
        resultCache.put(compositeKey, value);
    }

    @Override
    public void finish() throws Exception {
        // 输出最终汇总结果到控制台
        System.out.println("\n最终收入结果汇总 (每个订单的最新结果):");
        System.out.println("----------------------------------------");
        System.out.println("订单ID\t\t订单日期\t\t优先级\t\t总收入");
        System.out.println("----------------------------------------");
        resultCache.forEach((key, value) -> 
            System.out.printf("%-12d\t%-12s\t%-8d\t%.2f%n",
                value.f0, value.f1, value.f2, value.f3));
        System.out.println("----------------------------------------");
        System.out.printf("总订单数: %d%n", resultCache.size());
        System.out.println("----------------------------------------\n");
        
        // 写入最终结果到CSV文件
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {
            // 写入表头
            writer.write("order_key,order_date,shipping_priority,total_revenue,process_time\n");
            
            // 获取当前时间
            String processTime = LocalDateTime.now().format(DATE_FORMATTER);
            
            // 写入所有结果
            for (Tuple4<Long, String, Integer, Double> value : resultCache.values()) {
                String line = String.format("%d,%s,%d,%.2f,%s\n",
                    value.f0,    // order_key
                    value.f1,    // order_date
                    value.f2,    // shipping_priority
                    value.f3,    // total_revenue
                    processTime  // 处理时间
                );
                writer.write(line);
            }
        }
        
        // 清理缓存
        resultCache.clear();
    }
} 