package com.example.model;

public class Order {
    private Long orderKey;
    private Long custKey;
    private String orderStatus;
    private double totalPrice;
    private String orderDate;
    private String orderPriority;
    private String clerk;
    private int shippriority;
    private String comment;
    private boolean isAlive;

    public Order(Long orderKey, Long custKey, String orderStatus, double totalPrice, String orderDate,
                String orderPriority, String clerk, int shippriority, String comment, boolean isAlive) {
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.orderStatus = orderStatus;
        this.totalPrice = totalPrice;
        this.orderDate = orderDate;
        this.orderPriority = orderPriority;
        this.clerk = clerk;
        this.shippriority = shippriority;
        this.comment = comment;
        this.isAlive = isAlive;
    }

    public static Order fromString(String value) {
        String[] parts = value.split("\\|");
        if (parts.length != 9 || (!parts[0].startsWith("+OR") && !parts[0].startsWith("-OR"))) {
            throw new IllegalArgumentException("Invalid order format: " + value);
        }
        
        boolean isAlive = parts[0].startsWith("+OR");
        long orderKey = Long.parseLong(parts[0].substring(3));
        long custKey = Long.parseLong(parts[1]);
        String orderStatus = parts[2];
        double totalPrice = Double.parseDouble(parts[3]);
        String orderDate = parts[4];
        String orderPriority = parts[5];
        String clerk = parts[6];
        int shippriority = Integer.parseInt(parts[7]);
        String comment = parts[8];
        
        return new Order(orderKey, custKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shippriority, comment, isAlive);
    }

    public Long getOrderKey() {
        return orderKey;
    }

    public Long getCustKey() {
        return custKey;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public String getOrderPriority() {
        return orderPriority;
    }

    public String getClerk() {
        return clerk;
    }

    public int getShippriority() {
        return shippriority;
    }

    public String getComment() {
        return comment;
    }

    public boolean isAlive() {
        return isAlive;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderKey=" + orderKey +
                ", custKey=" + custKey +
                ", orderStatus='" + orderStatus + '\'' +
                ", totalPrice=" + totalPrice +
                ", orderDate='" + orderDate + '\'' +
                ", orderPriority='" + orderPriority + '\'' +
                ", clerk='" + clerk + '\'' +
                ", shippriority=" + shippriority +
                ", comment='" + comment + '\'' +
                ", isAlive=" + isAlive +
                '}';
    }
} 