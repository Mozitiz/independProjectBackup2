package com.example.model;

public class Customer {
    private Long custKey;
    private String name;
    private String address;
    private String nationKey;
    private String phone;
    private Double accountBalance;
    private String mktSegment;
    private String comment;
    private boolean isAlive;

    public Customer(Long custKey, String name, String address, String nationKey, String phone, 
                   Double accountBalance, String mktSegment, String comment, boolean isAlive) {
        this.custKey = custKey;
        this.name = name;
        this.address = address;
        this.nationKey = nationKey;
        this.phone = phone;
        this.accountBalance = accountBalance;
        this.mktSegment = mktSegment;
        this.comment = comment;
        this.isAlive = isAlive;
    }

    public static Customer fromString(String value) {
        if (!value.startsWith("+CU") && !value.startsWith("-CU")) {
            throw new IllegalArgumentException("Invalid customer record format");
        }

        String[] fields = value.split("\\|");
        if (fields.length < 8) {
            throw new IllegalArgumentException("Invalid customer record format: insufficient fields");
        }

        return new Customer(
            Long.parseLong(fields[0].substring(3)),  // 从+CU或-CU后提取custKey
            fields[1],                               // name
            fields[2],                               // address
            fields[3],                               // nationKey
            fields[4],                               // phone
            Double.parseDouble(fields[5]),           // accountBalance
            fields[6],                               // mktSegment
            fields[7],                               // comment
            value.startsWith("+CU")                  // isAlive: +CU为true，-CU为false
        );
    }

    public Long getCustKey() {
        return custKey;
    }

    public String getName() {
        return name;
    }

    public String getAddress() {
        return address;
    }

    public String getNationKey() {
        return nationKey;
    }

    public String getPhone() {
        return phone;
    }

    public Double getAccountBalance() {
        return accountBalance;
    }

    public String getMktSegment() {
        return mktSegment;
    }

    public String getComment() {
        return comment;
    }

    public boolean isAlive() {
        return isAlive;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custKey=" + custKey +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", nationKey='" + nationKey + '\'' +
                ", phone='" + phone + '\'' +
                ", accountBalance=" + accountBalance +
                ", mktSegment='" + mktSegment + '\'' +
                ", comment='" + comment + '\'' +
                ", isAlive=" + isAlive +
                '}';
    }
} 