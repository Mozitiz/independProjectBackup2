package com.example.model;

public class LineItem {
    private Long orderKey;        // L_ORDERKEY (主键的一部分, Identifier, 外键引用O_ORDERKEY)
    private Long partKey;         // L_PARTKEY (Identifier, 外键)
    private Long suppKey;         // L_SUPPKEY (Identifier, 外键)
    private Integer lineNumber;   // L_LINENUMBER (Integer, 主键的一部分)
    private double quantity;  // L_QUANTITY (Decimal)
    private double extendedPrice; // L_EXTENDEDPRICE (Decimal)
    private double discount;  // L_DISCOUNT (Decimal)
    private double tax;       // L_TAX (Decimal)
    private String returnFlag;    // L_RETURNFLAG (fixed text, size 1)
    private String lineStatus;    // L_LINESTATUS (fixed text, size 1)
    private String shipDate;      // L_SHIPDATE (Date)
    private String commitDate;    // L_COMMITDATE (Date)
    private String receiptDate;   // L_RECEIPTDATE (Date)
    private String shipInstruct;  // L_SHIPINSTRUCT (fixed text, size 25)
    private String shipMode;      // L_SHIPMODE (fixed text, size 10)
    private String comment;       // L_COMMENT (variable text, size 44)
    private boolean isAlive;      // 标记是否为有效记录

    // 无参构造函数
    public LineItem() {
    }

    public static LineItem fromString(String line) {
        try {
            // 检查是否是删除操作
            boolean isAlive = !line.startsWith("-LI");
            
            // 移除前缀（+LI或-LI）
            String[] parts = line.substring(3).split("\\|");
            
            if (parts.length < 16) {
                throw new IllegalArgumentException("Invalid line item format: " + line);
            }

            LineItem item = new LineItem();
            item.orderKey = Long.parseLong(parts[0]);
            item.partKey = Long.parseLong(parts[1]);
            item.suppKey = Long.parseLong(parts[2]);
            item.lineNumber = Integer.parseInt(parts[3]);
            item.quantity = Double.parseDouble(parts[4]);
            item.extendedPrice = Double.parseDouble(parts[5]);
            item.discount = Double.parseDouble(parts[6]);
            item.tax = Double.parseDouble(parts[7]);
            item.returnFlag = parts[8];
            item.lineStatus = parts[9];
            item.shipDate = parts[10];
            item.commitDate = parts[11];
            item.receiptDate = parts[12];
            item.shipInstruct = parts[13];
            item.shipMode = parts[14];
            item.comment = parts[15];
            item.isAlive = isAlive;

            return item;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing LineItem: " + line, e);
        }
    }

    // Getters
    public Long getOrderKey() { return orderKey; }
    public Long getPartKey() { return partKey; }
    public Long getSuppKey() { return suppKey; }
    public Integer getLineNumber() { return lineNumber; }
    public double getQuantity() { return quantity; }
    public double getExtendedPrice() { return extendedPrice; }
    public double getDiscount() { return discount; }
    public double getTax() { return tax; }
    public String getReturnFlag() { return returnFlag; }
    public String getLineStatus() { return lineStatus; }
    public String getShipDate() { return shipDate; }
    public String getCommitDate() { return commitDate; }
    public String getReceiptDate() { return receiptDate; }
    public String getShipInstruct() { return shipInstruct; }
    public String getShipMode() { return shipMode; }
    public String getComment() { return comment; }
    public boolean isAlive() { return isAlive; }

    @Override
    public String toString() {
        return String.format("LineItem{orderKey=%d, partKey=%d, suppKey=%d, lineNumber=%d, quantity=%s, " +
                "extendedPrice=%s, discount=%s, tax=%s, returnFlag='%s', lineStatus='%s', " +
                "shipDate=%s, commitDate=%s, receiptDate=%s, shipInstruct='%s', shipMode='%s', " +
                "comment='%s', isAlive=%b}",
                orderKey, partKey, suppKey, lineNumber, quantity,
                extendedPrice, discount, tax, returnFlag, lineStatus,
                shipDate, commitDate, receiptDate, shipInstruct, shipMode,
                comment, isAlive);
    }
} 