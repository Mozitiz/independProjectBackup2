import pandas as pd
import sqlite3
import re
from datetime import datetime
import time

class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self._create_tables()
    
    def _create_tables(self):
        """创建数据库表"""
        self.conn.execute('''
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name TEXT,
            c_address TEXT,
            c_nationkey INTEGER,
            c_phone TEXT,
            c_acctbal REAL,
            c_mktsegment TEXT,
            c_comment TEXT
        )
        ''')
        
        self.conn.execute('''
        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER,
            o_orderstatus TEXT,
            o_totalprice REAL,
            o_orderdate TEXT,
            o_orderpriority TEXT,
            o_clerk TEXT,
            o_shippriority INTEGER,
            o_comment TEXT,
            FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)
        )
        ''')
        
        self.conn.execute('''
        CREATE TABLE lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity REAL,
            l_extendedprice REAL,
            l_discount REAL,
            l_tax REAL,
            l_returnflag TEXT,
            l_linestatus TEXT,
            l_shipdate TEXT,
            l_commitdate TEXT,
            l_receiptdate TEXT,
            l_shipinstruct TEXT,
            l_shipmode TEXT,
            l_comment TEXT,
            PRIMARY KEY (l_orderkey, l_linenumber),
            FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey)
        )
        ''')
    
    def close(self):
        """关闭数据库连接"""
        self.conn.close()

class DataParser:
    @staticmethod
    def parse_tpch_line(line):
        """解析 TPC-H 格式的数据行"""
        match = re.match(r'([+-])([A-Z]{2})(.*)', line.strip())
        if not match:
            return None, None, None
        
        operation, table, data = match.groups()
        data_parts = data.strip().split('|')
        
        if table == 'CU':  # Customer
            return operation, 'customer', {
                'c_custkey': int(data_parts[0]),
                'c_name': data_parts[1],
                'c_address': data_parts[2],
                'c_nationkey': int(data_parts[3]),
                'c_phone': data_parts[4],
                'c_acctbal': float(data_parts[5]),
                'c_mktsegment': data_parts[6],
                'c_comment': data_parts[7]
            }
        elif table == 'OR':  # Orders
            return operation, 'orders', {
                'o_orderkey': int(data_parts[0]),
                'o_custkey': int(data_parts[1]),
                'o_orderstatus': data_parts[2],
                'o_totalprice': float(data_parts[3]),
                'o_orderdate': data_parts[4],
                'o_orderpriority': data_parts[5],
                'o_clerk': data_parts[6],
                'o_shippriority': int(data_parts[7]),
                'o_comment': data_parts[8]
            }
        elif table == 'LI':  # Lineitem
            return operation, 'lineitem', {
                'l_orderkey': int(data_parts[0]),
                'l_partkey': int(data_parts[1]),
                'l_suppkey': int(data_parts[2]),
                'l_linenumber': int(data_parts[3]),
                'l_quantity': float(data_parts[4]),
                'l_extendedprice': float(data_parts[5]),
                'l_discount': float(data_parts[6]),
                'l_tax': float(data_parts[7]),
                'l_returnflag': data_parts[8],
                'l_linestatus': data_parts[9],
                'l_shipdate': data_parts[10],
                'l_commitdate': data_parts[11],
                'l_receiptdate': data_parts[12],
                'l_shipinstruct': data_parts[13],
                'l_shipmode': data_parts[14],
                'l_comment': data_parts[15]
            }
        
        return None, None, None

class QueryExecutor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def process_data(self, input_file):
        """处理输入数据文件"""
        data_processing_start = time.time()
        
        with open(input_file, 'r') as f:
            for line in f:
                operation, table, data = DataParser.parse_tpch_line(line)
                if not operation or not table or not data:
                    continue
                
                if operation == '+':
                    self._insert_data(table, data)
                elif operation == '-':
                    self._delete_data(table, data)
        
        return time.time() - data_processing_start
    
    def _insert_data(self, table, data):
        """插入数据到指定表"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        values = tuple(data.values())
        
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.db_manager.conn.execute(query, values)
    
    def _delete_data(self, table, data):
        """从指定表删除数据"""
        if table == 'customer':
            self.db_manager.conn.execute("DELETE FROM customer WHERE c_custkey = ?", 
                                       (data['c_custkey'],))
        elif table == 'orders':
            self.db_manager.conn.execute("DELETE FROM orders WHERE o_orderkey = ?", 
                                       (data['o_orderkey'],))
        elif table == 'lineitem':
            self.db_manager.conn.execute(
                "DELETE FROM lineitem WHERE l_orderkey = ? AND l_linenumber = ?",
                (data['l_orderkey'], data['l_linenumber'])
            )
    
    def execute_query(self):
        """执行查询并返回结果"""
        query = """
        SELECT 
            l_orderkey, o_orderdate, o_shippriority,
            SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM 
            Lineitem,
            Customer,
            Orders
        WHERE 
                C_mktsegment = 'AUTOMOBILE'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < date('1995-03-13') 
            AND l_shipdate > date('1995-03-13')
        GROUP BY
            l_orderkey, o_orderdate, o_shippriority
        """
        return pd.read_sql_query(query, self.db_manager.conn)

def format_time(seconds):
    """将秒数转换为分和秒的格式"""
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes}分 {remaining_seconds:.2f}秒"

def process_tpch_data(input_file):
    """处理 TPC-H 格式的数据文件并执行查询"""
    start_time = time.time()
    
    # 初始化数据库和查询执行器
    db_manager = DatabaseManager()
    query_executor = QueryExecutor(db_manager)
    
    # 处理数据
    data_processing_time = query_executor.process_data(input_file)
    
    # 执行查询
    query_start = time.time()
    result = query_executor.execute_query()
    query_time = time.time() - query_start
    
    # 关闭数据库连接
    db_manager.close()
    
    # 计算总时间
    total_time = time.time() - start_time
    
    return result, {
        'total_time': total_time,
        'data_processing_time': data_processing_time,
        'query_time': query_time
    }

if __name__ == "__main__":
    # 处理数据并执行查询
    result, timing_info = process_tpch_data('input_data_all.csv')
    
    # 打印结果
    print(f"查询结果共 {len(result)} 条记录:")
    print(result.head(10))
    
    # 打印时间信息
    print("\n运行时间统计:")
    print(f"总运行时间: {format_time(timing_info['total_time'])}")
    print(f"数据处理时间: {format_time(timing_info['data_processing_time'])}")
    print(f"查询执行时间: {format_time(timing_info['query_time'])}")
    
    # 保存结果到 CSV 文件
    result.to_csv('query_result.csv', index=False)
    print("结果已保存到 query_result.csv") 