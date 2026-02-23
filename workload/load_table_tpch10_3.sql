-- =============================================================================
-- 新建数据库，在 load_table_tpch10_1.sql 基础上，额外创建 part_vector 和 partsupp_vector 向量表
-- 向量表包含主键和对应的向量字段
--
-- 执行顺序：
--   1. 先生成向量 CSV 文件: python3 workload/gen_vector_csv.py
--   2. 再执行本 SQL: mysql ... < load_table_tpch10_with_vector_tables.sql
-- =============================================================================


-- =============================================================================
-- PART TABLE with vector embedding (与 load_table_tpch10_1.sql 相同)
-- =============================================================================
-- BEGIN;

-- CREATE TABLE PART (
--     p_partkey      INTEGER NOT NULL,
--     p_name         VARCHAR(55),
--     p_mfgr         VARCHAR(25),
--     p_brand        VARCHAR(10),
--     p_type         VARCHAR(25),
--     p_size         INTEGER,
--     p_container    VARCHAR(10),
--     p_retailprice  DECIMAL,
--     p_comment      VARCHAR(23),
--     text_embedding vector(768)
-- );

-- DELIMITER //
-- CREATE PROCEDURE load_part_chunks()
-- BEGIN
--     DECLARE i INT DEFAULT 0;
--     DECLARE file_path VARCHAR(500);
--     DECLARE sql_stmt TEXT;
    
--     WHILE i < 100 DO
--         SET file_path = CONCAT('/data/dzh/seekdb/workload/tmp/vector_load_tpch10/part_with_vec_', i, '.csv');
--         SET sql_stmt = CONCAT(
--             'LOAD DATA INFILE ''', file_path, ''' ',
--             'INTO TABLE part ',
--             'FIELDS TERMINATED BY ''|'' ',
--             'LINES TERMINATED BY ''\\n'' ',
--             '(@c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10) ',
--             'SET p_partkey=@c1, p_name=@c2, p_mfgr=@c3, p_brand=@c4, p_type=@c5, ',
--             'p_size=@c6, p_container=@c7, p_retailprice=@c8, p_comment=@c9, text_embedding=@c10'
--         );
        
--         SET @sql = sql_stmt;
--         PREPARE stmt FROM @sql;
--         EXECUTE stmt;
--         DEALLOCATE PREPARE stmt;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL load_part_chunks();
-- DROP PROCEDURE load_part_chunks;

-- COMMIT;

-- =============================================================================
-- PART 向量表：主键 + text_embedding
-- 使用预生成的 part_vector_*.csv 直接导入（需先执行: python3 workload/gen_vector_csv.py）
-- =============================================================================
BEGIN;

CREATE TABLE part_vector (
    p_partkey      INTEGER NOT NULL PRIMARY KEY,
    text_embedding vector(768)
);

DELIMITER //
CREATE PROCEDURE load_part_vector_chunks()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE file_path VARCHAR(500);
    DECLARE sql_stmt TEXT;
    
    WHILE i < 100 DO
        SET file_path = CONCAT('/data/dzh/seekdb/workload/tpch_10_vector/part_vector/part_vector_', i, '.csv');
        SET sql_stmt = CONCAT(
            'LOAD DATA INFILE ''', file_path, ''' ',
            'INTO TABLE part_vector ',
            'FIELDS TERMINATED BY ''|'' ',
            'LINES TERMINATED BY ''\\n'' ',
            '(@c1, @c2) SET p_partkey=@c1, text_embedding=@c2'
        );
        
        SET @sql = sql_stmt;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL load_part_vector_chunks();
DROP PROCEDURE load_part_vector_chunks;

COMMIT;

-- =============================================================================
-- PARTSUPP TABLE with vector embeddings (与 load_table_tpch10_1.sql 相同)
-- =============================================================================
BEGIN;

CREATE TABLE PARTSUPP (
    ps_partkey      INTEGER NOT NULL,
    ps_suppkey      INTEGER NOT NULL,
    ps_availqty     INTEGER,
    ps_supplycost   DECIMAL,
    ps_comment      VARCHAR(199),
    ps_image_embedding vector(96),
    ps_text_embedding vector(768)
);

DELIMITER //
CREATE PROCEDURE load_partsupp_chunks()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE file_path VARCHAR(500);
    DECLARE sql_stmt TEXT;
    
    WHILE i < 400 DO
        SET file_path = CONCAT('/data/dzh/seekdb/workload/tmp/vector_load_tpch10/partsupp_with_vec_', i, '.csv');
        SET sql_stmt = CONCAT(
            'LOAD DATA INFILE ''', file_path, ''' ',
            'INTO TABLE partsupp ',
            'FIELDS TERMINATED BY ''|'' ',
            'LINES TERMINATED BY ''\\n'' ',
            '(@c1, @c2, @c3, @c4, @c5, @c6, @c7) ',
            'SET ps_partkey=@c1, ps_suppkey=@c2, ps_availqty=@c3, ps_supplycost=@c4, ps_comment=@c5, ',
            'ps_image_embedding=NULL, ps_text_embedding=@c7'
        );
        
        SET @sql = sql_stmt;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL load_partsupp_chunks();
DROP PROCEDURE load_partsupp_chunks;

COMMIT;

-- =============================================================================
-- PARTSUPP 向量表：主键(ps_partkey, ps_suppkey) + 两个向量
-- 使用预生成的 partsupp_vector_*.csv 直接导入（需先执行: python3 workload/gen_vector_csv.py）
-- =============================================================================
BEGIN;

CREATE TABLE partsupp_vector (
    ps_partkey         INTEGER NOT NULL,
    ps_suppkey         INTEGER NOT NULL,
    ps_image_embedding vector(96),
    ps_text_embedding  vector(768),
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

DELIMITER //
CREATE PROCEDURE load_partsupp_vector_chunks()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE file_path VARCHAR(500);
    DECLARE sql_stmt TEXT;
    
    WHILE i < 400 DO
        SET file_path = CONCAT('/data/dzh/seekdb/workload/tpch_10_vector/partsupp_vector/partsupp_vector_', i, '.csv');
        SET sql_stmt = CONCAT(
            'LOAD DATA INFILE ''', file_path, ''' ',
            'INTO TABLE partsupp_vector ',
            'FIELDS TERMINATED BY ''|'' ',
            'LINES TERMINATED BY ''\\n'' ',
            '(@c1, @c2, @c3, @c4) ',
            'SET ps_partkey=@c1, ps_suppkey=@c2, ps_image_embedding=NULLIF(@c3,''''), ps_text_embedding=@c4'
        );
        
        SET @sql = sql_stmt;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL load_partsupp_vector_chunks();
DROP PROCEDURE load_partsupp_vector_chunks;

COMMIT;

-- =============================================================================
-- ORDERS TABLE (与 load_table_tpch10_1.sql 相同)
-- =============================================================================
BEGIN;

CREATE TABLE ORDERS (
    O_ORDERKEY      INTEGER NOT NULL,
    O_CUSTKEY       INTEGER NOT NULL,
    O_ORDERSTATUS   CHAR(1) NOT NULL,
    O_TOTALPRICE    DECIMAL(15,2) NOT NULL,
    O_ORDERDATE     DATE NOT NULL,
    O_ORDERPRIORITY CHAR(15) NOT NULL,
    O_CLERK         CHAR(15) NOT NULL,
    O_SHIPPRIORITY  INTEGER NOT NULL,
    O_COMMENT       VARCHAR(79) NOT NULL
);
-- PARTITION BY RANGE COLUMNS(O_ORDERDATE) (
--     PARTITION p1 VALUES LESS THAN ('1993-01-01'),
--     PARTITION p2 VALUES LESS THAN ('1994-01-01'),
--     PARTITION p3 VALUES LESS THAN ('1995-01-01'),
--     PARTITION p4 VALUES LESS THAN ('1996-01-01'),
--     PARTITION p5 VALUES LESS THAN ('1997-01-01'),
--     PARTITION p6 VALUES LESS THAN ('1998-01-01'),
--     PARTITION p7 VALUES LESS THAN ('1999-01-01'),
--     PARTITION p8 VALUES LESS THAN MAXVALUE
-- );

LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

COMMIT;

-- =============================================================================
-- LINEITEM TABLE (与 load_table_tpch10_1.sql 相同)
-- =============================================================================
BEGIN;

CREATE TABLE LINEITEM (
    L_ORDERKEY      INTEGER NOT NULL,
    L_PARTKEY       INTEGER NOT NULL,
    L_SUPPKEY       INTEGER NOT NULL,
    L_LINENUMBER    INTEGER NOT NULL,
    L_QUANTITY      DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL,
    L_DISCOUNT      DECIMAL(15,2) NOT NULL,
    L_TAX           DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG    CHAR(1) NOT NULL,
    L_LINESTATUS    CHAR(1) NOT NULL,
    L_SHIPDATE      DATE NOT NULL,
    L_COMMITDATE    DATE NOT NULL,
    L_RECEIPTDATE   DATE NOT NULL,
    L_SHIPINSTRUCT  CHAR(25) NOT NULL,
    L_SHIPMODE      CHAR(10) NOT NULL,
    L_COMMENT       VARCHAR(44) NOT NULL
);
-- PARTITION BY RANGE COLUMNS(L_SHIPDATE) (
--     PARTITION p1 VALUES LESS THAN ('1993-01-01'),
--     PARTITION p2 VALUES LESS THAN ('1994-01-01'),
--     PARTITION p3 VALUES LESS THAN ('1995-01-01'),
--     PARTITION p4 VALUES LESS THAN ('1996-01-01'),
--     PARTITION p5 VALUES LESS THAN ('1997-01-01'),
--     PARTITION p6 VALUES LESS THAN ('1998-01-01'),
--     PARTITION p7 VALUES LESS THAN ('1999-01-01'),
--     PARTITION p8 VALUES LESS THAN MAXVALUE
-- );

LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/lineitem.csv'
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

COMMIT;


BEGIN;

-- CREATE VECTOR INDEX partsupp_wiki_hnsw ON partsupp_vector (ps_text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);

-- CREATE VECTOR INDEX part_wiki_hnsw ON part_vector (text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/region.csv'
  INTO TABLE region
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;

BEGIN;

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/nation.csv'
  INTO TABLE nation
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;

BEGIN;

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/supplier.csv'
  INTO TABLE supplier
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';


COMMIT;

BEGIN;

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/customer.csv'
  INTO TABLE customer
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';


COMMIT;