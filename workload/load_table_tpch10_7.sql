-- BEGIN;

-- -- PART TABLE with vector embedding
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

-- -- Load part table from chunk files (100 files: part_with_vec_0.csv to part_with_vec_99.csv)
-- -- Using stored procedure to loop through all chunk files
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

-- BEGIN;

-- CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
--                             R_NAME       CHAR(25) NOT NULL,
--                             R_COMMENT    VARCHAR(152));

--   LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/region.csv'
--   INTO TABLE region
--   FIELDS TERMINATED BY '|'
--   LINES TERMINATED BY '\n';

-- COMMIT;

-- BEGIN;

-- CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
--                             N_NAME       CHAR(25) NOT NULL,
--                             N_REGIONKEY  INTEGER NOT NULL,
--                             N_COMMENT    VARCHAR(152));

--   LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/nation.csv'
--   INTO TABLE nation
--   FIELDS TERMINATED BY '|'
--   LINES TERMINATED BY '\n';

-- COMMIT;

-- BEGIN;

-- CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
--                              S_NAME        CHAR(25) NOT NULL,
--                              S_ADDRESS     VARCHAR(40) NOT NULL,
--                              S_NATIONKEY   INTEGER NOT NULL,
--                              S_PHONE       CHAR(15) NOT NULL,
--                              S_ACCTBAL     DECIMAL(15,2) NOT NULL,
--                              S_COMMENT     VARCHAR(101) NOT NULL);

--   LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/supplier.csv'
--   INTO TABLE supplier
--   FIELDS TERMINATED BY '|'
--   LINES TERMINATED BY '\n';


-- COMMIT;

-- BEGIN;

-- CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
--                              C_NAME        VARCHAR(25) NOT NULL,
--                              C_ADDRESS     VARCHAR(40) NOT NULL,
--                              C_NATIONKEY   INTEGER NOT NULL,
--                              C_PHONE       CHAR(15) NOT NULL,
--                              C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
--                              C_MKTSEGMENT  CHAR(10) NOT NULL,
--                              C_COMMENT     VARCHAR(117) NOT NULL);

--   LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/customer.csv'
--   INTO TABLE customer
--   FIELDS TERMINATED BY '|'
--   LINES TERMINATED BY '\n';


-- COMMIT;

-- BEGIN;

-- -- PARTSUPP TABLE with vector embeddings
-- CREATE TABLE PARTSUPP (
--     ps_partkey      INTEGER NOT NULL,
--     ps_suppkey      INTEGER NOT NULL,
--     ps_availqty     INTEGER,
--     ps_supplycost   DECIMAL,
--     ps_comment      VARCHAR(199),
--     ps_image_embedding vector(96),
--     ps_text_embedding vector(768)
-- );

-- -- Load partsupp table from chunk files (400 files: partsupp_with_vec_0.csv to partsupp_with_vec_399.csv)
-- -- Using stored procedure to loop through all chunk files
-- DELIMITER //
-- CREATE PROCEDURE load_partsupp_chunks()
-- BEGIN
--     DECLARE i INT DEFAULT 0;
--     DECLARE file_path VARCHAR(500);
--     DECLARE sql_stmt TEXT;
    
--     WHILE i < 400 DO
--         SET file_path = CONCAT('/data/dzh/seekdb/workload/tmp/vector_load_tpch10/partsupp_with_vec_', i, '.csv');
--         SET sql_stmt = CONCAT(
--             'LOAD DATA INFILE ''', file_path, ''' ',
--             'INTO TABLE partsupp ',
--             'FIELDS TERMINATED BY ''|'' ',
--             'LINES TERMINATED BY ''\\n'' ',
--             '(@c1, @c2, @c3, @c4, @c5, @c6, @c7) ',
--             'SET ps_partkey=@c1, ps_suppkey=@c2, ps_availqty=@c3, ps_supplycost=@c4, ps_comment=@c5, ',
--             'ps_image_embedding=NULL, ps_text_embedding=@c7'
--         );
        
--         SET @sql = sql_stmt;
--         PREPARE stmt FROM @sql;
--         EXECUTE stmt;
--         DEALLOCATE PREPARE stmt;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL load_partsupp_chunks();
-- DROP PROCEDURE load_partsupp_chunks;

-- COMMIT;

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
    O_COMMENT       VARCHAR(79) NOT NULL,
    -- 核心限制：主键必须包含所有的分区键 (O_ORDERKEY 和 O_ORDERDATE)
    PRIMARY KEY (O_ORDERKEY, O_ORDERDATE)
)
-- 一级分区：按 O_ORDERKEY 进行 HASH 分区（假设分为 16 个分区）
PARTITION BY HASH(O_ORDERKEY) 
-- 二级分区：按 O_ORDERDATE 进行 RANGE 分区
SUBPARTITION BY RANGE COLUMNS(O_ORDERDATE)
SUBPARTITION TEMPLATE (
    SUBPARTITION p1 VALUES LESS THAN ('1993-01-01'),
    SUBPARTITION p2 VALUES LESS THAN ('1994-01-01'),
    SUBPARTITION p3 VALUES LESS THAN ('1995-01-01'),
    SUBPARTITION p4 VALUES LESS THAN ('1996-01-01'),
    SUBPARTITION p5 VALUES LESS THAN ('1997-01-01'),
    SUBPARTITION p6 VALUES LESS THAN ('1998-01-01'),
    SUBPARTITION p7 VALUES LESS THAN ('1999-01-01'),
    SUBPARTITION p8 VALUES LESS THAN (MAXVALUE)
)
PARTITIONS 32;

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/orders.csv'
  INTO TABLE orders
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';


COMMIT;

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
    L_COMMENT       VARCHAR(44) NOT NULL,
    -- 重要：主键必须包含所有分区键（L_ORDERKEY 和 L_SHIPDATE）
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER, L_SHIPDATE)
)
-- 一级分区：按 L_ORDERKEY 进行 HASH 打散
PARTITION BY HASH(L_ORDERKEY) 
-- 二级分区：按 L_SHIPDATE 进行 RANGE 范围划分
SUBPARTITION BY RANGE COLUMNS(L_SHIPDATE)
SUBPARTITION TEMPLATE (
    SUBPARTITION p1 VALUES LESS THAN ('1993-01-01'),
    SUBPARTITION p2 VALUES LESS THAN ('1994-01-01'),
    SUBPARTITION p3 VALUES LESS THAN ('1995-01-01'),
    SUBPARTITION p4 VALUES LESS THAN ('1996-01-01'),
    SUBPARTITION p5 VALUES LESS THAN ('1997-01-01'),
    SUBPARTITION p6 VALUES LESS THAN ('1998-01-01'),
    SUBPARTITION p7 VALUES LESS THAN ('1999-01-01'),
    SUBPARTITION p8 VALUES LESS THAN (MAXVALUE)
)
PARTITIONS 32; 

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch10/lineitem.csv'
  INTO TABLE lineitem
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;
