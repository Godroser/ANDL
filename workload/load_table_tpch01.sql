BEGIN;

  CREATE TABLE PART (
      p_partkey      INTEGER NOT NULL,
      p_name         VARCHAR(55),
      p_mfgr         VARCHAR(25),
      p_brand        VARCHAR(10),
      p_type         VARCHAR(25),
      p_size         INTEGER,
      p_container    VARCHAR(10),
      p_retailprice  DECIMAL,
      p_comment      VARCHAR(23),
      text_embedding vector(768)
  );

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/part.csv'
  INTO TABLE part
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';


COMMIT;

BEGIN;

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/region.csv'
  INTO TABLE region
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;

BEGIN;

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/nation.csv'
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

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/supplier.csv'
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

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/customer.csv'
  INTO TABLE customer
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;

BEGIN;

CREATE TABLE PARTSUPP (
            ps_partkey      INTEGER NOT NULL,
            ps_suppkey      INTEGER NOT NULL,
            ps_availqty     INTEGER,
            ps_supplycost   DECIMAL,
            ps_comment      VARCHAR(199),
            ps_text_embedding vector(768)
        );

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/partsupp.csv'
  INTO TABLE partsupp
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n'
  (@col1, @col2, @col3, @col4, @col5, @col6, @col7)
  SET ps_partkey = @col1,
      ps_suppkey = @col2,
      ps_availqty = @col3,
      ps_supplycost = @col4,
      ps_comment = @col5,
      ps_text_embedding = @col7;

COMMIT;

BEGIN;

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/orders.csv'
  INTO TABLE orders
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;

BEGIN;

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);

  LOAD DATA INFILE '/data/dzh/seekdb/workload/tpch0.1_export/lineitem.csv'
  INTO TABLE lineitem
  FIELDS TERMINATED BY '|'
  LINES TERMINATED BY '\n';

COMMIT;
