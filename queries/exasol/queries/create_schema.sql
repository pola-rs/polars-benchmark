set autocommit off;
create schema if not exists tpc;
open schema tpc;

-- keys dec(11) with exception of ORDERKEY dec(12) due to scaling requirements (Clause 1.3.1 - last comment)
-- integers dec(10)
-- decimals dec(12,2)

create or replace table nation  ( n_nationkey dec(11), n_name char(25) character set ascii, n_regionkey dec(11), n_comment varchar(152) character set ascii );
create or replace table region  ( r_regionkey dec(11), r_name char(25) character set ascii, r_comment varchar(152) character set ascii );
create or replace table part  ( p_partkey dec(11), p_name varchar(55) character set ascii, p_mfgr char(25) character set ascii, p_brand char(10) character set ascii, p_type varchar(25) character set ascii, p_size dec(10), p_container char(10) character set ascii, p_retailprice decimal(12,2), p_comment varchar(23) character set ascii, distribute by p_partkey );
create or replace table supplier ( s_suppkey dec(11), s_name char(25) character set ascii, s_address varchar(40) character set ascii, s_nationkey dec(11), s_phone char(15) character set ascii, s_acctbal decimal(12,2), s_comment varchar(101) character set ascii, distribute by s_suppkey );
create or replace table partsupp ( ps_partkey dec(11), ps_suppkey dec(11), ps_availqty dec(10), ps_supplycost decimal(12,2), ps_comment varchar(199) character set ascii, distribute by ps_partkey );
create or replace table customer ( c_custkey dec(11), c_name varchar(25) character set ascii, c_address varchar(40) character set ascii, c_nationkey dec(11), c_phone char(15) character set ascii, c_acctbal decimal(12,2), c_mktsegment char(10) character set ascii, c_comment varchar(117) character set ascii, distribute by c_custkey);
create or replace table orders  ( o_orderkey dec(12), o_custkey dec(11), o_orderstatus char(1) character set ascii, o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15) character set ascii, o_clerk char(15) character set ascii, o_shippriority dec(10), o_comment varchar(79) character set ascii, distribute by o_custkey);
create or replace table lineitem ( l_orderkey dec(12), l_partkey dec(11), l_suppkey dec(11), l_linenumber dec(10), l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1) character set ascii, l_linestatus char(1) character set ascii, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25) character set ascii, l_shipmode char(10) character set ascii, l_comment varchar(44) character set ascii, distribute by l_orderkey );

commit;

select to_char(current_timestamp, 'MM/DD/YYYY HH:MI:SS.FF3') 'END_OF_CREATE_SCHEMA' from dual;
