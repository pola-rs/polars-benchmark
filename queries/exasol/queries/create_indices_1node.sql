set autocommit off;
open schema tpc;

enforce local index on lineitem (l_suppkey);
enforce local index on lineitem (l_partkey, l_suppkey); 
enforce local index on lineitem (l_partkey); 
enforce local index on lineitem (l_orderkey);
enforce local index on nation (n_nationkey);
enforce local index on region (r_regionkey);
enforce local index on supplier (s_suppkey);
enforce local index on supplier (s_nationkey);
enforce local index on customer (c_custkey);
enforce local index on customer (c_nationkey);
enforce local index on part (p_partkey);
enforce local index on partsupp (ps_partkey, ps_suppkey);
enforce local index on partsupp (ps_partkey);
enforce local index on partsupp (ps_suppkey);
enforce local index on orders (o_orderkey);
enforce local index on orders (o_custkey);
commit;

select to_char(current_timestamp, 'MM/DD/YYYY HH:MI:SS.FF3') 'END_OF_OPTIMIZATION' from dual;
