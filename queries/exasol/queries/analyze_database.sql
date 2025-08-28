open schema tpc;

analyze database estimate statistics;
commit;

select to_char(current_timestamp, 'MM/DD/YYYY HH:MI:SS.FF3') 'END_OF_OPTIMIZATION' from dual;
