select reldatabase,relname,relkind from mo_catalog.mo_tables where relname = 'mo_increment_columns' and account_id = 0 order by reldatabase;
select relname,relkind from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and account_id = 0 and relname not like '__mo_index_unique__%' and relname != '__mo_account_lock' order by relname;