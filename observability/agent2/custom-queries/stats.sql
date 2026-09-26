SELECT
    (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database())          AS connections,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections')               AS max_connections,
    (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database())       AS xact_commit,
    (SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database())     AS xact_rollback,
    pg_database_size(current_database())                                                AS db_size,
    (SELECT count(*) FROM pg_locks)                                                     AS locks,
    COALESCE(pg_total_relation_size(to_regclass('public.billing_record')), 0)           AS billing_size,
    COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'billing_record'), 0) AS billing_rows;
