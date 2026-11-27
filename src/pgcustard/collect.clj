(ns pgcustard.collect
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [pgcustard.literals :as literals]))

(defn- tagged [query-name sql]
  (str sql "\n/*application='pgcustard',query='" query-name "'*/"))

(def session-counts-query
  "select count(*) filter (where state = 'active')                                        as active_sessions,
          count(*) filter (where state in ('idle in transaction',
                                           'idle in transaction (aborted)'))              as idle_in_transaction_sessions,
          count(*) filter (where state = 'idle')                                          as idle_sessions
   from pg_stat_activity
   where pid != pg_backend_pid()
     and datname = current_database()")

(def database-counters-query
  "select blks_hit, blks_read, deadlocks, stats_reset
   from pg_stat_database
   where datname = current_database()")

(def xid-usage-query
  "select d.datname as worst_database,
          age(d.datfrozenxid) as cluster_xid_age,
          age((select datfrozenxid from pg_database
               where datname = current_database())) as db_xid_age
   from pg_database d
   where d.datname not in ('template0', 'template1')
   order by age(d.datfrozenxid) desc
   limit 1")

(def autovacuum-workers-query
  "select (select count(*) from pg_stat_activity
            where backend_type = 'autovacuum worker')         as autovacuum_workers,
          current_setting('autovacuum_max_workers')::int      as autovacuum_max_workers,
          current_setting('autovacuum_freeze_max_age')::int   as autovacuum_freeze_max_age")

(defn xmin-holder-query [slot-inactive-since? query-id?]
  (str
   "with own_xid as (
      select pid, max(age(transactionid)) as xid_age
        from pg_locks
       where locktype = 'transactionid'
         and mode = 'ExclusiveLock'
         and granted
       group by pid
    ),
    holders as (
      select case when a.backend_type = 'walsender' then 'standby feedback'
                  else 'backend'
             end                                                              as kind,
             a.pid::text                                                      as holder,
             greatest(age(a.backend_xmin), o.xid_age)                         as xid_age,
             extract(epoch from now() - a.xact_start)::int                    as held_secs,
             nullif(a.state, '')                                              as state,
             nullif(coalesce(nullif(a.application_name, ''), a.usename::text), '')
                                                                              as who,
             "
   (if query-id?
     "nullif(a.query_id, 0)"
     "null::bigint")
   " as query_id
        from pg_stat_activity a
        left join own_xid o on o.pid = a.pid
       where a.pid <> pg_backend_pid()
         and (a.backend_xmin is not null or o.xid_age is not null)
         and coalesce(a.backend_type, '') <> 'autovacuum worker'
         and (a.datname = current_database()
              or a.datname is null
              or a.backend_type = 'walsender')

      union all

      select case when s.slot_type = 'logical' then 'logical slot'
                  else 'replication slot'
             end,
             s.slot_name::text,
             age(s.xmin),
             "
   (if slot-inactive-since?
     "extract(epoch from now() - s.inactive_since)::int"
     "null::int")
   ",
             case when s.active then 'active' else 'inactive' end,
             coalesce(s.plugin::text, s.slot_type),
             null::bigint
        from pg_replication_slots s
       where s.xmin is not null

      union all

      select 'prepared transaction',
             p.gid::text,
             age(p.transaction),
             extract(epoch from now() - p.prepared)::int,
             'prepared',
             p.owner::text,
             null::bigint
        from pg_prepared_xacts p
       where p.database = current_database()
    )
    select kind, holder, xid_age, held_secs, state, who, query_id
      from holders
     where xid_age > 0
     order by xid_age desc
     limit 1"))

(def checkpoint-query-pre17
  "select checkpoints_timed, checkpoints_req, stats_reset,
          extract(epoch from current_setting('checkpoint_timeout')::interval)::int as checkpoint_timeout_s
   from pg_stat_bgwriter")

(def checkpoint-query-17
  "select num_timed as checkpoints_timed, num_requested as checkpoints_req, stats_reset,
          extract(epoch from current_setting('checkpoint_timeout')::interval)::int as checkpoint_timeout_s
   from pg_stat_checkpointer")

(def checkpoint-query-18
  "select num_timed as checkpoints_timed, num_requested as checkpoints_req,
          num_done as checkpoints_done, stats_reset,
          extract(epoch from current_setting('checkpoint_timeout')::interval)::int as checkpoint_timeout_s
   from pg_stat_checkpointer")

(def ^:private ignored-schemas
  ["pg_catalog" "information_schema" "pg_toast"
   "_timescaledb_catalog" "_timescaledb_cache" "_timescaledb_config"
   "_timescaledb_functions" "_timescaledb_debug"
   "google_ml" "pgsnap"])

(def ^:private ignored-schema-list
  (str "(" (str/join ", " (map #(str "'" % "'") ignored-schemas)) ")"))

(defn- collected-table-sql [schema table]
  (str schema " not in " ignored-schema-list "
        and not (" schema " = '_timescaledb_internal' and starts_with(" table ", 'bgw_'))"))

(def ^:private exclusive-locks-cte
  "exclusive_locks as materialized (
     select coalesce(array_agg(distinct l.relation), '{}'::oid[]) as relations
       from pg_locks l
      where l.locktype = 'relation'
        and l.mode = 'AccessExclusiveLock'
        and l.database = (select oid from pg_database where datname = current_database())
   )")

(def ^:private chunk-ctes
  "hypertables as materialized (
     select h.id,
            c.oid,
            coalesce(ca.user_view_schema, h.schema_name) as schema_name,
            coalesce(ca.user_view_name, h.table_name)    as table_name
       from _timescaledb_catalog.hypertable h
       join pg_namespace n on n.nspname = h.schema_name
       join pg_class c on c.relnamespace = n.oid and c.relname = h.table_name and c.relkind = 'r'
       left join _timescaledb_catalog.continuous_agg ca on ca.mat_hypertable_id = h.id
      where h.compression_state <> 2
   ),
   chunks as materialized (
     select h.oid as hypertable, c.oid, c.relname, c.reltuples, s.compress_relid
       from hypertables h
       join pg_inherits inh on inh.inhparent = h.oid
       join pg_class c on c.oid = inh.inhrelid and c.relkind = 'r'
       left join _timescaledb_catalog.compression_settings s on s.relid = c.oid
   ),
   compression_tables as materialized (
     select compress_relid as oid
       from chunks
      where compress_relid is not null
     union
     select c.oid
       from _timescaledb_catalog.hypertable h
       join pg_namespace n on n.nspname = h.schema_name
       join pg_class c on c.relnamespace = n.oid and c.relname = h.table_name
      where h.compression_state = 2
   )")

(def ^:private hidden-tables-cte
  "hidden_tables as materialized (
     select oid
       from compression_tables
     union all
     select oid
       from chunks
      where oid <> all (?::oid[])
   )")

(def ^:private chunks-tracked-per-hypertable 3)

(def ^:private chunk-vacuum-floor 1000)

(def ^:private chunk-analyze-floor 100)

(def ^:private chunk-density-min-rows 1000)

(def ^:private chunk-excess-floor (* 10 1024 1024))

(def ^:private tracked-chunks-cte
  (str
   "chunk_stats as materialized (
     select ch.hypertable,
            ch.oid,
            t.n_tup_ins,
            t.n_tup_upd,
            t.n_tup_hot_upd,
            t.n_tup_del,
            greatest(ch.reltuples::numeric, z.live_tuples)        as rows_estimate,
            greatest(t.n_dead_tup, t.n_ins_since_vacuum)          as vacuum_debt,
            t.n_mod_since_analyze                                 as analyze_debt,
            coalesce(z.main_bytes, 0) + coalesce(z.index_bytes, 0) as bytes,
            ch.reltuples
       from chunks ch
       join user_tables t on t.relid = ch.oid
       join sizes z on z.oid = ch.oid
   ),
   chunk_density as (
     select hypertable, percentile_cont(0.5) within group (order by bytes / reltuples) as bytes_per_row
       from chunk_stats
      where reltuples >= " chunk-density-min-rows "
      group by hypertable
   ),
   chunk_problems as (
     select s.hypertable,
            s.oid,
            s.bytes,
            s.vacuum_debt,
            s.analyze_debt,
            s.excess,
            row_number() over (partition by s.hypertable order by s.vacuum_debt desc, s.oid)  as vacuum_place,
            row_number() over (partition by s.hypertable order by s.analyze_debt desc, s.oid) as analyze_place,
            row_number() over (partition by s.hypertable order by s.excess desc, s.oid)       as excess_place
       from (select c.*, coalesce(c.bytes - greatest(c.reltuples, 0) * d.bytes_per_row, 0) as excess
               from chunk_stats c
               left join chunk_density d on d.hypertable = c.hypertable) s
   ),
   chunk_slots as (
     select oid,
            row_number() over (partition by hypertable
                               order by (vacuum_place = 1 and vacuum_debt >= " chunk-vacuum-floor ")
                                        or (analyze_place = 1 and analyze_debt >= " chunk-analyze-floor ")
                                        or (excess_place = 1 and excess >= " chunk-excess-floor ") desc,
                                        bytes desc,
                                        oid) as slot
       from chunk_problems
   ),
   tracked_chunks as materialized (
     select oid from chunk_slots where slot <= " chunks-tracked-per-hypertable "
   ),
   compressed_rows as (
     select c.hypertable_id, sum(z.numrows_pre_compression) as rows
       from _timescaledb_catalog.compression_chunk_size z
       join _timescaledb_catalog.chunk c on c.id = z.chunk_id
      group by c.hypertable_id
   ),
   hypertable_totals as materialized (
     select h.oid,
            h.schema_name,
            h.table_name,
            count(s.oid)                                                       as chunk_count,
            coalesce(sum(s.rows_estimate), 0) + coalesce(max(r.rows), 0)       as rows_estimate,
            coalesce(sum(s.n_tup_ins), 0)::bigint                              as chunk_tup_ins,
            coalesce(sum(s.n_tup_upd), 0)::bigint                              as chunk_tup_upd,
            coalesce(sum(s.n_tup_hot_upd), 0)::bigint                          as chunk_tup_hot_upd,
            coalesce(sum(s.n_tup_del), 0)::bigint                              as chunk_tup_del
       from hypertables h
       left join chunk_stats s on s.hypertable = h.oid
       left join compressed_rows r on r.hypertable_id = h.id
      group by h.oid, h.schema_name, h.table_name
   )"))

(defn table-size-query [hypertables?]
  (str
   "with recursive " exclusive-locks-cte ",
   " (when hypertables? (str chunk-ctes ",")) "
   locked_relations as materialized (
     select unnest(relations) as oid
       from exclusive_locks
     union
     select i.indrelid
       from pg_index i
       cross join exclusive_locks
      where i.indexrelid = any (exclusive_locks.relations)
   ),
   user_tables as materialized (
     select c.oid                                    as relid,
            n.nspname                                as schemaname,
            c.relname,
            c.relkind,
            pg_stat_get_dead_tuples(c.oid)           as n_dead_tup,
            pg_stat_get_ins_since_vacuum(c.oid)      as n_ins_since_vacuum,
            pg_stat_get_tuples_inserted(c.oid)       as n_tup_ins,
            pg_stat_get_tuples_updated(c.oid)        as n_tup_upd,
            pg_stat_get_tuples_hot_updated(c.oid)    as n_tup_hot_upd,
            pg_stat_get_tuples_deleted(c.oid)        as n_tup_del,
            pg_stat_get_last_vacuum_time(c.oid)      as last_vacuum,
            pg_stat_get_last_autovacuum_time(c.oid)  as last_autovacuum,
            pg_stat_get_last_analyze_time(c.oid)     as last_analyze,
            pg_stat_get_last_autoanalyze_time(c.oid) as last_autoanalyze,
            pg_stat_get_mod_since_analyze(c.oid)     as n_mod_since_analyze
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace
      where c.relkind in ('r', 'm', 'p')
        and " (collected-table-sql "n.nspname" "c.relname")
   (when hypertables? "
        and c.oid not in (select oid from compression_tables)") "
   ),
   members (root, oid, relkind) as (
     select relid, relid, relkind
       from user_tables"
   (when hypertables? "
     union all
     select hypertable, unnest(array_remove(array[oid, compress_relid::oid], null)), 'r'::\"char\"
       from chunks") "
     union all
     select m.root, c.oid, c.relkind
       from members m
       join pg_inherits inh on inh.inhparent = m.oid
       join pg_class c on c.oid = inh.inhrelid
      where m.relkind = 'p'
   ),
   leaves as materialized (
     select c.oid,
            c.relkind,
            c.relpersistence,
            c.reltoastrelid,
            c.reltuples,
            tc.reltuples                                             as toast_reltuples,
            (locked.oid is not null or toast_locked.oid is not null) as locked
       from (select distinct oid from members where relkind <> 'p') leaf
       join pg_class c on c.oid = leaf.oid
       left join pg_class tc on tc.oid = c.reltoastrelid
       left join locked_relations locked on locked.oid = c.oid
       left join locked_relations toast_locked on toast_locked.oid = c.reltoastrelid
   ),
   sizes as materialized (
     select oid,
            locked,
            reltuples,
            toast_reltuples,
            case when relkind <> 'f' then pg_stat_get_live_tuples(oid) end      as live_tuples,
            pg_stat_get_live_tuples(reltoastrelid)                              as toast_live_tuples,
            case when not locked then pg_indexes_size(oid) end                  as index_bytes,
            case when not locked then pg_total_relation_size(reltoastrelid) end as toast_bytes,
            case when not locked then pg_relation_size(oid) end                 as main_bytes,
            case when not locked
                 then pg_relation_size(oid, 'fsm') + pg_relation_size(oid, 'vm')
                      + case when relpersistence = 'u' then pg_relation_size(oid, 'init') else 0 end
            end                                                                 as other_fork_bytes,
            case when not locked then pg_relation_size(reltoastrelid) end       as toast_main_bytes
       from leaves
   ),
   totals as (
     select m.root,
            bool_or(z.locked)                                                              as locked,
            case when not bool_or(z.locked) then sum(z.index_bytes) end                    as index_bytes,
            case when not bool_or(z.locked) then sum(z.toast_bytes) end                    as toast_bytes,
            case when not bool_or(z.locked) then sum(z.main_bytes + z.other_fork_bytes) end as heap_bytes,
            case when not bool_or(z.locked) then sum(z.main_bytes) end                     as heap_main_bytes,
            case when not bool_or(z.locked) then sum(coalesce(z.toast_main_bytes, 0)) end  as toast_main_bytes,
            sum(z.live_tuples)                                                             as live_tuples,
            sum(z.toast_live_tuples)                                                       as toast_live_tuples,
            case when bool_or(z.reltuples < 0) then null
                 else sum(z.reltuples::numeric)::bigint
            end                                                                            as catalog_rows,
            case when bool_or(z.toast_reltuples < 0) then null
                 else sum(coalesce(z.toast_reltuples, 0)::numeric)::bigint
            end                                                                            as toast_catalog_rows
       from members m
       join sizes z on z.oid = m.oid
      group by m.root
   )" (when hypertables? (str ",\n   " tracked-chunks-cte)) "
   select " (if hypertables? "coalesce(ht.schema_name, t.schemaname)" "t.schemaname") " as schema_name,
          " (if hypertables? "coalesce(ht.table_name, t.relname)" "t.relname") "    as table_name,
          t.relkind::text                as relkind,
          " (if hypertables?
              "coalesce(ht.rows_estimate, totals.live_tuples)"
              "totals.live_tuples") "    as rows_estimate,
          t.n_dead_tup,
          t.n_ins_since_vacuum,
          t.n_tup_ins,
          t.n_tup_upd,
          t.n_tup_hot_upd,
          t.n_tup_del,
          " (if hypertables? "coalesce(pn.nspname, ch_ht.schema_name)" "pn.nspname") " as parent_schema,
          " (if hypertables? "coalesce(pc.relname, ch_ht.table_name)" "pc.relname") "  as parent_table,
          t.last_vacuum,
          t.last_autovacuum,
          t.last_analyze,
          t.last_autoanalyze,
          t.n_mod_since_analyze,
          totals.index_bytes,
          totals.toast_bytes,
          totals.heap_bytes,
          totals.heap_main_bytes,
          totals.toast_main_bytes,
          totals.toast_live_tuples,
          " (if hypertables?
              "case when ht.oid is null then totals.catalog_rows else ht.rows_estimate::bigint end"
              "totals.catalog_rows") "   as catalog_rows,
          totals.toast_catalog_rows,
          coalesce(totals.locked, false) as locked"
   (when hypertables? ",
          ch.oid::bigint                 as chunk_oid,
          ht.chunk_count,
          ht.chunk_tup_ins,
          ht.chunk_tup_upd,
          ht.chunk_tup_hot_upd,
          ht.chunk_tup_del") "
     from user_tables t
     left join totals on totals.root = t.relid
     left join (pg_inherits inh
                join pg_class pc on pc.oid = inh.inhparent and pc.relkind = 'p'
                join pg_namespace pn on pn.oid = pc.relnamespace)
       on inh.inhrelid = t.relid"
   (when hypertables? "
     left join hypertable_totals ht on ht.oid = t.relid
     left join chunks ch on ch.oid = t.relid
     left join hypertable_totals ch_ht on ch_ht.oid = ch.hypertable
    where ch.oid is null
       or ch.oid in (select oid from tracked_chunks)")))

(defn- fillfactor-sql [reloptions]
  (str "substring(array_to_string(" reloptions ", ' ') from 'fillfactor=([0-9]+)')::smallint"))

(def ^:private max-align-sql
  "case when version() ~ 'mingw32' or version() ~ '64-bit|x86_64|ppc64|ia64|amd64' then 8 else 4 end")

(def ^:private width-kind-sql
  "case when attlen > 0 then 'fixed'
        when typcategory = 'S' and atttypmod > 4 then 'typmod'
        else 'unsized'
   end")

(def ^:private column-widths-sql
  (str "coalesce(sum(attlen) filter (where " width-kind-sql " = 'fixed'), 0)      as fixed_width,
        coalesce(sum(atttypmod) filter (where " width-kind-sql " = 'typmod'), 0) as typmod_width,
        count(*) filter (where " width-kind-sql " = 'unsized')                    as unsized_columns"))

(defn table-row-shape-query [hypertables?]
  (str
   (when hypertables? (str "with " chunk-ctes ",\n   " hidden-tables-cte "\n   "))
   "select " (if hypertables? "coalesce(h.schema_name, ns.nspname)" "ns.nspname") " as schema_name,
          " (if hypertables? "coalesce(h.table_name, tbl.relname)" "tbl.relname") "   as table_name,
          bool_or(att.attlen < 0)            as has_varlena,
          " (fillfactor-sql "tbl.reloptions") " as fillfactor,
          count(*)                           as column_count,
          " column-widths-sql ",
          current_setting('block_size')::int as block_bytes,
          " max-align-sql "                  as max_align
     from pg_attribute as att
     join pg_class as tbl on att.attrelid = tbl.oid
     join pg_namespace as ns on ns.oid = tbl.relnamespace
     join pg_type as typ on typ.oid = att.atttypid"
   (when hypertables? "
     left join hypertables as h on h.oid = tbl.oid") "
    where att.attnum > 0
      and not att.attisdropped
      and tbl.relkind in ('r', 'm')
      and " (collected-table-sql "ns.nspname" "tbl.relname")
   (when hypertables? "
      and tbl.oid not in (select oid from hidden_tables)") "
    group by 1, 2, 4"))

(def ^:private hypertable-index-ctes
  "index_shapes as materialized (
     select i.indexrelid,
            i.indrelid,
            t.hypertable,
            c.relname,
            c.reltuples,
            row(i.indnkeyatts, i.indisunique, i.indclass::text, i.indoption::text, i.indcollation::text,
                c.relam, i.indexprs is null, i.indpred is null,
                array(select (select a.attname
                                from pg_attribute a
                               where a.attrelid = i.indrelid
                                 and a.attnum = k.attnum)
                        from unnest(i.indkey::int2[]) with ordinality k (attnum, position)
                       order by k.position))::text as shape
       from (select oid, oid as hypertable from hypertables
             union all
             select oid, hypertable from chunks) t
       join pg_index i on i.indrelid = t.oid
       join pg_class c on c.oid = i.indexrelid
   ),
   hypertable_index_twins as materialized (
     select s.indexrelid,
            s.indrelid,
            s.relname,
            s.shape,
            min(s.indexrelid::bigint) over (partition by s.indrelid, s.shape, d.definition) as first_twin
       from index_shapes s
       join hypertables h on h.oid = s.indrelid
       join pg_index i on i.indexrelid = s.indexrelid
       cross join exclusive_locks
       cross join lateral (
         select case when s.indrelid = any (exclusive_locks.relations) then s.indexrelid::text
                     else concat_ws(' ', pg_get_expr(i.indexprs, i.indrelid), pg_get_expr(i.indpred, i.indrelid))
                end as definition
       ) d
   ),
   chunk_indexes as materialized (
     select distinct on (s.indexrelid)
            s.indexrelid,
            s.indrelid,
            s.reltuples,
            h.first_twin,
            h.relname as hypertable_index
       from index_shapes s
       join chunks ch on ch.oid = s.indrelid
       join hypertable_index_twins h on h.indrelid = s.hypertable and h.shape = s.shape
      order by s.indexrelid,
               s.relname = left(ch.relname || '_' || h.relname, 63)
                 or right(s.relname, length(h.relname)) = h.relname desc,
               h.indexrelid
   ),
   hypertable_indexes as materialized (
     select ci.first_twin,
            bool_or(l.locked)                                                   as locked,
            sum(case when not l.locked then pg_relation_size(ci.indexrelid) end)::bigint as index_bytes,
            sum(greatest(ci.reltuples, 0)::numeric)::bigint                     as catalog_rows,
            sum(pg_stat_get_live_tuples(ci.indrelid))::bigint                   as rows_estimate,
            sum(pg_stat_get_numscans(ci.indexrelid))::bigint                            as idx_scan,
            sum(pg_stat_get_tuples_returned(ci.indexrelid))::bigint                     as idx_tup_read,
            sum(pg_stat_get_tuples_fetched(ci.indexrelid))::bigint                      as idx_tup_fetch
       from chunk_indexes ci
       cross join exclusive_locks
       cross join lateral (
         select ci.indexrelid = any (exclusive_locks.relations)
                or ci.indrelid = any (exclusive_locks.relations) as locked
       ) l
      group by ci.first_twin
   )")

(defn index-metrics-query [hypertables?]
  (str
   "with " exclusive-locks-cte
   (when hypertables? (str ",\n   " chunk-ctes ",\n   " hidden-tables-cte ",\n   " hypertable-index-ctes)) "
    select " (if hypertables? "coalesce(ht.schema_name, s.schemaname)" "s.schemaname") " as schema_name,
           " (if hypertables? "coalesce(ht.table_name, s.relname)" "s.relname") "    as table_name,
           s.indexrelname                 as index_name,
           x.locked,
           case when not x.locked then pg_relation_size(s.indexrelid)" (when hypertables? " + coalesce(hi.index_bytes, 0)") " end as index_bytes,
           s.idx_scan,
           s.idx_tup_read,
           s.idx_tup_fetch,
           ix.indisvalid                  as is_valid,
           (p.index_relid is not null)    as is_building,
           (ix.indisunique or ix.indisexclusion or ix.indisreplident) as serves_constraint,
           current_setting('block_size')::int as block_bytes,
           " max-align-sql "              as max_align,
           ci.relpages                    as index_relpages,
           (ix.indpred is not null)       as is_partial,
           ix.indnatts                    as index_natts,
           " (fillfactor-sql "ci.reloptions") " as fillfactor,
           b.fixed_width,
           b.typmod_width,
           b.unsized_columns,
           " (if hypertables?
               "coalesce(hi.rows_estimate, pg_stat_get_live_tuples(s.relid))"
               "pg_stat_get_live_tuples(s.relid)") " as rows_estimate,
           " (when hypertables? "coalesce(hi.catalog_rows, ")
           "case when ci.reltuples < 0 then null
                else ci.reltuples::bigint
           end" (when hypertables? ")") " as catalog_rows,
           (ci.relam = (select oid from pg_am where amname = 'btree')) as is_btree,
           case when not x.locked then array_to_string(array(
             select pg_get_indexdef(s.indexrelid, k, false)
             from generate_series(1, ix.indnkeyatts) k
           ), E'\n') end as key_columns,
           array_to_string(array(
             select concat_ws(' ', ix.indcollation[k - 1], ix.indclass[k - 1], ix.indoption[k - 1])
             from generate_series(1, ix.indnkeyatts) k
           ), E'\n') as key_options,
           case when not x.locked then array_to_string(array(
             select pg_get_indexdef(s.indexrelid, k, false)
             from generate_series(ix.indnkeyatts + 1, ix.indnatts) k
           ), E'\n') end as included_columns,
           case when not x.locked then pg_get_expr(ix.indpred, ix.indrelid) end as index_predicate,
           case when not x.locked then pg_get_indexdef(s.indexrelid) end as index_def"
   (when hypertables? ",
           chunk_index.hypertable_index,
           case when ht.oid is not null then coalesce(hi.idx_scan, 0) end      as chunk_idx_scan,
           case when ht.oid is not null then coalesce(hi.idx_tup_read, 0) end  as chunk_idx_tup_read,
           case when ht.oid is not null then coalesce(hi.idx_tup_fetch, 0) end as chunk_idx_tup_fetch") "
    from pg_stat_user_indexes s
    join pg_index ix on ix.indexrelid = s.indexrelid
    join pg_class ci on ci.oid = s.indexrelid
    cross join exclusive_locks"
   (when hypertables? "
    left join hypertables ht on ht.oid = s.relid
    left join hypertable_index_twins twin on twin.indexrelid = s.indexrelid
    left join hypertable_indexes hi on hi.first_twin = twin.first_twin
    left join chunk_indexes chunk_index on chunk_index.indexrelid = s.indexrelid") "
    cross join lateral (
      select s.indexrelid = any (exclusive_locks.relations)
             or s.relid = any (exclusive_locks.relations)" (when hypertables? "
             or coalesce(hi.locked, false)") " as locked
    ) x
    left join pg_stat_progress_create_index p on p.index_relid = s.indexrelid
    cross join lateral (
      select " column-widths-sql "
        from (select coalesce(a1.attlen, a2.attlen)       as attlen,
                     coalesce(a1.atttypmod, a2.atttypmod) as atttypmod,
                     typ.typcategory
                from unnest(ix.indkey::int2[]) with ordinality k (attnum, position)
                left join pg_attribute a1 on k.attnum <> 0
                                         and a1.attrelid = ix.indrelid
                                         and a1.attnum = k.attnum
                left join pg_attribute a2 on k.attnum = 0
                                         and a2.attrelid = ix.indexrelid
                                         and a2.attnum = k.position
                left join pg_type typ on typ.oid = coalesce(a1.atttypid, a2.atttypid)) index_columns
    ) b
    where " (collected-table-sql "s.schemaname" "s.relname")
   (when hypertables? "
      and s.relid not in (select oid from hidden_tables)")))

(defn stmt-totals-query [toplevel?]
  (str "select sum(total_exec_time) as total_exec_time, sum(calls)::bigint as calls
        from pg_stat_statements(false)
        where dbid = (select oid from pg_database where datname = current_database())"
       (when toplevel? "\n          and toplevel = true")))

(defn query-texts-query [toplevel? queryids]
  (str "select queryid, max(query) as query
   from pg_stat_statements(true)
   where dbid = (select oid from pg_database where datname = current_database())
     and queryid in (" (str/join "," queryids) ")"
       (when toplevel? "\n     and toplevel = true")
       "
   group by queryid"))

(defn recorded-queryids-query [toplevel? queryids]
  (str "select distinct queryid
   from pg_stat_statements(false)
   where dbid = (select oid from pg_database where datname = current_database())
     and queryid in (" (str/join "," queryids) ")"
       (when toplevel? "\n     and toplevel = true")))

(def ^:private pgss-available-sql
  "to_regprocedure('pg_stat_statements(boolean)') is not null
   and exists (select from pg_settings where name = 'pg_stat_statements.max')")

(def ^:private pgss-features-sql
  (str "(" pgss-available-sql ") as pgss_available,
          exists (select from pg_attribute
                  where attrelid = to_regclass('pg_stat_statements')
                    and attname = 'toplevel')                         as pgss_toplevel,
          exists (select from pg_attribute
                  where attrelid = to_regclass('pg_stat_statements')
                    and attname = 'stats_since')                      as pgss_stats_since"))

(def ^:private hypertables-available-sql
  "exists (select from pg_attribute a
                    join pg_class c on c.oid = a.attrelid
                    join pg_namespace n on n.oid = c.relnamespace
                   where n.nspname = '_timescaledb_catalog'
                     and c.relname = 'compression_settings'
                     and a.attname = 'compress_relid'
                     and has_schema_privilege(n.oid, 'usage')
                     and has_table_privilege(c.oid, 'select'))")

(defn- recorded-queryids [read-db toplevel? queryids]
  (map :queryid (jdbc/query read-db [(tagged "xmin-holder-queryids" (recorded-queryids-query toplevel? queryids))])))

(def database-snapshot-query
  (str "select current_setting('server_version_num')::integer::numeric as pg_version,
          current_database()                                           as database_name,
          pg_database_size(current_database())::bigint                as database_size,
          " pgss-features-sql ",
          to_regclass('pg_stat_statements_info') is not null           as pgss_info_available,
          " hypertables-available-sql "                                as hypertables_available,
          sessions.*, autovacuum.*, xid.*, counters.*
   from (" session-counts-query ") sessions
   cross join (" autovacuum-workers-query ") autovacuum
   left join (" xid-usage-query ") xid on true
   left join (" database-counters-query ") counters on true"))

(defn- checkpoint-query [version]
  (cond (>= version 180000) checkpoint-query-18
        (>= version 170000) checkpoint-query-17
        :else               checkpoint-query-pre17))

(defn optional-database-query [version {:keys [pgss? pgss-info? toplevel?]}]
  (str "select checkpoints.*, xmin.*"
       (when pgss? ", totals.*")
       (when pgss-info? ", info.stats_reset as query_stats_reset")
       "\n   from (" (checkpoint-query version) ") checkpoints"
       "\n   left join (" (xmin-holder-query (>= version 170000) (>= version 140000)) ") xmin on true"
       (when pgss? (str "\n   cross join (" (stmt-totals-query toplevel?) ") totals"))
       (when pgss-info? "\n   cross join pg_stat_statements_info info")))

(def ^:private top-queries-tracked 20)

(def ^:private top-queries-considered 60)

(defn top-queries-query [toplevel? stats-since?]
  (str "select queryid,
          sum(calls)                                                        as calls,
          sum(total_exec_time)                                              as total_exec_time,
          sum(calls * (stddev_exec_time ^ 2 + mean_exec_time ^ 2))         as sum_exec_time_sq,
          sum(rows)                                                        as rows,
          sum(shared_blks_hit)                                             as shared_blks_hit,
          sum(shared_blks_read)                                            as shared_blks_read,
          " (if stats-since? "max(stats_since)" "null::timestamptz") "      as stats_since
   from pg_stat_statements(false)
   where dbid = (select oid from pg_database where datname = current_database())
     and queryid is not null"
       (when toplevel? "\n     and toplevel = true")
       "
   group by queryid
   order by sum(total_exec_time) desc
   limit " top-queries-considered))

(def ^:private statement-timeout-ms 15000)

(def ^:private lock-timeout-ms 5000)

(def ^:private session-settings-query
  (str "select set_config('statement_timeout', '" statement-timeout-ms "', false),
          set_config('lock_timeout', '" lock-timeout-ms "', false),
          set_config('search_path',
                     concat_ws(', ', nullif(s.reset_val, ''),
                               (select e.extnamespace::regnamespace::text
                                  from pg_extension e
                                 where e.extname = 'pg_stat_statements')),
                     false)
     from pg_settings s
    where s.name = 'search_path'"))

(defn configure-session! [read-db]
  (jdbc/query read-db [(tagged "session-settings" session-settings-query)]))

(defn database [read-db]
  (let [snapshot (first (jdbc/query read-db [(tagged "database-metrics" database-snapshot-query)]))
        pgss?    (boolean (:pgss_available snapshot))]
    {:snapshot snapshot
     :optional (try (first (jdbc/query read-db
                             [(tagged "database-metrics-extra"
                                      (optional-database-query
                                        (or (:pg_version snapshot) 0)
                                        {:pgss?      pgss?
                                         :pgss-info? (and pgss? (boolean (:pgss_info_available snapshot)))
                                         :toplevel?  (and pgss? (boolean (:pgss_toplevel snapshot)))}))]))
                    (catch Exception _ nil))}))

(defn- query-texts [read-db toplevel? queryids]
  (if (seq queryids)
    (mapv (fn [row] (update row :query #(some-> % literals/redact)))
          (jdbc/query read-db [(tagged "query-texts" (query-texts-query toplevel? queryids))]))
    []))

(defn xmin-query-texts [read-db pgss history]
  (when (:pgss_available pgss)
    (let [queryids  ((:unresolved-xmin-queryids history))
          toplevel? (:pgss_toplevel pgss)]
      (when (seq queryids)
        (query-texts read-db toplevel? (recorded-queryids read-db toplevel? queryids))))))

(defn- hypertables-available? [collected]
  (boolean (get-in collected [:database :snapshot :hypertables_available])))

(defn- tracked-chunks-param [sizes]
  (str "{" (str/join "," (keep :chunk_oid sizes)) "}"))

(defn tables [read-db hypertables?]
  (let [sizes (jdbc/query read-db [(tagged "table-metrics" (table-size-query hypertables?))])]
    {:sizes  sizes
     :shapes (jdbc/query read-db (cond-> [(tagged "table-row-widths" (table-row-shape-query hypertables?))]
                                   hypertables? (conj (tracked-chunks-param sizes))))}))

(defn indexes [read-db hypertables? sizes]
  (jdbc/query read-db (cond-> [(tagged "index-metrics" (index-metrics-query hypertables?))]
                        hypertables? (conj (tracked-chunks-param sizes)))))

(defn- tracked-query-rows [history rows]
  (let [frozen ((:frozen-queryids history) rows)]
    (into []
      (comp (remove (comp frozen :queryid))
            (take top-queries-tracked))
      rows)))

(defn queries [read-db {toplevel? :pgss_toplevel stats-since? :pgss_stats_since :as pgss} history]
  (when (:pgss_available pgss)
    (let [rows     (tracked-query-rows history
                     (jdbc/query read-db [(tagged "query-metrics" (top-queries-query toplevel? stats-since?))]))
          queryids (->> rows (map :queryid) (remove nil?))]
      {:rows  rows
       :texts (query-texts read-db toplevel? ((:missing-query-texts history) queryids))})))

(defn- xmin-query-texts-or-none [read-db pgss history]
  (try (xmin-query-texts read-db pgss history)
       (catch Exception _ [])))

(defn- collection-steps [read-db history]
  [["database" (fn [collected]
                 (let [{:keys [snapshot] :as readings} (database read-db)]
                   (-> collected
                       (assoc :database readings)
                       (update :query_texts into (xmin-query-texts-or-none read-db snapshot history)))))]
   ["tables"   (fn [collected]
                 (assoc collected :tables (tables read-db (hypertables-available? collected))))]
   ["indexes"  (fn [collected]
                 (assoc collected :indexes (indexes read-db (hypertables-available? collected)
                                                    (get-in collected [:tables :sizes]))))]
   ["queries"  (fn [collected]
                 (let [{:keys [rows texts]} (queries read-db (get-in collected [:database :snapshot]) history)]
                   (-> collected
                       (assoc :queries rows)
                       (update :query_texts into texts))))]])

(defn collection [read-db history]
  (reduce (fn [collected [section step]]
            (try (step collected)
                 (catch Exception e
                   (reduced (assoc collected :failure {:section section :error e})))))
          {:query_texts []}
          (collection-steps read-db history)))
