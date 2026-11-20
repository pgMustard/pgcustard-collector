(ns pgcustard.collect
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [pgcustard.literals :as literals]))

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

(def ^:private exclusive-locks-cte
  "exclusive_locks as materialized (
     select coalesce(array_agg(distinct l.relation), '{}'::oid[]) as relations
       from pg_locks l
      where l.locktype = 'relation'
        and l.mode = 'AccessExclusiveLock'
        and l.database = (select oid from pg_database where datname = current_database())
   )")

(def table-size-query
  (str
   "with " exclusive-locks-cte "
   select s.schemaname                                           as schema_name,
          s.relname                                              as table_name,
          c.relkind::text                                        as relkind,
          sz.live_tuples                                         as rows_estimate,
          s.n_dead_tup,
          s.n_ins_since_vacuum,
          s.n_tup_ins,
          s.n_tup_upd,
          s.n_tup_hot_upd,
          s.n_tup_del,
          parent.parent_schema,
          parent.parent_table,
          s.last_vacuum,
          s.last_autovacuum,
          s.last_analyze,
          s.last_autoanalyze,
          s.n_mod_since_analyze,
          sz.index_bytes,
          sz.toast_bytes,
          sz.heap_bytes,
          sz.heap_main_bytes,
          sz.toast_main_bytes,
          sz.toast_live_tuples,
          sz.catalog_rows,
          sz.toast_catalog_rows,
          coalesce(sz.locked, false)                             as locked
     from pg_stat_user_tables s
     join pg_class c on c.oid = s.relid
     cross join exclusive_locks
     left join lateral (
       select pn.nspname as parent_schema, pc.relname as parent_table
         from pg_inherits inh
         join pg_class pc on pc.oid = inh.inhparent and pc.relkind = 'p'
         join pg_namespace pn on pn.oid = pc.relnamespace
        where inh.inhrelid = s.relid
        limit 1
     ) parent on true
     cross join lateral (
       select case when c.relkind = 'p'
                   then (with recursive descendants (oid) as (
                           select inh.inhrelid from pg_inherits inh where inh.inhparent = s.relid
                           union all
                           select inh.inhrelid
                             from pg_inherits inh
                             join descendants d on inh.inhparent = d.oid
                         )
                         select coalesce(array_agg(d.oid), '{}'::oid[])
                           from descendants d
                           join pg_class dc on dc.oid = d.oid
                          where dc.relkind <> 'p')
                   else array[s.relid]
              end as oids
     ) tree
     cross join lateral (
       select bool_or(r.locked)                             as locked,
              case when not bool_or(r.locked) then sum(r.index_bytes) end as index_bytes,
              case when not bool_or(r.locked) then sum(r.toast_bytes) end as toast_bytes,
              case when not bool_or(r.locked) then sum(r.main_bytes + r.other_fork_bytes) end as heap_bytes,
              case when not bool_or(r.locked) then sum(r.main_bytes) end as heap_main_bytes,
              case when not bool_or(r.locked) then sum(coalesce(r.toast_main_bytes, 0)) end as toast_main_bytes,
              sum(pg_stat_get_live_tuples(r.oid)) filter (where r.relkind <> 'f') as live_tuples,
              sum(pg_stat_get_live_tuples(r.reltoastrelid)) as toast_live_tuples,
              case when bool_or(r.reltuples < 0) then null
                   else sum(r.reltuples::numeric)::bigint
              end                                           as catalog_rows,
              case when bool_or(r.toast_reltuples < 0) then null
                   else sum(coalesce(r.toast_reltuples, 0)::numeric)::bigint
              end                                           as toast_catalog_rows
         from (select m.oid, m.relkind, m.reltuples, m.reltoastrelid, tc.reltuples as toast_reltuples,
                      x.locked,
                      case when not x.locked then pg_indexes_size(m.oid) end                  as index_bytes,
                      case when not x.locked then pg_total_relation_size(m.reltoastrelid) end as toast_bytes,
                      case when not x.locked then pg_relation_size(m.oid) end                 as main_bytes,
                      case when not x.locked
                           then pg_relation_size(m.oid, 'fsm') + pg_relation_size(m.oid, 'vm')
                                + pg_relation_size(m.oid, 'init')
                      end                                                                     as other_fork_bytes,
                      case when not x.locked then pg_relation_size(m.reltoastrelid) end       as toast_main_bytes
                 from pg_class m
                 left join pg_class tc on tc.oid = m.reltoastrelid
                 cross join lateral (
                   select m.oid = any (exclusive_locks.relations)
                          or m.reltoastrelid = any (exclusive_locks.relations)
                          or exists (select 1
                                       from pg_index i
                                      where i.indrelid in (m.oid, m.reltoastrelid)
                                        and i.indexrelid = any (exclusive_locks.relations)) as locked
                 ) x
                where m.oid = any (tree.oids)) r
     ) sz
    where s.schemaname not in " ignored-schema-list))

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

(def table-row-shape-query
  (str
   "select ns.nspname                         as schema_name,
          tbl.relname                        as table_name,
          bool_or(att.attlen < 0)            as has_varlena,
          " (fillfactor-sql "tbl.reloptions") " as fillfactor,
          count(*)                           as column_count,
          " column-widths-sql ",
          current_setting('block_size')::int as block_bytes,
          " max-align-sql "                  as max_align
     from pg_attribute as att
     join pg_class as tbl on att.attrelid = tbl.oid
     join pg_namespace as ns on ns.oid = tbl.relnamespace
     join pg_type as typ on typ.oid = att.atttypid
    where att.attnum > 0
      and not att.attisdropped
      and tbl.relkind in ('r', 'm')
      and ns.nspname not in " ignored-schema-list "
    group by 1, 2, 4"))

(def index-metrics-query
  (str
   "with " exclusive-locks-cte "
    select s.schemaname                   as schema_name,
           s.relname                      as table_name,
           s.indexrelname                 as index_name,
           x.locked,
           case when not x.locked then pg_relation_size(s.indexrelid) end as index_bytes,
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
           b.fillfactor,
           b.fixed_width,
           b.typmod_width,
           b.unsized_columns,
           st.n_live_tup                  as rows_estimate,
           case when ci.reltuples < 0 then null
                else ci.reltuples::bigint
           end                            as catalog_rows,
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
           case when not x.locked then pg_get_indexdef(s.indexrelid) end as index_def
    from pg_stat_user_indexes s
    join pg_index ix on ix.indexrelid = s.indexrelid
    join pg_class ci on ci.oid = s.indexrelid
    cross join exclusive_locks
    cross join lateral (
      select s.indexrelid = any (exclusive_locks.relations)
             or s.relid = any (exclusive_locks.relations) as locked
    ) x
    left join pg_stat_progress_create_index p on p.index_relid = s.indexrelid
    left join pg_stat_all_tables st on st.relid = s.relid
    left join (
      select idxoid, fillfactor,
             " column-widths-sql "
      from (
        select ic.idxoid, ic.fillfactor,
               coalesce(a1.attlen, a2.attlen)       as attlen,
               coalesce(a1.atttypmod, a2.atttypmod) as atttypmod,
               typ.typcategory
        from (
          select i.indexrelid as idxoid, i.indrelid as tbloid,
                 " (fillfactor-sql "ci.reloptions") " as fillfactor,
                 pg_catalog.string_to_array(
                   pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)), ' ')::int[] as indkey,
                 pg_catalog.generate_series(1, i.indnatts) as attpos
          from pg_catalog.pg_index i
          join pg_catalog.pg_class ci on ci.oid = i.indexrelid
          where ci.relnamespace not in (select oid from pg_namespace
                                         where nspname in " ignored-schema-list ")
        ) as ic
        left join pg_catalog.pg_attribute a1 on
          ic.indkey[ic.attpos] <> 0
          and a1.attrelid = ic.tbloid
          and a1.attnum = ic.indkey[ic.attpos]
        left join pg_catalog.pg_attribute a2 on
          ic.indkey[ic.attpos] = 0
          and a2.attrelid = ic.idxoid
          and a2.attnum = ic.attpos
        left join pg_catalog.pg_type typ on typ.oid = coalesce(a1.atttypid, a2.atttypid)
      ) i
      group by 1, 2
    ) b on b.idxoid = s.indexrelid
    where s.schemaname not in " ignored-schema-list))

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

(defn- pgss-features [read-db]
  (first (jdbc/query read-db [(str "select " pgss-features-sql)])))

(defn- recorded-queryids [read-db toplevel? queryids]
  (map :queryid (jdbc/query read-db [(recorded-queryids-query toplevel? queryids)])))

(def database-snapshot-query
  (str "select current_setting('server_version_num')::integer::numeric as pg_version,
          current_database()                                           as database_name,
          pg_database_size(current_database())::bigint                as database_size,
          " pgss-features-sql ",
          to_regclass('pg_stat_statements_info') is not null           as pgss_info_available,
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
  (jdbc/query read-db [session-settings-query]))

(defn database [read-db]
  (let [snapshot (first (jdbc/query read-db [database-snapshot-query]))
        pgss?    (boolean (:pgss_available snapshot))]
    {:snapshot snapshot
     :optional (try (first (jdbc/query read-db
                             [(optional-database-query
                                (or (:pg_version snapshot) 0)
                                {:pgss?      pgss?
                                 :pgss-info? (and pgss? (boolean (:pgss_info_available snapshot)))
                                 :toplevel?  (and pgss? (boolean (:pgss_toplevel snapshot)))})]))
                    (catch Exception _ nil))}))

(defn- query-texts [read-db toplevel? queryids]
  (if (seq queryids)
    (mapv (fn [row] (update row :query #(some-> % literals/redact)))
          (jdbc/query read-db [(query-texts-query toplevel? queryids)]))
    []))

(defn xmin-query-texts [read-db history]
  (let [queryids ((:unresolved-xmin-queryids history))
        pgss     (when (seq queryids) (pgss-features read-db))]
    (when (:pgss_available pgss)
      (let [toplevel? (:pgss_toplevel pgss)]
        (query-texts read-db toplevel? (recorded-queryids read-db toplevel? queryids))))))

(defn tables [read-db]
  {:sizes  (jdbc/query read-db [table-size-query])
   :shapes (jdbc/query read-db [table-row-shape-query])})

(defn indexes [read-db]
  (jdbc/query read-db [index-metrics-query]))

(defn- tracked-query-rows [history rows]
  (let [frozen ((:frozen-queryids history) rows)]
    (into []
      (comp (remove (comp frozen :queryid))
            (take top-queries-tracked))
      rows)))

(defn queries [read-db history]
  (let [{toplevel? :pgss_toplevel stats-since? :pgss_stats_since :as pgss} (pgss-features read-db)]
    (when (:pgss_available pgss)
      (let [rows     (tracked-query-rows history
                       (jdbc/query read-db [(top-queries-query toplevel? stats-since?)]))
            queryids (->> rows (map :queryid) (remove nil?))]
        {:rows  rows
         :texts (query-texts read-db toplevel? ((:missing-query-texts history) queryids))}))))

(defn- xmin-query-texts-or-none [read-db history]
  (try (xmin-query-texts read-db history)
       (catch Exception _ [])))

(defn- collection-steps [read-db history]
  [["database" (fn [collected]
                 (-> collected
                     (assoc :database (database read-db))
                     (update :query_texts into (xmin-query-texts-or-none read-db history))))]
   ["tables"   (fn [collected] (assoc collected :tables (tables read-db)))]
   ["indexes"  (fn [collected] (assoc collected :indexes (indexes read-db)))]
   ["queries"  (fn [collected]
                 (let [{:keys [rows texts]} (queries read-db history)]
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
