#! /usr/bin/env ruby
#
# metrics-postgres-table-bloat
#
# DESCRIPTION:
#   This plugin collects metrics from the results of a postgres table bloat query.
#
# OUTPUT:
#   metric data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: pg
#   gem: sensu-plugin
#
# USAGE:
#   metrics-postgres-table-bloat.rb -u db_user -p db_pass -h db_server -d db -q 'select foo from bar'
#
# NOTES:
#
# LICENSE:
#   Copyright 2015, Eric Heydrick <eheydrick@gmail.com>
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugins-postgres/pgpass'
require 'sensu-plugin/metric/cli'
require 'pg'

class MetricsPostgresQuery < Sensu::Plugin::Metric::CLI::Graphite
  option :pgpass,
         description: 'Pgpass file',
         short: '-f FILE',
         long: '--pgpass',
         default: ENV['PGPASSFILE'] || "#{ENV['HOME']}/.pgpass"

  option :user,
         description: 'Postgres User',
         short: '-u USER',
         long: '--user USER'

  option :password,
         description: 'Postgres Password',
         short: '-p PASS',
         long: '--password PASS'

  option :hostname,
         description: 'Hostname to login to',
         short: '-h HOST',
         long: '--hostname HOST'

  option :port,
         description: 'Database port',
         short: '-P PORT',
         long: '--port PORT'

  option :database,
         description: 'Database name',
         short: '-d DB',
         long: '--db DB'

  option :query,
         description: 'Database query to execute',
         short: '-q QUERY',
         long: '--query QUERY',
         required: true,
         default: "WITH constants AS (
   SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
),
no_stats AS (
   SELECT table_schema, table_name,
       n_live_tup::numeric as est_rows,
       pg_table_size(relid)::numeric as table_size
   FROM information_schema.columns
       JOIN pg_stat_user_tables as psut
          ON table_schema = psut.schemaname
          AND table_name = psut.relname
       LEFT OUTER JOIN pg_stats
       ON table_schema = pg_stats.schemaname
           AND table_name = pg_stats.tablename
           AND column_name = attname
   WHERE attname IS NULL
       AND table_schema NOT IN ('pg_catalog', 'information_schema')
   GROUP BY table_schema, table_name, relid, n_live_tup
),
null_headers AS (
   SELECT
       hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
       SUM((1-null_frac)*avg_width) as datawidth,
       MAX(null_frac) as maxfracsum,
       schemaname,
       tablename,
       hdr, ma, bs
   FROM pg_stats CROSS JOIN constants
       LEFT OUTER JOIN no_stats
           ON schemaname = no_stats.table_schema
           AND tablename = no_stats.table_name
   WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
       AND no_stats.table_name IS NULL
       AND EXISTS ( SELECT 1
           FROM information_schema.columns
               WHERE schemaname = columns.table_schema
                   AND tablename = columns.table_name )
   GROUP BY schemaname, tablename, hdr, ma, bs
),
data_headers AS (
   SELECT
       ma, bs, hdr, schemaname, tablename,
       (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
       (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
   FROM null_headers
),
table_estimates AS (
   SELECT schemaname, tablename, bs,
       reltuples::numeric as est_rows, relpages * bs as table_bytes,
   CEIL((reltuples*
           (datahdr + nullhdr2 + 4 + ma -
               (CASE WHEN datahdr%ma=0
                   THEN ma ELSE datahdr%ma END)
               )/(bs-20))) * bs AS expected_bytes,
       reltoastrelid
   FROM data_headers
       JOIN pg_class ON tablename = relname
       JOIN pg_namespace ON relnamespace = pg_namespace.oid
           AND schemaname = nspname
   WHERE pg_class.relkind = 'r'
),
estimates_with_toast AS (
   SELECT schemaname, tablename,
       TRUE as can_estimate,
       est_rows,
       table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
       expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
   FROM table_estimates LEFT OUTER JOIN pg_class as toast
       ON table_estimates.reltoastrelid = toast.oid
           AND toast.relkind = 't'
),
table_estimates_plus AS (
   SELECT current_database() as databasename,
           schemaname, tablename, can_estimate,
           est_rows,
           CASE WHEN table_bytes > 0
               THEN table_bytes::NUMERIC
               ELSE NULL::NUMERIC END
               AS table_bytes,
           CASE WHEN expected_bytes > 0
               THEN expected_bytes::NUMERIC
               ELSE NULL::NUMERIC END
                   AS expected_bytes,
           CASE WHEN expected_bytes > 0 AND table_bytes > 0
               AND expected_bytes <= table_bytes
               THEN (table_bytes - expected_bytes)::NUMERIC
               ELSE 0::NUMERIC END AS bloat_bytes
   FROM estimates_with_toast
   UNION ALL
   SELECT current_database() as databasename,
       table_schema, table_name, FALSE,
       est_rows, table_size,
       NULL::NUMERIC, NULL::NUMERIC
   FROM no_stats
)
select sum(round(table_bytes/(1024^2)::NUMERIC,3)) as total_MB,
       sum(round(bloat_bytes/(1024::NUMERIC^2),2)) as wasted_MB,
       round(sum(bloat_bytes)*100/sum(table_bytes)) as bloat_pct
 FROM table_estimates_plus
where can_estimate;"

  option :count_tuples,
         description: 'Count the number of tuples (rows) returned by the query',
         short: '-t',
         long: '--tuples',
         boolean: true,
         default: false

  option :scheme,
         description: 'Metric naming scheme, text to prepend to metric',
         short: '-s SCHEME',
         long: '--scheme SCHEME',
         default: 'postgres'

  option :multirow,
         description: 'Determines if we return first row or all rows',
         short: '-m',
         long: '--multirow',
         boolean: true,
         default: false

  option :timeout,
         description: 'Connection timeout (seconds)',
         short: '-T TIMEOUT',
         long: '--timeout TIMEOUT',
         default: nil

  include Pgpass

  def run
    begin
      pgpass
      con = PG.connect(host: config[:hostname],
                       dbname: config[:database],
                       user: config[:user],
                       password: config[:password],
                       port: config[:port],
                       connect_timeout: config[:timeout])
      res = con.exec(config[:query].to_s)
    rescue PG::Error => e
      unknown "Unable to query PostgreSQL: #{e.message}"
    end

    value = if config[:count_tuples]
              res.ntuples
            else
              res.first.values.first unless res.first.nil?
            end

    if config[:multirow] && !config[:count_tuples]
      res.values.each do |row|
        output "#{config[:scheme]}.total_MB", row[0]
        output "#{config[:scheme]}.wasted_MB", row[1]
        output "#{config[:scheme]}.bloat_pct", row[2]
      end
    else
      output config[:scheme], value
    end
    ok
  end
end