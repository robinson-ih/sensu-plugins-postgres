#! /usr/bin/env ruby
#
# metrics-postgres-index-bloat
#
# DESCRIPTION:
#   This plugin collects metrics from the results of a postgres index bloat query.
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
#   metrics-postgres-index-bloat.rb -u db_user -p db_pass -h db_server -d db -q 'select foo from bar'
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
         default: "WITH btree_index_atts AS (
    SELECT nspname,
        indexclass.relname as index_name,
        indexclass.reltuples,
        indexclass.relpages,
        indrelid, indexrelid,
        indexclass.relam,
        tableclass.relname as tablename,
        regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
        indexrelid as index_oid
    FROM pg_index
    JOIN pg_class AS indexclass ON pg_index.indexrelid = indexclass.oid
    JOIN pg_class AS tableclass ON pg_index.indrelid = tableclass.oid
    JOIN pg_namespace ON pg_namespace.oid = indexclass.relnamespace
    JOIN pg_am ON indexclass.relam = pg_am.oid
    WHERE pg_am.amname = 'btree' and indexclass.relpages > 0
         AND nspname NOT IN ('pg_catalog','information_schema')
    ),
index_item_sizes AS (
    SELECT
    ind_atts.nspname, ind_atts.index_name,
    ind_atts.reltuples, ind_atts.relpages, ind_atts.relam,
    indrelid AS table_oid, index_oid,
    current_setting('block_size')::numeric AS bs,
    8 AS maxalign,
    24 AS pagehdr,
    CASE WHEN max(coalesce(pg_stats.null_frac,0)) = 0
        THEN 2
        ELSE 6
    END AS index_tuple_hdr,
    sum( (1-coalesce(pg_stats.null_frac, 0)) * coalesce(pg_stats.avg_width, 1024) ) AS nulldatawidth
    FROM pg_attribute
    JOIN btree_index_atts AS ind_atts ON pg_attribute.attrelid = ind_atts.indexrelid AND pg_attribute.attnum = ind_atts.attnum
    JOIN pg_stats ON pg_stats.schemaname = ind_atts.nspname
          -- stats for regular index columns
          AND ( (pg_stats.tablename = ind_atts.tablename AND pg_stats.attname = pg_catalog.pg_get_indexdef(pg_attribute.attrelid, pg_attribute.attnum, TRUE))
          -- stats for functional indexes
          OR   (pg_stats.tablename = ind_atts.index_name AND pg_stats.attname = pg_attribute.attname))
    WHERE pg_attribute.attnum > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
index_aligned_est AS (
    SELECT maxalign, bs, nspname, index_name, reltuples,
        relpages, relam, table_oid, index_oid,
        coalesce (
            ceil (
                reltuples * ( 6
                    + maxalign
                    - CASE
                        WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
                        ELSE index_tuple_hdr%maxalign
                      END
                    + nulldatawidth
                    + maxalign
                    - CASE /* Add padding to the data to align on MAXALIGN */
                        WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
                        ELSE nulldatawidth::integer%maxalign
                      END
                )::numeric
              / ( bs - pagehdr::NUMERIC )
              +1 )
         , 0 )
      as expected
    FROM index_item_sizes
)
SELECT sum(round(bs*(index_aligned_est.relpages)/1024/1024)::int) AS total_MB,
       sum(round(bs*(index_aligned_est.relpages-expected)/1024/1024)::int) AS wasted_MB,
       round(min(bs)*(sum(index_aligned_est.relpages)-sum(expected))::bigint * 100 / (min(bs)*(sum(index_aligned_est.relpages))::bigint)) AS bloat_pct
    FROM index_aligned_est
    JOIN pg_class ON pg_class.oid=index_aligned_est.table_oid
      JOIN pg_stat_user_indexes AS stat ON index_aligned_est.index_oid = stat.indexrelid;"

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
        output "#{config[:scheme]}.#{config[:database]}.total_MB", row[0]
        output "#{config[:scheme]}.#{config[:database]}.wasted_MB", row[1]
        output "#{config[:scheme]}.#{config[:database]}.bloat_pct", row[2]
      end
    else
      output config[:scheme], value
    end
    ok
  end
end