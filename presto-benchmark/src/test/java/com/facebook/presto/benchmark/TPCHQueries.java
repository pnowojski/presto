/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.benchmark;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TPCHQueries
{
    private static final Map<String, String> QUERIES;

    static {
        ImmutableMap.Builder<String, String> queries = ImmutableMap.builder();

        queries.put("q01",
                "SELECT\n" +
                        "  l.returnflag,\n" +
                        "  l.linestatus,\n" +
                        "  sum(l.quantity)                                       AS sum_qty,\n" +
                        "  sum(l.extendedprice)                                  AS sum_base_price,\n" +
                        "  sum(l.extendedprice * (1 - l.discount))               AS sum_disc_price,\n" +
                        "  sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,\n" +
                        "  avg(l.quantity)                                       AS avg_qty,\n" +
                        "  avg(l.extendedprice)                                  AS avg_price,\n" +
                        "  avg(l.discount)                                       AS avg_disc,\n" +
                        "  count(*)                                              AS count_order\n" +
                        "FROM\n" +
                        "  \"lineitem\" AS l\n" +
                        "WHERE\n" +
                        "  l.shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY\n" +
                        "GROUP BY\n" +
                        "  l.returnflag,\n" +
                        "  l.linestatus\n" +
                        "ORDER BY\n" +
                        "  l.returnflag,\n" +
                        "  l.linestatus\n");

        queries.put("q02",
                "SELECT\n" +
                        "  s.acctbal,\n" +
                        "  s.name,\n" +
                        "  n.name,\n" +
                        "  p.partkey,\n" +
                        "  p.mfgr,\n" +
                        "  s.address,\n" +
                        "  s.phone,\n" +
                        "  s.comment\n" +
                        "FROM\n" +
                        "  \"part\" p,\n" +
                        "  \"supplier\" s,\n" +
                        "  \"partsupp\" ps,\n" +
                        "  \"nation\" n,\n" +
                        "  \"region\" r\n" +
                        "WHERE\n" +
                        "  p.partkey = ps.partkey\n" +
                        "  AND s.suppkey = ps.suppkey\n" +
                        "  AND p.size = 15\n" +
                        "  AND p.type like '%BRASS'\n" +
                        "  AND s.nationkey = n.nationkey\n" +
                        "  AND n.regionkey = r.regionkey\n" +
                        "  AND r.name = 'EUROPE'\n" +
                        "  AND ps.supplycost = (\n" +
                        "    SELECT\n" +
                        "      min(ps.supplycost)\n" +
                        "    FROM\n" +
                        "      \"partsupp\" ps,\n" +
                        "      \"supplier\" s,\n" +
                        "      \"nation\" n,\n" +
                        "      \"region\" r\n" +
                        "    WHERE\n" +
                        "      p.partkey = ps.partkey\n" +
                        "      AND s.suppkey = ps.suppkey\n" +
                        "      AND s.nationkey = n.nationkey\n" +
                        "      AND n.regionkey = r.regionkey\n" +
                        "      AND r.name = 'EUROPE'\n" +
                        "  )\n" +
                        "ORDER BY\n" +
                        "  s.acctbal desc,\n" +
                        "  n.name,\n" +
                        "  s.name,\n" +
                        "  p.partkey\n");

        queries.put("q03",
                "SELECT\n" +
                        "  l.orderkey,\n" +
                        "  sum(l.extendedprice * (1 - l.discount)) AS revenue,\n" +
                        "  o.orderdate,\n" +
                        "  o.shippriority\n" +
                        "FROM\n" +
                        "  \"customer\" AS c,\n" +
                        "  \"orders\" AS o,\n" +
                        "  \"lineitem\" AS l\n" +
                        "WHERE\n" +
                        "  c.mktsegment = 'BUILDING'\n" +
                        "  AND c.custkey = o.custkey\n" +
                        "  AND l.orderkey = o.orderkey\n" +
                        "  AND o.orderdate < DATE '1995-03-15'\n" +
                        "  AND l.shipdate > DATE '1995-03-15'\n" +
                        "GROUP BY\n" +
                        "  l.orderkey,\n" +
                        "  o.orderdate,\n" +
                        "  o.shippriority\n" +
                        "ORDER BY\n" +
                        "  revenue DESC,\n" +
                        "  o.orderdate\n" +
                        "LIMIT 10\n");

        queries.put("q04",
                "SELECT \n" +
                        "  o.orderpriority, \n" +
                        "  count(*) AS order_count \n" +
                        "FROM \n" +
                        "  \"orders\" o\n" +
                        "WHERE  \n" +
                        "  o.orderdate >= DATE '1993-07-01'\n" +
                        "  AND o.orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH\n" +
                        "  AND EXISTS (\n" +
                        "    SELECT \n" +
                        "      * \n" +
                        "    FROM \n" +
                        "      \"lineitem\" l\n" +
                        "    WHERE \n" +
                        "      l.orderkey = o.orderkey \n" +
                        "      AND l.commitdate < l.receiptdate\n" +
                        "  )\n" +
                        "GROUP BY \n" +
                        "  o.orderpriority\n" +
                        "ORDER BY \n" +
                        "  o.orderpriority\n");

        queries.put("q05",
                "SELECT\n" +
                        "  n.name,\n" +
                        "  sum(l.extendedprice * (1 - l.discount)) AS revenue\n" +
                        "FROM\n" +
                        "  \"customer\" AS c,\n" +
                        "  \"orders\" AS o,\n" +
                        "  \"lineitem\" AS l,\n" +
                        "  \"supplier\" AS s,\n" +
                        "  \"nation\" AS n,\n" +
                        "  \"region\" AS r\n" +
                        "WHERE\n" +
                        "  c.custkey = o.custkey\n" +
                        "  AND l.orderkey = o.orderkey\n" +
                        "  AND l.suppkey = s.suppkey\n" +
                        "  AND c.nationkey = s.nationkey\n" +
                        "  AND s.nationkey = n.nationkey\n" +
                        "  AND n.regionkey = r.regionkey\n" +
                        "  AND r.name = 'ASIA'\n" +
                        "  AND o.orderdate >= DATE '1994-01-01'\n" +
                        "  AND o.orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR\n" +
                        "GROUP BY\n" +
                        "  n.name\n" +
                        "ORDER BY\n" +
                        "  revenue DESC\n");

        queries.put("q06",
                "SELECT \n" +
                        "  sum(l.extendedprice*l.discount) AS revenue\n" +
                        "FROM \n" +
                        "  \"lineitem\" l\n" +
                        "WHERE \n" +
                        "  l.shipdate >= DATE '1994-01-01'\n" +
                        "  AND l.shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR\n" +
                        "  AND l.discount BETWEEN .06 - 0.01 AND .06 + 0.01\n" +
                        "  AND l.quantity < 24\n");

        queries.put("q07",
                "SELECT\n" +
                        "  supp_nation,\n" +
                        "  cust_nation,\n" +
                        "  l_year,\n" +
                        "  sum(volume) AS revenue\n" +
                        "FROM (\n" +
                        "       SELECT\n" +
                        "         n1.name                      AS supp_nation,\n" +
                        "         n2.name                      AS cust_nation,\n" +
                        "         extract(YEAR FROM l.shipdate)      AS l_year,\n" +
                        "         l.extendedprice * (1 - l.discount) AS volume\n" +
                        "       FROM\n" +
                        "         \"supplier\" AS s,\n" +
                        "         \"lineitem\" AS l,\n" +
                        "         \"orders\" AS o,\n" +
                        "         \"customer\" AS c,\n" +
                        "         \"nation\" AS n1,\n" +
                        "         \"nation\" AS n2\n" +
                        "       WHERE\n" +
                        "         s.suppkey = l.suppkey\n" +
                        "         AND o.orderkey = l.orderkey\n" +
                        "         AND c.custkey = o.custkey\n" +
                        "         AND s.nationkey = n1.nationkey\n" +
                        "         AND c.nationkey = n2.nationkey\n" +
                        "         AND (\n" +
                        "           (n1.name = 'FRANCE' AND n2.name = 'GERMANY')\n" +
                        "           OR (n1.name = 'GERMANY' AND n2.name = 'FRANCE')\n" +
                        "         )\n" +
                        "         AND l.shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'\n" +
                        "     ) AS shipping\n" +
                        "GROUP BY\n" +
                        "  supp_nation,\n" +
                        "  cust_nation,\n" +
                        "  l_year\n" +
                        "ORDER BY\n" +
                        "  supp_nation,\n" +
                        "  cust_nation,\n" +
                        "  l_year");

        queries.put("q08",
                "SELECT\n" +
                        "  o_year,\n" +
                        "  sum(CASE\n" +
                        "      WHEN nation = 'BRAZIL'\n" +
                        "        THEN volume\n" +
                        "      ELSE 0\n" +
                        "      END) / sum(volume) AS mkt_share\n" +
                        "FROM (\n" +
                        "       SELECT\n" +
                        "         extract(YEAR FROM o.orderdate)     AS o_year,\n" +
                        "         l.extendedprice * (1 - l.discount) AS volume,\n" +
                        "         n2.name                      AS nation\n" +
                        "       FROM\n" +
                        "         \"part\" AS p,\n" +
                        "         \"supplier\" AS s,\n" +
                        "         \"lineitem\" AS l,\n" +
                        "         \"orders\" AS o,\n" +
                        "         \"customer\" AS c,\n" +
                        "         \"nation\" AS n1,\n" +
                        "         \"nation\" AS n2,\n" +
                        "         \"region\" AS r\n" +
                        "       WHERE\n" +
                        "         p.partkey = l.partkey\n" +
                        "         AND s.suppkey = l.suppkey\n" +
                        "         AND l.orderkey = o.orderkey\n" +
                        "         AND o.custkey = c.custkey\n" +
                        "         AND c.nationkey = n1.nationkey\n" +
                        "         AND n1.regionkey = r.regionkey\n" +
                        "         AND r.name = 'AMERICA'\n" +
                        "         AND s.nationkey = n2.nationkey\n" +
                        "         AND o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'\n" +
                        "         AND p.type = 'ECONOMY ANODIZED STEEL'\n" +
                        "     ) AS all_nations\n" +
                        "GROUP BY\n" +
                        "  o_year\n" +
                        "ORDER BY\n" +
                        "  o_year\n");

        queries.put("q09",
                "SELECT\n" +
                        "  nation,\n" +
                        "  o_year,\n" +
                        "  sum(amount) AS sum_profit\n" +
                        "FROM (\n" +
                        "       SELECT\n" +
                        "         n.name                                              AS nation,\n" +
                        "         extract(YEAR FROM o.orderdate)                          AS o_year,\n" +
                        "         l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity AS amount\n" +
                        "       FROM\n" +
                        "         \"part\" AS p,\n" +
                        "         \"supplier\" AS s,\n" +
                        "         \"lineitem\" AS l,\n" +
                        "         \"partsupp\" AS ps,\n" +
                        "         \"orders\" AS o,\n" +
                        "         \"nation\" AS n\n" +
                        "       WHERE\n" +
                        "         s.suppkey = l.suppkey\n" +
                        "         AND ps.suppkey = l.suppkey\n" +
                        "         AND ps.partkey = l.partkey\n" +
                        "         AND p.partkey = l.partkey\n" +
                        "         AND o.orderkey = l.orderkey\n" +
                        "         AND s.nationkey = n.nationkey\n" +
                        "         AND p.name LIKE '%green%'\n" +
                        "     ) AS profit\n" +
                        "GROUP BY\n" +
                        "  nation,\n" +
                        "  o_year\n" +
                        "ORDER BY\n" +
                        "  nation,\n" +
                        "  o_year DESC\n");

        queries.put("q10",
                "SELECT\n" +
                        "  c.custkey,\n" +
                        "  c.name,\n" +
                        "  sum(l.extendedprice * (1 - l.discount)) AS revenue,\n" +
                        "  c.acctbal,\n" +
                        "  n.name,\n" +
                        "  c.address,\n" +
                        "  c.phone,\n" +
                        "  c.comment\n" +
                        "FROM\n" +
                        "  \"lineitem\" AS l,\n" +
                        "  \"orders\" AS o,\n" +
                        "  \"customer\" AS c,\n" +
                        "  \"nation\" AS n\n" +
                        "WHERE\n" +
                        "  c.custkey = o.custkey\n" +
                        "  AND l.orderkey = o.orderkey\n" +
                        "  AND o.orderdate >= DATE '1993-10-01'\n" +
                        "  AND o.orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH\n" +
                        "  AND l.returnflag = 'R'\n" +
                        "  AND c.nationkey = n.nationkey\n" +
                        "GROUP BY\n" +
                        "  c.custkey,\n" +
                        "  c.name,\n" +
                        "  c.acctbal,\n" +
                        "  c.phone,\n" +
                        "  n.name,\n" +
                        "  c.address,\n" +
                        "  c.comment\n" +
                        "ORDER BY\n" +
                        "  revenue DESC\n" +
                        "LIMIT 20\n");

        QUERIES = queries.build();
    }

    public static String getQuery(String queryId)
    {
        return QUERIES.get(queryId);
    }
}
