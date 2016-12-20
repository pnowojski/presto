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

import com.facebook.presto.sql.planner.Plan;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 30)
@Measurement(iterations = 30)
public class BenchmarkTPCHSmall
{
    @State(Thread)
    public static class Context
    {
        @Param({"q05", "q07", "q08", "q09", "q10"})
        private String query = "q09";

        private String schema = "\"sf0.05\"";

        private final MemoryLocalQueryRunner queryRunner = new MemoryLocalQueryRunner();

        private Plan queryPlan;

        @Setup
        public void setUp()
        {
            queryRunner.execute(format("CREATE TABLE memory.default.lineitem AS SELECT * FROM tpch.%s.lineitem", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.orders AS SELECT * FROM tpch.%s.orders", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.part AS SELECT * FROM tpch.%s.part", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.partsupp AS SELECT * FROM tpch.%s.partsupp", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.supplier AS SELECT * FROM tpch.%s.supplier", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.customer AS SELECT * FROM tpch.%s.customer", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.nation AS SELECT * FROM tpch.%s.nation", schema));
            queryRunner.execute(format("CREATE TABLE memory.default.region AS SELECT * FROM tpch.%s.region", schema));

            queryPlan = queryRunner.plan(TPCHQueries.getQuery(query));
        }

        public void run()
        {
            queryRunner.execute(queryPlan);
        }
    }

    @Benchmark
    public void run(Context context)
    {
        context.run();
    }
}
