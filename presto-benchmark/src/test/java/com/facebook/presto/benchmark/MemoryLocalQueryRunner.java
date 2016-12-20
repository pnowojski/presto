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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.plugin.memory.MemoryConnectorFactory;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.NullOutputOperator;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.intellij.lang.annotations.Language;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.Unit.TERABYTE;

public class MemoryLocalQueryRunner
{
    private static Session DEFAULT_SESSION = testSessionBuilder()
            .setSystemProperty("optimizer.optimize-hash-generation", "true")
            .setSystemProperty(SystemSessionProperties.REORDER_JOINS, "true")
            .setCatalog("memory")
            .setSchema("default").build();

    protected LocalQueryRunner localQueryRunner = createMemoryLocalQueryRunner();
    protected MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
    protected MemoryPool systemMemoryPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));

    protected SpillSpaceTracker spillTracker = new SpillSpaceTracker(new DataSize(1, TERABYTE));
    protected TaskContext taskContext = new QueryContext(new QueryId("test"), new DataSize(512, MEGABYTE), memoryPool, systemMemoryPool, localQueryRunner.getExecutor(), new DataSize(1, TERABYTE), spillTracker)
            .addTaskContext(new TaskStateMachine(new TaskId("query", 0, 0), localQueryRunner.getExecutor()),
                    DEFAULT_SESSION,
                    false,
                    false);

    public Plan plan(@Language("SQL") String query)
    {
        return localQueryRunner.createPlan(query);
    }

    public void execute(Plan plan)
    {
        Queue<Driver> drivers = new ArrayDeque<>(localQueryRunner.createDrivers(plan, new NullOutputOperator.NullOutputFactory(), taskContext));
        Set<Driver> blockedDrivers = new ConcurrentHashSet<>();

        while (!drivers.isEmpty() || !blockedDrivers.isEmpty()) {
            synchronized (this) {
                Driver driver = drivers.poll();
                if (driver == null || driver.isFinished()) {
                    continue;
                }
                ListenableFuture<?> blocked = driver.process();
                if (blocked.isDone()) {
                    drivers.add(driver);
                }
                else {
                    blockedDrivers.add(driver);
                    blocked.addListener(() -> {
                        synchronized (this) {
                            drivers.add(driver);
                            blockedDrivers.remove(driver);
                        }
                    }, localQueryRunner.getExecutor());
                }
            }
        }
    }

    public void execute(@Language("SQL") String query)
    {
        execute(plan(query));
    }

    private static LocalQueryRunner createMemoryLocalQueryRunner()
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(DEFAULT_SESSION);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.<String, String>of());
        localQueryRunner.createCatalog("memory", new MemoryConnectorFactory(1), ImmutableMap.<String, String>of());

        return localQueryRunner;
    }
}
