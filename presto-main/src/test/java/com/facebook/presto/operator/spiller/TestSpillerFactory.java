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
package com.facebook.presto.operator.spiller;

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.testing.Assertions.assertInstanceOf;

public class TestSpillerFactory
{
    @Test
    public void testOverrideDefaultSpiller()
            throws Exception
    {
        List<Module> additionalModules = ImmutableList.of(new CustomSpillerModule());
        Map<String, String> serverProperties = ImmutableMap.of("experimental.spiller-implementation", "custom");
        try (TestingPrestoServer server = new TestingPrestoServer(true, serverProperties, null, null, new SqlParserOptions(), additionalModules)) {
            assertInstanceOf(server.getSpillerFactory(), CustomSpillerFactory.class);
        }
    }

    private static class CustomSpillerModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(SpillerFactory.class).to(CustomSpillerFactory.class).in(Scopes.SINGLETON);
        }
    }

    private static class CustomSpillerFactory
            implements SpillerFactory
    {
        @Override
        public Spiller create(List<Type> types)
        {
            throw new IllegalStateException("Must not be called");
        }
    }
}
