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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughJoin;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PartialAggregationPushDown
        implements PlanOptimizer
{
    private final FunctionRegistry functionRegistry;

    public PartialAggregationPushDown(FunctionRegistry registry)
    {
        requireNonNull(registry, "registry is null");

        this.functionRegistry = registry;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(symbolAllocator, idAllocator, isPushAggregationThroughJoin(session)), plan, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final SymbolAllocator allocator;
        private final PlanNodeIdAllocator idAllocator;
        private final boolean pushAggregationThroughJoin;

        public Rewriter(SymbolAllocator allocator, PlanNodeIdAllocator idAllocator, boolean pushAggregationThroughJoin)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.pushAggregationThroughJoin = pushAggregationThroughJoin;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode child = node.getSource();

            if (child instanceof JoinNode && pushAggregationThroughJoin) {
                return pushPartialThroughJoin(node, (JoinNode) child, context);
            }

            if (!(child instanceof ExchangeNode)) {
                return context.defaultRewrite(node);
            }

            // partial aggregation can only be pushed through exchange that doesn't change
            // the cardinality of the stream (i.e., gather or repartition)
            ExchangeNode exchange = (ExchangeNode) child;
            if ((exchange.getType() != GATHER && exchange.getType() != REPARTITION) ||
                    exchange.getPartitioningScheme().isReplicateNulls()) {
                return context.defaultRewrite(node);
            }

            if (exchange.getType() == REPARTITION) {
                // if partitioning columns are not a subset of grouping keys,
                // we can't push this through
                List<Symbol> partitioningColumns = exchange.getPartitioningScheme()
                        .getPartitioning()
                        .getArguments()
                        .stream()
                        .filter(Partitioning.ArgumentBinding::isVariable)
                        .map(Partitioning.ArgumentBinding::getColumn)
                        .collect(Collectors.toList());

                if (!node.getGroupingKeys().containsAll(partitioningColumns)) {
                    return context.defaultRewrite(node);
                }
            }

            // currently, we only support plans that don't use pre-computed hash functions
            if (node.getHashSymbol().isPresent() || exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                return context.defaultRewrite(node);
            }

            boolean decomposable = node.getFunctions().values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);

            if (!decomposable) {
                return context.defaultRewrite(node);
            }

            switch (node.getStep()) {
                case SINGLE:
                    // Split it into a FINAL on top of a PARTIAL and
                    // reprocess the resulting plan to push the partial
                    // below the exchange (see case below).
                    return context.rewrite(split(node));
                case PARTIAL:
                    // Push it underneath each branch of the exchange
                    // and reprocess in case it can be pushed further down
                    // (e.g., if there are local/remote exchanges stacked)
                    return context.rewrite(pushPartial(node, exchange));
                default:
                    return context.defaultRewrite(node);
            }
        }

        private PlanNode pushPartialThroughJoin(AggregationNode node, JoinNode child, RewriteContext<Void> context)
        {
            if (node.getStep() != PARTIAL) {
                return context.defaultRewrite(node);
            }
            if (child.getType() != JoinNode.Type.INNER) {
                return context.defaultRewrite(node);
            }
            if (!joinKeyColumnsSameAsGroupingSet(node, child)) {
                return context.defaultRewrite(node);
            }

            // TODO: leave partial aggregation above Join?
            if (allAggregationsOn(node.getAggregations(), child.getLeft().getOutputSymbols())) {
                return pushPartialToLeftChild(node, child, context);
            }
            if (allAggregationsOn(node.getAggregations(), child.getRight().getOutputSymbols())) {
                return pushPartialToRightChild(node, child, context);
            }

            return context.defaultRewrite(node);
        }

        private PlanNode pushPartialToLeftChild(AggregationNode node, JoinNode child, RewriteContext<Void> context)
        {
            AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), child.getCriteria(), context);
            return pushPartialToJoin(pushedAggregation, child, pushedAggregation, context.rewrite(child.getRight()), child.getRight().getOutputSymbols());
        }

        private PlanNode pushPartialToRightChild(AggregationNode node, JoinNode child, RewriteContext<Void> context)
        {
            AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), child.getCriteria(), context);
            return pushPartialToJoin(pushedAggregation, child, context.rewrite(child.getLeft()), pushedAggregation, child.getLeft().getOutputSymbols());
        }

        private PlanNode pushPartialToJoin(
                AggregationNode pushedAggregation,
                JoinNode child,
                PlanNode leftChild,
                PlanNode rightChild,
                Collection<Symbol> otherSymbols)
        {
            ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
            outputSymbols.addAll(pushedAggregation.getOutputSymbols());
            outputSymbols.addAll(otherSymbols);

            return new JoinNode(
                    child.getId(),
                    child.getType(),
                    leftChild,
                    rightChild,
                    child.getCriteria(),
                    outputSymbols.build(),
                    child.getFilter(),
                    child.getLeftHashSymbol(),
                    child.getRightHashSymbol(),
                    child.getDistributionType());
        }

        private AggregationNode replaceAggregationSource(AggregationNode aggregation, PlanNode source, List<EquiJoinClause> criteria, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(source);
            ImmutableSet<Symbol> rewrittenSourceSymbols = ImmutableSet.copyOf(rewrittenSource.getOutputSymbols());
            ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();

            for (EquiJoinClause joinClause : criteria) {
                if (rewrittenSourceSymbols.contains(joinClause.getLeft())) {
                    mapping.put(joinClause.getRight(), joinClause.getLeft());
                }
                else {
                    mapping.put(joinClause.getLeft(), joinClause.getRight());
                }
            }

            return new SymbolMapper(mapping.build()).map(aggregation, source, aggregation.getId());
        }

        private boolean allAggregationsOn(Map<Symbol, FunctionCall> aggregations, List<Symbol> outputSymbols)
        {
            Set<String> outputNames = outputSymbols.stream().map(Symbol::getName).collect(toImmutableSet());

            return aggregations.values().stream()
                    .flatMap(functionCall -> functionCall.getArguments().stream())
                    .allMatch(expression -> isSymbolReferenceTo(expression, outputNames));
        }

        private boolean isSymbolReferenceTo(Expression expression, Set<String> outputNames)
        {
            return expression instanceof SymbolReference && outputNames.contains(((SymbolReference) expression).getName());
        }

        private boolean joinKeyColumnsSameAsGroupingSet(AggregationNode aggregation, JoinNode join)
        {
            if (join.getFilter().isPresent()) {
                return false;
            }
            if (aggregation.getGroupingSets().size() != 1) {
                return false;
            }
            Set<Symbol> groupingSet = ImmutableSet.copyOf(Iterables.getOnlyElement(aggregation.getGroupingSets()));
            for (EquiJoinClause clause : join.getCriteria()) {
                if (!groupingSet.contains(clause.getLeft()) && !groupingSet.contains(clause.getRight())) {
                    return false;
                }
            }
            return true;
        }

        private PlanNode pushPartial(AggregationNode partial, ExchangeNode exchange)
        {
            List<PlanNode> partials = new ArrayList<>();
            for (int i = 0; i < exchange.getSources().size(); i++) {
                PlanNode source = exchange.getSources().get(i);

                ImmutableMap.Builder<Symbol, Symbol> mappingsBuilder = ImmutableMap.builder();
                for (int outputIndex = 0; outputIndex < exchange.getOutputSymbols().size(); outputIndex++) {
                    Symbol output = exchange.getOutputSymbols().get(outputIndex);
                    Symbol input = exchange.getInputs().get(i).get(outputIndex);
                    if (!output.equals(input)) {
                        mappingsBuilder.put(output, input);
                    }
                }

                Map<Symbol, Symbol> mappings = mappingsBuilder.build();
                SymbolMapper symbolMapper = new SymbolMapper(mappings);
                AggregationNode mappedPartial = symbolMapper.map(partial, source, idAllocator.getNextId());

                Assignments.Builder assignments = Assignments.builder();
                for (int outputIndex = 0; outputIndex < partial.getOutputSymbols().size(); outputIndex++) {
                    Symbol output = partial.getOutputSymbols().get(outputIndex);
                    Symbol input = mappings.getOrDefault(output, output);
                    assignments.put(output, input.toSymbolReference());
                }
                partials.add(new ProjectNode(idAllocator.getNextId(), mappedPartial, assignments.build()));
            }

            for (PlanNode node : partials) {
                verify(partial.getOutputSymbols().equals(node.getOutputSymbols()));
            }

            // Since this exchange source is now guaranteed to have the same symbols as the inputs to the the partial
            // aggregation, we don't need to rewrite symbols in the partitioning function
            PartitioningScheme partitioning = new PartitioningScheme(
                    exchange.getPartitioningScheme().getPartitioning(),
                    partial.getOutputSymbols(),
                    exchange.getPartitioningScheme().getHashColumn(),
                    exchange.getPartitioningScheme().isReplicateNulls(),
                    exchange.getPartitioningScheme().getBucketToPartition());

            return new ExchangeNode(
                    idAllocator.getNextId(),
                    exchange.getType(),
                    exchange.getScope(),
                    partitioning,
                    partials,
                    ImmutableList.copyOf(Collections.nCopies(partials.size(), partial.getOutputSymbols())));
        }

        private PlanNode split(AggregationNode node)
        {
            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(signature);

                Symbol intermediateSymbol = allocator.newSymbol(signature.getName(), function.getIntermediateType());
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(intermediateSymbol.toSymbolReference())));
            }

            PlanNode partial = new AggregationNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    intermediateCalls,
                    intermediateFunctions,
                    intermediateMask,
                    node.getGroupingSets(),
                    PARTIAL,
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());

            return new AggregationNode(
                    node.getId(),
                    partial,
                    finalCalls,
                    node.getFunctions(),
                    ImmutableMap.of(),
                    node.getGroupingSets(),
                    FINAL,
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }
    }
}
