package io.druid.query.count;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/7/13
 * Time: 11:45 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.QueryGranularity;
import io.druid.query.*;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.*;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class FrequencyCountQueryRunnerTest
{

    private static final Interval DATA_INTERVAL = new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z");
    private static QuerySegmentFinder testFinder;
    private static final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    private static final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    private static final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    private static final Segment rtSegment = new IncrementalIndexSegment(rtIndex);
    private static final Segment mMappedSegment =  new QueryableIndexSegment(null, mMappedTestIndex);
    private static final Segment mergedSegment = new QueryableIndexSegment(null, mergedRealtimeIndex);

    public static Collection<?> makeQueryRunners(
            QueryRunnerFactory factory
    )
            throws IOException
    {


        return Arrays.asList(
                new Object[][]{/*
                        {
                                makeQueryRunner(factory, rtSegment, new SegmentDescriptor(DATA_INTERVAL, "one", 1))
                        },*/
                        {
                                makeQueryRunner(factory, mMappedSegment, new SegmentDescriptor(DATA_INTERVAL, "two", 2))
                        },
                        {
                                makeQueryRunner(factory, mergedSegment, new SegmentDescriptor(DATA_INTERVAL, "three", 3))
                        }
                }
        );
    }

    public static <T> QueryRunner<T> makeQueryRunner(
            QueryRunnerFactory<T, Query<T>> factory,
            Segment adapter,
            SegmentDescriptor descriptor
    )
    {
        ReferenceCountingSegment rfSegment = new ReferenceCountingSegment(adapter);
        rfSegment.setMetadata(descriptor);
        return new FinalizeResultsQueryRunner<T>(
                factory.createRunner(rfSegment),
                factory.getToolchest()
        );
    }

    @Parameterized.Parameters
    public static Collection<?> constructorFeeder() throws IOException
    {
        testFinder = new QuerySegmentFinder() {
            @Override
            public Optional<Segment> findSegment(String dataSource, SegmentDescriptor spec) {
                if (spec.getPartitionNumber() == 1) {
                    return Optional.of(rtSegment);
                } else if (spec.getPartitionNumber() == 2) {
                    return Optional.of(mMappedSegment);
                } else {
                    return Optional.of(mergedSegment);
                }
            }
        };
        FrequencyCountQueryEngine testEngine = new FrequencyCountQueryEngine(testFinder);
        return makeQueryRunners(
                new FrequencyCountQueryRunnerFactory(new FrequencyCountQueryQueryToolChest(), testEngine)
        );
    }

    private final QueryRunner runner;

    public FrequencyCountQueryRunnerTest(
            QueryRunner runner
    )
    {
        this.runner = runner;
    }


    @Test
    public void testFullOnCount()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                .addDimension(QueryRunnerTestHelper.qualityDimension)
                .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );

        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("quality",
                                createMapOf("automotive", 0, "technology", 0, "mezzanine", 93, "news", 0, "premium", 93, "travel", 0, "health", 0, "entertainment", 0, "business", 0)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }

    @Test
    public void testFullOnCountNoFilter()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                .addDimension(QueryRunnerTestHelper.qualityDimension)
                //.filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );

        int count = 0;
        Result<FrequencyCountResult> lastResult = null;
        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("quality",
                                createMapOf("automotive", 93, "technology", 93, "mezzanine", 279, "news", 93, "premium", 279, "travel", 93, "health", 93, "entertainment", 93, "business", 93)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }

    @Test
    public void testWithSearch()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        SearchQuerySpec searchQuerySpec = new InSearchQuerySpec(Arrays.asList("premium", "travel"));
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                .addDimension(QueryRunnerTestHelper.qualityDimension)
                .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .query(searchQuerySpec)
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );

        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("quality",
                                ImmutableMap.of("premium", 93, "travel", 0)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }

    @Test
    public void testNoDimensions()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                //.addDimension(QueryRunnerTestHelper.qualityDimension)
                .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );

        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("count",
                                ImmutableMap.of("total", 186)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }

    @Test
    public void testNoDimensionsNoFilter()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                //.addDimension(QueryRunnerTestHelper.qualityDimension)
                //.filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );

        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("count",
                                ImmutableMap.of("total", 1209)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }

    @Test
    public void testJoinWithFilter()
    {
        QueryGranularity gran = QueryGranularity.DAY;
        FrequencyCountQuery query = ModuleHelper.newFrequencyCountQueryBuilder()
                .dataSource(QueryRunnerTestHelper.dataSource)
                .intervals(QueryRunnerTestHelper.fullOnInterval)
                        //.addDimension(QueryRunnerTestHelper.qualityDimension)
                .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                .addJoin(new DefaultJoinSpec("testDataSource", new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "premium")))
                .build();

        DateTime expectedEarliest = new DateTime("2011-01-12");
        DateTime expectedLast = new DateTime("2011-04-15");

        Iterable<Result<FrequencyCountResult>> results = Sequences.toList(
                runner.run(query),
                Lists.<Result<FrequencyCountResult>>newArrayList()
        );
        /*
        int count = 0;
        Result<FrequencyCountResult> lastResult = null;
        for (Result<FrequencyCountResult> result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }*/
        List<Result<FrequencyCountResult>> expectedResults = Arrays.asList(
                new Result<FrequencyCountResult>(
                        new DateTime("2011-01-12"),
                        new FrequencyCountResult("count",
                                ImmutableMap.of("total", 93)
                        )
                )
        );
        TestHelper.assertExpectedResults(expectedResults, results);
    }


    static <K, V> ImmutableMap<K, V> createMapOf(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9) {
        return  new ImmutableMap.Builder<K, V>()
                .put(k1, v1)
                .put(k2, v2)
                .put(k3, v3)
                .put(k4, v4)
                .put(k5, v5)
                .put(k6, v6)
                .put(k7, v7)
                .put(k8, v8)
                .put(k9, v9)
                .build();
    }
}

