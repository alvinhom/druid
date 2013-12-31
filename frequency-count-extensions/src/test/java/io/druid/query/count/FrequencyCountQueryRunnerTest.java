package io.druid.query.count;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/7/13
 * Time: 11:45 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.common.base.Optional;
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
                new Object[][]{
                        {
                                makeQueryRunner(factory, rtSegment, new SegmentDescriptor(DATA_INTERVAL, "one", 1))
                        },
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
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

        Iterable<FrequencyCountResult> results = Sequences.toList(
                runner.run(query),
                Lists.<FrequencyCountResult>newArrayList()
        );

        int count = 0;
        FrequencyCountResult lastResult = null;
        for (FrequencyCountResult result : results) {
            lastResult = result;

            System.out.println(result);

            expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
            ++count;
        }

        // Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
    }
}

