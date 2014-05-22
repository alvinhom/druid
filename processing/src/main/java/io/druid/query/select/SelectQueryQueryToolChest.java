/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.granularity.QueryGranularity;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSourceUtil;
import io.druid.query.IntervalChunkingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.filter.DimFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class SelectQueryQueryToolChest extends QueryToolChest<Result<SelectResultValue>, SelectQuery>
{
  private static final byte SELECT_QUERY = 0x13;
  private static final Joiner COMMA_JOIN = Joiner.on(",");
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<SelectResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectResultValue>>()
      {
      };
  private final QueryConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public SelectQueryQueryToolChest(QueryConfig config, ObjectMapper jsonMapper)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeResults(QueryRunner<Result<SelectResultValue>> queryRunner)
  {
    return new ResultMergeQueryRunner<Result<SelectResultValue>>(queryRunner)
    {
      @Override
      protected Ordering<Result<SelectResultValue>> makeOrdering(Query<Result<SelectResultValue>> query)
      {
        return Ordering.from(
            new ResultGranularTimestampComparator<SelectResultValue>(
                ((SelectQuery) query).getGranularity()
            )
        );
      }

      @Override
      protected BinaryFn<Result<SelectResultValue>, Result<SelectResultValue>, Result<SelectResultValue>> createMergeFn(
          Query<Result<SelectResultValue>> input
      )
      {
        SelectQuery query = (SelectQuery) input;
        return new SelectBinaryFn(
            query.getGranularity(),
            query.getPagingSpec()
        );
      }
    };
  }

  @Override
  public Sequence<Result<SelectResultValue>> mergeSequences(Sequence<Sequence<Result<SelectResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<SelectResultValue>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SelectQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
        .setUser4("Select")
        .setUser5(COMMA_JOIN.join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Result<SelectResultValue>, Result<SelectResultValue>> makePreComputeManipulatorFn(
      final SelectQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SelectResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SelectResultValue>, Object, SelectQuery> getCacheStrategy(final SelectQuery query)
  {
    return new CacheStrategy<Result<SelectResultValue>, Object, SelectQuery>()
    {
      @Override
      public byte[] computeCacheKey(SelectQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        final Set<String> dimensions = Sets.newTreeSet();
        if (query.getDimensions() != null) {
          dimensions.addAll(query.getDimensions());
        }

        final byte[][] dimensionsBytes = new byte[dimensions.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (String dimension : dimensions) {
          dimensionsBytes[index] = dimension.getBytes();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final Set<String> metrics = Sets.newTreeSet();
        if (query.getMetrics() != null) {
          metrics.addAll(query.getMetrics());
        }

        final byte[][] metricBytes = new byte[metrics.size()][];
        int metricBytesSize = 0;
        index = 0;
        for (String metric : metrics) {
          metricBytes[index] = metric.getBytes();
          metricBytesSize += metricBytes[index].length;
          ++index;
        }

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1
                + granularityBytes.length
                + filterBytes.length
                + query.getPagingSpec().getCacheKey().length
                + dimensionsBytesSize
                + metricBytesSize
            )
            .put(SELECT_QUERY)
            .put(granularityBytes)
            .put(filterBytes)
            .put(query.getPagingSpec().getCacheKey());

        for (byte[] dimensionsByte : dimensionsBytes) {
          queryCacheKey.put(dimensionsByte);
        }

        for (byte[] metricByte : metricBytes) {
          queryCacheKey.put(metricByte);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SelectResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SelectResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<SelectResultValue> input)
          {
            return Arrays.asList(
                input.getTimestamp().getMillis(),
                input.getValue().getPagingIdentifiers(),
                input.getValue().getEvents()
            );
          }
        };
      }

      @Override
      public Function<Object, Result<SelectResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SelectResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Result<SelectResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = granularity.toDateTime(((Number) resultIter.next()).longValue());

            return new Result<SelectResultValue>(
                timestamp,
                new SelectResultValue(
                    (Map<String, Integer>) jsonMapper.convertValue(
                        resultIter.next(), new TypeReference<Map<String, Integer>>()
                    {
                    }
                    ),
                    (List<EventHolder>) jsonMapper.convertValue(
                        resultIter.next(), new TypeReference<List<EventHolder>>()
                    {
                    }
                    )
                )
            );
          }
        };
      }

      @Override
      public Sequence<Result<SelectResultValue>> mergeSequences(Sequence<Sequence<Result<SelectResultValue>>> seqOfSequences)
      {
        return new MergeSequence<Result<SelectResultValue>>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> preMergeQueryDecoration(QueryRunner<Result<SelectResultValue>> runner)
  {
    return new IntervalChunkingQueryRunner<Result<SelectResultValue>>(
        runner,
        config.getChunkPeriod()

    );
  }

  public Ordering<Result<SelectResultValue>> getOrdering()
  {
    return Ordering.natural();
  }
}
