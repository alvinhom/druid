/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.query.count;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.JodaUtils;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulationFn;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FrequencyCountQueryQueryToolChest extends QueryToolChest<Result<FrequencyCountResult>, FrequencyCountQuery>
{
  private static final TypeReference<Result<FrequencyCountResult>> TYPE_REFERENCE = new TypeReference<Result<FrequencyCountResult>>(){};

  @Override
  public QueryRunner<Result<FrequencyCountResult>> mergeResults(final QueryRunner<Result<FrequencyCountResult>> runner)
  {
    return new ResultMergeQueryRunner<Result<FrequencyCountResult>>(runner)
    {
      @Override
      protected Ordering<Result<FrequencyCountResult>> makeOrdering(Query<Result<FrequencyCountResult>> query)
      {
        return getOrdering();
        /* TODO not sure above works
        return Ordering.from(
            new ResultGranularTimestampComparator<FrequencyCountResult>(
                ((FrequencyCountQuery) query).getGranularity()
            )
        );
        */
      }

      @Override
      protected BinaryFn<Result<FrequencyCountResult>, Result<FrequencyCountResult>, Result<FrequencyCountResult>> createMergeFn(final Query<Result<FrequencyCountResult>> inQ)
      {
        return new BinaryFn<Result<FrequencyCountResult>, Result<FrequencyCountResult>, Result<FrequencyCountResult>>()
        {
          private final FrequencyCountQuery query = (FrequencyCountQuery) inQ;

          @Override
          public Result<FrequencyCountResult> apply(Result<FrequencyCountResult> arg1, Result<FrequencyCountResult> arg2)
          {
            if (arg1 == null) {
              return arg2;
            }

            if (arg2 == null) {
              return arg1;
            }

            FrequencyCountResult arg1Val = arg1.getValue();
            FrequencyCountResult arg2Val = arg2.getValue();

            /*  TODO doublecheck the merge
            if (!query.isMerge()) {
              throw new ISE("Merging when a merge isn't supposed to happen[%s], [%s]", arg1, arg2);
            }
            */

            /*
            List<Interval> newIntervals = JodaUtils.condenseIntervals(
                    Iterables.concat(arg1Val.getIntervals(), arg2Val.getIntervals())
            );
            */

            final Map<String, Integer> leftColumns = arg1Val.getDimensionCounts();
            final Map<String, Integer> rightColumns = arg2Val.getDimensionCounts();
            Map<String, Integer> columns = Maps.newTreeMap();

            Set<String> rightColumnNames = Sets.newHashSet(rightColumns.keySet());
            for (Map.Entry<String, Integer> entry : leftColumns.entrySet()) {
              final String columnName = entry.getKey();
              // Merge by adding the counts together.
              Integer right = rightColumns.get(columnName);
              columns.put(columnName, entry.getValue() + ((right == null) ? 0 : right));
              rightColumnNames.remove(columnName);
            }

            for (String columnName : rightColumnNames) {
              columns.put(columnName, rightColumns.get(columnName));
            }

            return new Result<FrequencyCountResult>(arg1.getTimestamp(), new FrequencyCountResult("merged", columns));
          }
        };
      }
    };
  }

  @Override
  public Sequence<Result<FrequencyCountResult>> mergeSequences(Sequence<Sequence<Result<FrequencyCountResult>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<FrequencyCountResult>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(FrequencyCountQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource())
        .setUser4(query.getType())
        .setUser5(Joiner.on(",").join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.valueOf(query.hasFilters()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Result<FrequencyCountResult>, Result<FrequencyCountResult>> makeMetricManipulatorFn(
      FrequencyCountQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<FrequencyCountResult>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  public Ordering<Result<FrequencyCountResult>> getOrdering()
  {
    return Ordering.natural();
  }
}
