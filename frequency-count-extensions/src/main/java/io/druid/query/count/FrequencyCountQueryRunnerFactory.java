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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.query.*;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.*;
import io.druid.segment.filter.Filters;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class FrequencyCountQueryRunnerFactory implements QueryRunnerFactory<Result<FrequencyCountResult>, FrequencyCountQuery>
{

  // private static final FrequencyCountQueryQueryToolChest toolChest = new FrequencyCountQueryQueryToolChest();
  private final FrequencyCountQueryQueryToolChest toolChest;
  private final FrequencyCountQueryEngine engine;

  @Inject
  public FrequencyCountQueryRunnerFactory(
            FrequencyCountQueryQueryToolChest toolChest,
            FrequencyCountQueryEngine engine
  )
  {
        this.toolChest = toolChest;
        this.engine = engine;
  }

  @Override
  public QueryRunner<Result<FrequencyCountResult>> createRunner(final Segment segment)
  {
    return new FrequencyCountQueryRunner(segment, engine);
  }

  @Override
  public QueryRunner<Result<FrequencyCountResult>> mergeRunners(
      final ExecutorService queryExecutor, Iterable<QueryRunner<Result<FrequencyCountResult>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<FrequencyCountResult>>(queryExecutor, toolChest.getOrdering(), queryRunners);
  }

  @Override
  public QueryToolChest<Result<FrequencyCountResult>, FrequencyCountQuery> getToolchest()
  {
    return toolChest;
  }

  private static class CountOrdering extends Ordering<Result<FrequencyCountResult>>
  {
      @Override
      public int compare(Result<FrequencyCountResult> left, Result<FrequencyCountResult> right)
      {
          // order does not matter
          return 1;
      }
  }

  private static class FrequencyCountQueryRunner implements QueryRunner<Result<FrequencyCountResult>>
  {
      private final Segment segment;
      private final FrequencyCountQueryEngine engine;

      public FrequencyCountQueryRunner(Segment segment, FrequencyCountQueryEngine engine) {
          this.segment = segment;
          this.engine = engine;
      }

      @Override
      public Sequence<Result<FrequencyCountResult>> run (final Query<Result<FrequencyCountResult>> input)
      {
          if (!(input instanceof FrequencyCountQuery)) {
              throw new ISE("Got a [%s] which isin't a %s", input.getClass(), FrequencyCountQuery.class);
          }

          final FrequencyCountQuery query = (FrequencyCountQuery) input;
          return engine.process(query, segment, Filters.convertDimensionFilters(query.getDimensionsFilter()));

      }

      /*
      private Iterable<FrequencyCountResult> runFrequencyCount(final FrequencyCountQuery query, final Filter filter)
      {
          final List<String> dimensions = query.getDimensions();

          final List<FrequencyCountResult> retVal = Lists.newArrayList();

          SearchQuerySpec searchQuerySpec = query.getQuery();

          ImmutableConciseSet filterSet = null;
          if (index != null) {
              BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(index);
              if (filter != null) {
                  filterSet = filter.goConcise(selector);
              }
              List<JoinSpec> joinSpecs = query.getJoin();
              if (joinSpecs != null && !joinSpecs.isEmpty()) {
                  // lookup the data source from segment finder
                  if (segment instanceof ReferenceCountingSegment) {
                      SegmentDescriptor descriptor = ((ReferenceCountingSegment) segment).getMetadata();
                      if (descriptor != null) {
                          for (JoinSpec aJoin: joinSpecs) {
                            Optional<Segment> joinSegment = finder.findSegment(aJoin.getDatasource(), descriptor);
                            Segment segment = joinSegment.get();
                            BitmapIndexSelector joinSelector = new ColumnSelectorBitmapIndexSelector(segment.asQueryableIndex());
                            Filter joinFilter = Filters.convertDimensionFilters(aJoin.getFilter());
                            ImmutableConciseSet rhs = joinFilter.goConcise(joinSelector);
                            List<ImmutableConciseSet> conciseSets = Lists.newArrayList(filterSet, rhs);
                            ImmutableConciseSet intersects = ImmutableConciseSet.intersection(conciseSets);
                            filterSet = intersects;
                          }
                      }
                  } else {
                      throw new ISE("Invalid segment, expecting ReferenceCountingSegment but got " + segment.getClass().getName());
                  }
              }
              // check to see if we need are doing group by
              if (dimensions == null || dimensions.isEmpty()) {
                  Map<String, Integer> dimensionMap = new HashMap<String, Integer>();
                  if (filterSet != null) {
                      dimensionMap.put("total", filterSet.size());
                  } else {
                      dimensionMap.put("total", selector.getNumRows());
                  }
                  retVal.add(new FrequencyCountResult("count", Lists.newArrayList(adapter.getInterval()), dimensionMap));
              } else {
                  for (String dimension : dimensions) {
                      Map<String, Integer> dimensionMap = new HashMap<String, Integer>();
                      Iterable<String> dims = selector.getDimensionValues(dimension);
                      if (dims != null) {
                          for (String dimVal : dims) {
                              dimVal = dimVal == null ? "" : dimVal;
                              if (searchQuerySpec == null || searchQuerySpec.accept(dimVal)) {
                                  if (filterSet != null) {
                                      ImmutableConciseSet lhs = selector.getConciseInvertedIndex(dimension, dimVal);
                                      ImmutableConciseSet rhs = filterSet;

                                      List<ImmutableConciseSet> conciseSets = Lists.newArrayList(lhs, rhs);

                                      ImmutableConciseSet intersects = ImmutableConciseSet.intersection(conciseSets);

                                      dimensionMap.put(dimVal, intersects.size());
                                  } else {
                                      ImmutableConciseSet lhs = selector.getConciseInvertedIndex(dimension, dimVal);
                                      dimensionMap.put(dimVal, lhs.size());
                                  }
                              }
                          }
                          retVal.add(new FrequencyCountResult(dimension, Lists.newArrayList(adapter.getInterval()), dimensionMap));
                      }
                  }
              }
          }
          return retVal;
      }
      */
  }
}
