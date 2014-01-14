package io.druid.query.count;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.ISE;
import io.druid.query.QuerySegmentFinder;
import io.druid.query.SegmentDescriptor;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.segment.*;
import io.druid.segment.filter.Filters;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import io.druid.query.Result;

import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/30/13
 * Time: 11:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class FrequencyCountQueryEngine {

    private QuerySegmentFinder finder;


    @Inject
    public FrequencyCountQueryEngine (
            QuerySegmentFinder finder
    )
    {
        this.finder = finder;
    }

    public Sequence<Result<FrequencyCountResult>> process(final FrequencyCountQuery query, final Segment segment, final Filter filter)
    {
        final QueryableIndex index =  segment.asQueryableIndex();
        final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
        final List<String> dimensions = query.getDimensions();

        final List<Result<FrequencyCountResult>> retVal = Lists.newArrayList();

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
                            Segment otherSegment = joinSegment.get();
                            BitmapIndexSelector joinSelector = new ColumnSelectorBitmapIndexSelector(otherSegment.asQueryableIndex());
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
                //retVal.add(new Result(segment.getDataInterval().getStart(), new FrequencyCountResult("count", Lists.newArrayList(adapter.getInterval()), dimensionMap)));
                retVal.add(new Result(segment.getDataInterval().getStart(), new FrequencyCountResult("count", dimensionMap)));
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
                        retVal.add(new Result(segment.getDataInterval().getStart(),new FrequencyCountResult(dimension, dimensionMap)));
                    }
                }
            }
        }
          return new BaseSequence<Result<FrequencyCountResult>, Iterator<Result<FrequencyCountResult>>>(
              new BaseSequence.IteratorMaker<Result<FrequencyCountResult>, Iterator<Result<FrequencyCountResult>>>()
              {
                  @Override
                  public Iterator<Result<FrequencyCountResult>> make()
                  {
                    return
                       retVal.iterator();
                  }

                  @Override
                  public void cleanup(Iterator<Result<FrequencyCountResult>> toClean)
                  {

                  }
              }
          );
    }
}
