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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.spec.QuerySegmentSpec;


import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("frequencyCount")
public class FrequencyCountQuery extends BaseQuery<Result<FrequencyCountResult>>
{
  public static final String FREQUENCYCOUNT = "frequencyCount";

  private final DimFilter dimFilter;
  private final List<String> dimensions;
  private final SearchQuerySpec query;
  private final List<JoinSpec> join;

  @JsonCreator
  public FrequencyCountQuery(
          @JsonProperty("dataSource") DataSource dataSource,
          @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
          @JsonProperty("filter") DimFilter dimFilter,
          @JsonProperty("dimensions") List<String> dimensions,
          @JsonProperty("query") SearchQuerySpec query,
          @JsonProperty("join") List<JoinSpec> join,
          @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.dimensions = dimensions;
    this.query = query;
    this.join=join;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return FREQUENCYCOUNT;
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }


  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public SearchQuerySpec getQuery()
  {
    return query;
  }

  @JsonProperty
  public List<JoinSpec> getJoin()
  {
        return join;
  }

  public FrequencyCountQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
  {
    return new FrequencyCountQuery(
        getDataSource(),
        querySegmentSpec,
        dimFilter,
        dimensions,
        query,
        join,
        getContext()
    );
  }

  public FrequencyCountQuery withOverriddenContext(Map<String, Object> contextOverrides)
  {
    return new FrequencyCountQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        dimensions,
        query,
        join,
        computeOverridenContext(contextOverrides)
    );
  }

  @Override
  public BaseQuery<Result<FrequencyCountResult>> withDataSource(DataSource dataSource)
  {
        return new FrequencyCountQuery(
                dataSource,
                getQuerySegmentSpec(),
                dimFilter,
                dimensions,
                query,
                join,
                getContext()
        );
  }
  
  @Override
  public String toString()
  {
    return "FrequencyCountQuery{" +
           "dataSource='" + getDataSource() + '\'' +
           ", intervals=" + getQuerySegmentSpec() +
           ", filter=" + dimFilter +
           ", dimensions='" + dimensions + '\'' +
           ", query=" + getQuery() +
           ", join=" + join + '\'' +
           ", context=" + getContext() +
           '}';
  }

}
