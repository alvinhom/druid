package io.druid.query.count;

import com.google.common.collect.Lists;

import io.druid.query.DataSource;
import io.druid.query.TableDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.search.search.InsensitiveContainsSearchQuerySpec;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/7/13
 * Time: 11:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class ModuleHelper {

    /**
     * A Builder for FrequencyCountQuery.
     * <p/>
     * Required: dataSource(), intervals(), and dimensions() must be called before build()
     * Optional: filters(), query(), and context() can be called before build()
     * <p/>
     * Usage example:
     * <pre><code>
     *   FrequencyCountQuery query = ModuleHelper.newFrequencyQueryBuilder()
     *                                        .dataSource("Example")
     *                                        .intervals("2012-01-01/2012-01-02")
     *                                        .dimensions("categories");
     *                                        .filters()
     *                                        .build();
     * </code></pre>
     *
     * @see io.druid.query.count.FrequencyCountQuery
     */
    public static class FrequencyCountQueryBuilder
    {
        private DataSource dataSource;
        private QuerySegmentSpec querySegmentSpec;
        private DimFilter dimFilter;
        private List<String> dimensions;
        private Map<String, Object> context;
        private SearchQuerySpec searchQuerySpec;
        private List<JoinSpec> join;
        private FrequencyCountQueryBuilder()
        {
            join = Lists.newArrayList();
            querySegmentSpec = null;
            dimFilter = null;
            searchQuerySpec = null;
            dimensions = Lists.newArrayList();
            context = null;
        }

        public FrequencyCountQuery build()
        {
            return new FrequencyCountQuery(
                    dataSource,
                    querySegmentSpec,
                    dimFilter,
                    dimensions,
                    searchQuerySpec,
                    join,
                    context
            );
        }

        public FrequencyCountQueryBuilder copy(FrequencyCountQuery query)
        {
            return new FrequencyCountQueryBuilder()
                    .dataSource(query.getDataSource())
                    .intervals(query.getIntervals())
                    .filters(query.getDimensionsFilter())
                    .query(query.getQuery())
                    .context(query.getContext());
        }

        public FrequencyCountQueryBuilder copy(FrequencyCountQueryBuilder builder)
        {
            return new FrequencyCountQueryBuilder()
                    .dataSource(builder.dataSource)
                    .intervals(builder.querySegmentSpec)
                    .filters(builder.dimFilter)
                    .context(builder.context);
        }

        public DataSource getDataSource()
        {
            return dataSource;
        }

        public List<JoinSpec> getJoin()
        {
            return join;
        }

        public FrequencyCountQueryBuilder addJoin(JoinSpec joinSpec) {
            join.add(joinSpec);
            return this;
        }

        public QuerySegmentSpec getQuerySegmentSpec()
        {
            return querySegmentSpec;
        }

        public DimFilter getDimFilter()
        {
            return dimFilter;
        }


        public Map<String, Object> getContext()
        {
            return context;
        }

        public FrequencyCountQueryBuilder dataSource(String ds)
        {
            dataSource =  new TableDataSource(ds);
            return this;
        }

        public FrequencyCountQueryBuilder dataSource(DataSource ds)
        {
            dataSource =  ds;
            return this;
        }

        public FrequencyCountQueryBuilder intervals(QuerySegmentSpec q)
        {
            querySegmentSpec = q;
            return this;
        }

        public FrequencyCountQueryBuilder intervals(String s)
        {
            querySegmentSpec = new LegacySegmentSpec(s);
            return this;
        }

        public FrequencyCountQueryBuilder intervals(List<Interval> l)
        {
            querySegmentSpec = new LegacySegmentSpec(l);
            return this;
        }

        public FrequencyCountQueryBuilder filters(String dimensionName, String value)
        {
            dimFilter = new SelectorDimFilter(dimensionName, value);
            return this;
        }

        public FrequencyCountQueryBuilder filters(String dimensionName, String value, String... values)
        {
            List<DimFilter> fields = Lists.<DimFilter>newArrayList(new SelectorDimFilter(dimensionName, value));
            for (String val : values) {
                fields.add(new SelectorDimFilter(dimensionName, val));
            }
            dimFilter = new OrDimFilter(fields);
            return this;
        }

        public FrequencyCountQueryBuilder filters(DimFilter f)
        {
            dimFilter = f;
            return this;
        }


        public FrequencyCountQueryBuilder query(SearchQuerySpec s)
        {
            searchQuerySpec = s;
            return this;
        }

        public FrequencyCountQueryBuilder query(String q)
        {
            searchQuerySpec = new InsensitiveContainsSearchQuerySpec(q);
            return this;
        }

        public FrequencyCountQueryBuilder query(Map<String, Object> q)
        {
            searchQuerySpec = new InsensitiveContainsSearchQuerySpec((String) q.get("value"));
            return this;
        }

        public FrequencyCountQueryBuilder addDimension(String column)
        {
            if (dimensions == null) {
                dimensions = Lists.newArrayList();
            }

            dimensions.add(column);
            return this;
        }

        public FrequencyCountQueryBuilder setDimensions(List<String> dimensions)
        {
            this.dimensions = Lists.newArrayList(dimensions);
            return this;
        }


        public FrequencyCountQueryBuilder context(Map<String, Object> c)
        {
            context = c;
            return this;
        }
    }

    public static FrequencyCountQueryBuilder newFrequencyCountQueryBuilder()
    {
        return new FrequencyCountQueryBuilder();
    }
}
