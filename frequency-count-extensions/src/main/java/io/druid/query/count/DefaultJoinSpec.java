package io.druid.query.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.query.filter.DimFilter;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/11/13
 * Time: 4:50 PM
 * To change this template use File | Settings | File Templates.
 */
@JsonTypeName("defaultJoinSpec")
public class DefaultJoinSpec implements JoinSpec{
    private static final byte CACHE_TYPE_ID = 0x0;
    private final String dataSource;
    private final DimFilter filter;

    @JsonCreator
    public DefaultJoinSpec(
            @JsonProperty("dataSource") String dataSource,
            @JsonProperty("filter") DimFilter filter
    )
    {
        Preconditions.checkNotNull(dataSource, "dataSource can't be null");
        this.dataSource = dataSource;
        this.filter = filter;
    }

    @JsonProperty
    public String getDataSource() {
        return dataSource;
    }

    @JsonProperty
    public DimFilter getFilter() {
        return filter;
    }

    @Override
    public byte[] getCacheKey()
    {
        byte[] dimensionBytes = dataSource.getBytes();
        byte[] subKey = filter.getCacheKey();

        return ByteBuffer.allocate(1 + dimensionBytes.length + subKey.length)
                .put(CACHE_TYPE_ID)
                .put(dimensionBytes)
                .put(subKey)
                .array();
    }

    @Override
    public String toString()
    {
        return "DefaultJoinSpec{" +
                "dataSource='" + dataSource + '\'' +
                ", filter='" + filter + '\'' +
                '}';
    }
}
