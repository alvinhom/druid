package io.druid.query.count;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.query.filter.DimFilter;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/11/13
 * Time: 4:47 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultJoinSpec.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "default", value = DefaultJoinSpec.class)
})
public interface JoinSpec
{
    public String getDataSource();
    public DimFilter getFilter();
    public byte[] getCacheKey();
}

