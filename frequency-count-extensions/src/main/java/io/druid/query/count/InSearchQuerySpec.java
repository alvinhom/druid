package io.druid.query.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.query.search.search.SearchQuerySpec;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/9/13
 * Time: 11:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class InSearchQuerySpec implements SearchQuerySpec
{
    private static final byte CACHE_TYPE_ID = 0x2;

    private final List<String> values;

    @JsonCreator
    public InSearchQuerySpec(
            @JsonProperty("values") List<String> values
    )
    {
        this.values = Lists.transform(
                values,
                new Function<String, String>()
                {
                    @Override
                    public String apply(String s)
                    {
                        return s.toLowerCase();
                    }
                }
        );
    }

    @JsonProperty
    public List<String> getValues()
    {
        return values;
    }

    @Override
    public boolean accept(String dimVal)
    {
        for (String value : values) {
            if (dimVal.toLowerCase().contains(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] getCacheKey()
    {
        final byte[][] valuesBytes = new byte[values.size()][];
        int valuesBytesSize = 0;
        int index = 0;
        for (String value : values) {
            valuesBytes[index] = value.getBytes();
            valuesBytesSize += valuesBytes[index].length;
            ++index;
        }

        final ByteBuffer queryCacheKey = ByteBuffer.allocate(1 + valuesBytesSize)
                .put(CACHE_TYPE_ID);

        for (byte[] bytes : valuesBytes) {
            queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
    }

    @Override
    public String toString()
    {
        return "InSearchQuerySpec{" +
                "values=" + values +
                "}";
    }
}
