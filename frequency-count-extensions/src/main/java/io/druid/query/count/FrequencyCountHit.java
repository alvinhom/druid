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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
public class FrequencyCountHit implements Comparable<FrequencyCountHit>
{
  private final String dimension;
  private final String value;
  private final Integer count;

  @JsonCreator
  public FrequencyCountHit(
          @JsonProperty("dimension") String dimension,
          @JsonProperty("value") String value,
          @JsonProperty("count") Integer count
  )
  {
    this.dimension = checkNotNull(dimension);
    this.value = checkNotNull(value);
    this.count = count;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public int getCount()
  {
    return count;
  }

  @Override
  public int compareTo(FrequencyCountHit o)
  {
    int retVal = dimension.compareTo(o.dimension);
    if (retVal == 0) {
      retVal = value.compareTo(o.value);
    }
    return retVal;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FrequencyCountHit searchHit = (FrequencyCountHit) o;

    if (dimension != null ? !dimension.equals(searchHit.dimension) : searchHit.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(searchHit.value) : searchHit.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "FrequencyCountHit{" +
           "dimension='" + dimension + '\'' +
           ", value='" + value + '\'' +
            ", count='" + count + '\'' +
           '}';
  }
}
