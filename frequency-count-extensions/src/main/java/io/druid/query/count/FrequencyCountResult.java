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
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

public class FrequencyCountResult
{
  private final String dimension;
  private final Map<String, Integer> dimensionCounts;

  @JsonCreator
  public FrequencyCountResult(
          @JsonProperty("dimension") String dimension,
          @JsonProperty("dimensionCounts") Map<String, Integer> dimensionCounts

  )
  {
    this.dimension = dimension;
    this.dimensionCounts = dimensionCounts;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }




    @JsonProperty
  public Map<String, Integer> getDimensionCounts()
  {

    return dimensionCounts;
  }


  public String toDetailedString()
  {
    return "FrequencyCountResult{" +
           "dimension='" + dimension + '\'' +
           ", dimensionCounts=" + dimensionCounts +
           '}';
  }

  @Override
  public String toString()
  {
    return "FrequencyCountResult{" +
           "dimension='" + dimension + '\'' +
            ", dimensionCounts=" + dimensionCounts +
           '}';
  }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FrequencyCountResult)) return false;

        FrequencyCountResult that = (FrequencyCountResult) o;

        if (!dimension.equals(that.dimension)) return false;
        if (!dimensionCounts.equals(that.dimensionCounts)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dimension.hashCode();
        result = 31 * result + dimensionCounts.hashCode();
        return result;
    }
}
