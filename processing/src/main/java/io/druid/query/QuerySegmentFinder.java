package io.druid.query;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/9/13
 * Time: 10:22 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.common.base.Optional;
import io.druid.segment.Segment;

/**
 */
public interface QuerySegmentFinder
{
    /**
     * Gets the Segment with the SegmentSpec
     *
     * @param dataSource  name of the data source
     * @param spec  the segment descriptor
     * @return a Queryable object that represents the interval
     */
    public Optional<Segment> findSegment(String dataSource, SegmentDescriptor spec);

}
