package io.druid.query;

import com.google.common.base.Optional;
import io.druid.segment.Segment;

/**
 * Created with IntelliJ IDEA.
 * User: alvhom
 * Date: 12/30/13
 * Time: 5:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class NoopQuerySegmentFinder implements QuerySegmentFinder {

    public Optional<Segment> findSegment(String dataSource, SegmentDescriptor spec) {
        return Optional.absent();
    }

}
