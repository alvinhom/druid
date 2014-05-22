package io.druid.server.coordinator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Created by alvhom on 1/17/14.
 */
public class ColocationStrategy implements BalancerStrategy {
    private static final EmittingLogger log = new EmittingLogger(ColocationStrategy.class);
    private final BiMap<String, String> collocateMap =  HashBiMap.create();
    private final BalancerStrategy next;

    public ColocationStrategy(DateTime referenceDateTime) {
        // populate map from config
        collocateMap.put("ibe240", "ap240");
        //this.next=null;
        this.next = new CostBalancerStrategyFactory(1).createBalancerStrategy(referenceDateTime);
    }

    @Override
    public ServerHolder findNewSegmentHomeReplicator(
            DataSegment proposalSegment, List<ServerHolder> serverHolders
    )
    {
        ServerHolder holder = chooseBestServer(proposalSegment, serverHolders, false);
        if (holder != null && !holder.isServingSegment(proposalSegment)) {
            return holder;
        }
        return next.findNewSegmentHomeReplicator(proposalSegment, serverHolders);
    }


    @Override
    public ServerHolder findNewSegmentHomeBalancer(
            DataSegment proposalSegment, List<ServerHolder> serverHolders
    )
    {
        ServerHolder holder = chooseBestServer(proposalSegment, serverHolders, true);
        if (holder != null && !holder.isServingSegment(proposalSegment)) {
            return holder;
        }
        return next.findNewSegmentHomeBalancer(proposalSegment, serverHolders);
    }

    /**
     * For colocation strategy, we want to colocate segments based on a definition
     * that 2 data source's aligned segments should be colocated in a single node.
     * This is useful for doing joins using locate data instead of doing remote calls.
     *
     * @param proposalSegment A DataSegment that we are proposing to move.
     * @param serverHolders   An iterable of ServerHolders for a particular tier.
     *
     * @return A ServerHolder with the new home for a segment.
     */
    private ServerHolder chooseBestServer(
            final DataSegment proposalSegment,
            final Iterable<ServerHolder> serverHolders,
            boolean includeCurrentServer
    )
    {
        String pairedDataSource = collocateMap.get(proposalSegment.getDataSource());
        if (pairedDataSource == null) {
            pairedDataSource = collocateMap.inverse().get(proposalSegment.getDataSource());
        }
        final long proposalSegmentSize = proposalSegment.getSize();
        if (pairedDataSource != null) {
            // see if the other segment is already being served, if so, use that server, otherwise return null
            for (ServerHolder server : serverHolders) {
                if (includeCurrentServer || !server.isServingSegment(proposalSegment)) {
                    /** Don't calculate cost if the server doesn't have enough space or is loading the segment */
                    if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
                        continue;
                    }
                    for (DataSegment segment : server.getServer().getSegments().values()) {
                        if (segment.getDataSource().equals(pairedDataSource)) {
                            // check to see if it is the same partition
                            if (proposalSegment.getShardSpec().getPartitionNum() == segment.getShardSpec().getPartitionNum()) {
                                return server;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    public BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders)
    {
        return next.pickSegmentToMove(serverHolders);
    }

    @Override
    public List<BalancerSegmentHolder> pickSegmentsToMove(final List<ServerHolder> serverHolders)
    {
        BalancerSegmentHolder segmentToMove = next.pickSegmentToMove(serverHolders);
        List<BalancerSegmentHolder> segmentList = Lists.newArrayList(segmentToMove);
        if (segmentToMove != null) {
            // find paired segments
            DataSegment mySegment = segmentToMove.getSegment();
            String pairedDataSource = collocateMap.get(mySegment.getDataSource());
            if (pairedDataSource == null) {
                pairedDataSource = collocateMap.inverse().get(mySegment.getDataSource());
            }
            if (pairedDataSource != null) {
                // see if the other segment is already being served, if so, use that server, otherwise return null
                for (ServerHolder server : serverHolders) {
                    for (DataSegment segment : server.getServer().getSegments().values()) {
                        if (segment.getDataSource().equals(pairedDataSource)) {
                            // check to see if it is the same partition
                            if (mySegment.getShardSpec().getPartitionNum() == segment.getShardSpec().getPartitionNum()) {
                                segmentList.add(new BalancerSegmentHolder(server.getServer(), segment));
                            }
                        }
                    }
                }
            }
        }
        return segmentList;
    }

    @Override
    public void emitStats(
            String tier,
            CoordinatorStats stats, List<ServerHolder> serverHolderList
    )
    {
    }
}
