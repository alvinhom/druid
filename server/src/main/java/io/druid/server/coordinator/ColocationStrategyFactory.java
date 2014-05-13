package io.druid.server.coordinator;

import org.joda.time.DateTime;

/**
 * Created by alvhom on 1/17/14.
 */
public class ColocationStrategyFactory implements BalancerStrategyFactory
{

    @Override
    public BalancerStrategy createBalancerStrategy(DateTime referenceTimestamp)
    {
        return new ColocationStrategy(referenceTimestamp);
    }
}


