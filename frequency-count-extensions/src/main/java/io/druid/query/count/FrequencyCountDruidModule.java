/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.guice.DruidBinders;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.query.groupby.GroupByQueryEngine;

import java.util.List;

/**
 */
public class FrequencyCountDruidModule implements DruidModule
{

    @Override
    public List<? extends Module> getJacksonModules()
    {
        return ImmutableList.of(
                new SimpleModule().registerSubtypes(new NamedType(FrequencyCountQuery.class, "frequencyCount"))
        );
    }


    @Override
    public void configure(Binder binder) {
        DruidBinders.queryToolChestBinder(binder)
                .addBinding(FrequencyCountQuery.class)
                .to(FrequencyCountQueryQueryToolChest.class);

        DruidBinders.queryRunnerFactoryBinder(binder)
                .addBinding(FrequencyCountQuery.class)
                .to(FrequencyCountQueryRunnerFactory.class);
        binder.bind(FrequencyCountQueryEngine.class).in(LazySingleton.class);
    }
}
