/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.configuration;

import java.util.Map;

import org.junit.Test;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.listenAddress;

public class AdvertisedAddressSettingsTest
{
    private static Setting<ListenSocketAddress> listenAddressSetting = listenAddress( "listen_address", 1234 );
    private static Setting<AdvertisedSocketAddress> advertisedAddressSetting =
            advertisedAddress( "advertised_address", listenAddressSetting );

    @Test
    public void shouldParseExplicitSettingValueWhenProvided()
    {
        // given
        Map<String,String> config = stringMap(
                GraphDatabaseSettings.default_advertised_address.name(), "server1.example.com",
                advertisedAddressSetting.name(), "server1.internal:4000" );

        // when
        AdvertisedSocketAddress advertisedSocketAddress = advertisedAddressSetting.apply( config::get );

        // then
        assertEquals( "server1.internal", advertisedSocketAddress.getHostname() );
        assertEquals( 4000, advertisedSocketAddress.getPort() );
    }

    @Test
    public void shouldCombineDefaultHostnameWithPortFromListenAddressSettingWhenNoValueProvided()
    {
        // given
        Map<String,String> config = stringMap(
                GraphDatabaseSettings.default_advertised_address.name(), "server1.example.com" );

        // when
        AdvertisedSocketAddress advertisedSocketAddress = advertisedAddressSetting.apply( config::get );

        // then
        assertEquals( "server1.example.com", advertisedSocketAddress.getHostname() );
        assertEquals( 1234, advertisedSocketAddress.getPort() );
    }

    @Test
    public void shouldCombineDefaultHostnameWithExplicitPortWhenOnlyAPortProvided()
    {
        // given
        Map<String,String> config = stringMap(
                GraphDatabaseSettings.default_advertised_address.name(), "server1.example.com",
                advertisedAddressSetting.name(), ":4000" );

        // when
        AdvertisedSocketAddress advertisedSocketAddress = advertisedAddressSetting.apply( config::get );

        // then
        assertEquals( "server1.example.com", advertisedSocketAddress.getHostname() );
        assertEquals( 4000, advertisedSocketAddress.getPort() );
    }
}
