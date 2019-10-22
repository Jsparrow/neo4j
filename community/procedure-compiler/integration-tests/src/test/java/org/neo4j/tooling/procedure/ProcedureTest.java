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
package org.neo4j.tooling.procedure;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.tooling.procedure.procedures.valid.Procedures;

import static org.assertj.core.api.Assertions.assertThat;


public class ProcedureTest
{
    private static final Class<?> PROCEDURES_CLASS = Procedures.class;

    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public Neo4jRule graphDb = new Neo4jRule()
            .dumpLogsOnFailure( () -> System.out ) // Late-bind to System.out to work better with SuppressOutput rule.
            .withProcedure( PROCEDURES_CLASS );
    private String procedureNamespace = PROCEDURES_CLASS.getPackage().getName();

    @Test
    public void calls_simplistic_procedure()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            StatementResult result = session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".theAnswer()").toString() );

            assertThat( result.single().get( "value" ).asLong() ).isEqualTo( 42L );
        }
    }

    @Test
    public void calls_procedures_with_simple_input_type_returning_void()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput00()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput01('string')").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput02(42)").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput03(42)").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput04(4.2)").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput05(true)").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput06(false)").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput07({foo:'bar'})").toString() );
            session.run( new StringBuilder().append("MATCH (n)            CALL ").append(procedureNamespace).append(".simpleInput08(n) RETURN n").toString() );
            session.run( new StringBuilder().append("MATCH p=(()-[r]->()) CALL ").append(procedureNamespace).append(".simpleInput09(p) RETURN p").toString() );
            session.run( new StringBuilder().append("MATCH ()-[r]->()     CALL ").append(procedureNamespace).append(".simpleInput10(r) RETURN r").toString() );
        }
    }

    @Test
    public void calls_procedures_with_different_modes_returning_void()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".performsWrites()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".defaultMode()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".readMode()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".writeMode()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".schemaMode()").toString() );
            session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".dbmsMode()").toString() );
        }
    }

    @Test
    public void calls_procedures_with_simple_input_type_returning_record_with_primitive_fields()
    {
        try ( Driver driver = GraphDatabase.driver( graphDb.boltURI(), configuration() );
                Session session = driver.session() )
        {

            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput11('string') YIELD field04 AS p RETURN p").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput12(42)").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput13(42)").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput14(4.2)").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput15(true)").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput16(false)").toString() ).single() ).isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput17({foo:'bar'})").toString() ).single() )
                    .isNotNull();
            assertThat( session.run( new StringBuilder().append("CALL ").append(procedureNamespace).append(".simpleInput21()").toString() ).single() ).isNotNull();
        }

    }

    private Config configuration()
    {
        return Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE )
                .withConnectionTimeout( 10, TimeUnit.SECONDS )
                .toConfig();
    }

}
