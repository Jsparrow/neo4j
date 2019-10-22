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
package org.neo4j.values.utils;

import java.util.function.Supplier;

import org.neo4j.values.AnyValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertEquals( formatMessage( "should be equivalent to", a, b ), a, b );
        assertEquals( formatMessage( "should be equivalent to", b, a ), b, a );
        assertTrue( formatMessage( "should be equal to", a, b ),
                a.ternaryEquals( b ) );
        assertTrue( formatMessage( "should be equal to", b, a ),
                b.ternaryEquals( a ) );
        assertEquals( formatMessage( "should have same hashcode as", a, b ), a.hashCode(), b.hashCode() );
    }

    private static String formatMessage( String should, AnyValue a, AnyValue b )
    {
        return String.format( "%s(%s) %s %s(%s)", a.getClass().getSimpleName(), a.toString(), should, b.getClass().getSimpleName(), b.toString() );
    }

    public static void assertEqualValues( AnyValue a, AnyValue b )
    {
        assertEquals( new StringBuilder().append(a).append(" should be equivalent to ").append(b).toString(), a, b );
        assertEquals( new StringBuilder().append(a).append(" should be equivalent to ").append(b).toString(), b, a );
        assertTrue( new StringBuilder().append(a).append(" should be equal to ").append(b).toString(), a.ternaryEquals( b ) );
        assertTrue( new StringBuilder().append(a).append(" should be equal to ").append(b).toString(), b.ternaryEquals( a ) );
    }

    public static void assertNotEqual( AnyValue a, AnyValue b )
    {
        assertNotEquals( new StringBuilder().append(a).append(" should not be equivalent to ").append(b).toString(), a, b );
        assertNotEquals( new StringBuilder().append(b).append(" should not be equivalent to ").append(a).toString(), b, a );
        assertFalse( new StringBuilder().append(a).append(" should not equal ").append(b).toString(), a.ternaryEquals( b ) );
        assertFalse( new StringBuilder().append(b).append(" should not equal ").append(a).toString(), b.ternaryEquals( a ) );
    }

    public static void assertIncomparable( AnyValue a, AnyValue b )
    {
        assertNotEquals( new StringBuilder().append(a).append(" should not be equivalent to ").append(b).toString(), a, b );
        assertNotEquals( new StringBuilder().append(b).append(" should not be equivalent to ").append(a).toString(), b, a );
        assertNull( new StringBuilder().append(a).append(" should be incomparable to ").append(b).toString(), a.ternaryEquals( b ) );
        assertNull( new StringBuilder().append(b).append(" should be incomparable to ").append(a).toString(), b.ternaryEquals( a ) );
    }

    public static <X extends Exception, T> X assertThrows( Class<X> exception, Supplier<T> thunk )
    {
        T value;
        try
        {
            value = thunk.get();
        }
        catch ( Exception e )
        {
            if ( exception.isInstance( e ) )
            {
                return exception.cast( e );
            }
            else
            {
                throw new AssertionError( "Expected " + exception.getName(), e );
            }
        }
        throw new AssertionError( new StringBuilder().append("Expected ").append(exception.getName()).append(" but returned: ").append(value).toString() );
    }
}
