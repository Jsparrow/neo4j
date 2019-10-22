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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;
import org.mockito.InOrder;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.helpers.StubCursorFactory;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubRead;
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.kernel.impl.locking.Locks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.set;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.storageengine.api.lock.LockTracer.NONE;

public class TwoPhaseNodeForRelationshipLockingTest
{
    private static int type = 77;
	private final Transaction transaction = mock( Transaction.class );
	private final Locks.Client locks = mock( Locks.Client.class );
	private final long nodeId = 42L;

	@Test
    public void shouldLockNodesInOrderAndConsumeTheRelationships() throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( collector, locks,
                NONE );

        returnRelationships(
                transaction, false,
                new TestRelationshipChain( nodeId ).outgoing( 21L, 43L, 0 )
                        .incoming( 22L, 40L, type )
                        .outgoing( 23L, 41L, type )
                        .outgoing( 2L, 3L, type )
                        .incoming( 3L, 49L, type )
                        .outgoing( 50L, 41L, type ) );
        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, transaction, new StubNodeCursor( false ).withNode( nodeId ) );

        // then
        inOrder.verify( locks ).acquireExclusive( NONE, NODE, 3L, 40L, 41L, nodeId, 43L, 49L );
        assertEquals( set( 21L, 22L, 23L, 2L, 3L, 50L ), collector.set );
    }

	@Test
    public void shouldLockNodesInOrderAndConsumeTheRelationshipsAndRetryIfTheNewRelationshipsAreCreated()
            throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( collector, locks, NONE );

        TestRelationshipChain chain = new TestRelationshipChain( nodeId )
                .outgoing( 21L, 43L, type )
                .incoming( 22L, 40, type )
                .outgoing( 23L, 41L, type );
        returnRelationships( transaction, true, chain );

        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, transaction, new StubNodeCursor( false ).withNode( nodeId ) );

        // then
        inOrder.verify( locks ).acquireExclusive( NONE, NODE,  40L, 41L, nodeId );

        inOrder.verify( locks ).releaseExclusive( NODE, 40L, 41L, nodeId );

        inOrder.verify( locks ).acquireExclusive( NONE, NODE, 40L, 41L, nodeId, 43L );
        assertEquals( set( 21L, 22L, 23L ), collector.set );
    }

	@Test
    public void lockNodeWithoutRelationships() throws Exception
    {
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( collector, locks, NONE );
        returnRelationships( transaction, false, new TestRelationshipChain( 42 ) );

        locking.lockAllNodesAndConsumeRelationships( nodeId, transaction, new StubNodeCursor( false ).withNode( nodeId ) );

        verify( locks ).acquireExclusive( NONE, NODE, nodeId );
        verifyNoMoreInteractions( locks );
    }

	static void returnRelationships( Transaction transaction,
            final boolean skipFirst, final TestRelationshipChain relIds ) throws EntityNotFoundException
    {

        StubRead read = new StubRead();
        when( transaction.dataRead() ).thenReturn( read );
        StubCursorFactory cursorFactory = new StubCursorFactory( true );
        if ( skipFirst )
        {
            cursorFactory.withRelationshipTraversalCursors( new StubRelationshipCursor( relIds.tail() ),
                    new StubRelationshipCursor( relIds ) );
        }
        else
        {
            cursorFactory.withRelationshipTraversalCursors( new StubRelationshipCursor( relIds ) );
        }

        when( transaction.cursors() ).thenReturn( cursorFactory );
    }

	private static class Collector implements ThrowingConsumer<Long,KernelException>
    {
        public final Set<Long> set = new HashSet<>();

        @Override
        public void accept( Long input ) throws KernelException
        {
            assertNotNull( input );
            set.add( input );
        }
    }
}
