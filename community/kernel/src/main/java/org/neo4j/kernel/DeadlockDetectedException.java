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
package org.neo4j.kernel;

import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that a deadlock between two or more transactions has been detected.
 */
public class DeadlockDetectedException extends TransientTransactionFailureException implements Status.HasStatus
{
    public DeadlockDetectedException( String message )
    {
        super( message, null );
    }

    public DeadlockDetectedException( String message, Throwable cause )
    {
        super( new StringBuilder().append("Don't panic.\n").append("\n").append("A deadlock scenario has been detected and avoided. This means that two or more transactions, which were ").append("holding locks, were wanting to await locks held by one another, which would have resulted in a deadlock ").append("between these transactions. This exception was thrown instead of ending up in that deadlock.\n").append("\n").append("See the deadlock section in the Neo4j Java developer reference for how to avoid this: ").append("https://neo4j.com/docs/java-reference/current/#transactions-deadlocks\n")
				.append("\n").append("Details: '").append(message).append("'.").toString(), cause );
    }

    @Override
    public Status status()
    {
        return Status.Transaction.DeadlockDetected;
    }
}
