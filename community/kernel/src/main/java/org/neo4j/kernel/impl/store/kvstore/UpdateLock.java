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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class UpdateLock extends ReentrantReadWriteLock
{
    UpdateLock()
    {
        super( true /* always fair */ );
    }

    @Override
    public String toString()
    {
        return new StringBuilder().append("AbstractKeyValyeStore-UpdateLock[owner = ").append(getOwner()).append(", is write locked = ").append(isWriteLocked()).append(", writer holds count = ").append(getWriteHoldCount())
				.append(", read holds count = ").append(getReadHoldCount()).append(", readers count = ").append(getReadLockCount()).append(", threads waiting for write lock = ").append(getQueuedWriterThreads()).append(", threads waiting for read lock = ")
				.append(getQueuedReaderThreads()).append("] ").append(super.toString()).toString();
    }
}
