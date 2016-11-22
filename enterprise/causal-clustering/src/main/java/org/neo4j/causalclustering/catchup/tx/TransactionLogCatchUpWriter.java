/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.tx;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private final Lifespan lifespan = new Lifespan();
    private final File neoStoreFile;
    private final PageCache pageCache;
    private final Log log;
    private final TransactionLogWriter writer;

    private AtomicLong firstTxId = new AtomicLong( BASE_TX_ID );
    private AtomicLong lastTxId = new AtomicLong( BASE_TX_ID );
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fs;
    private final File storeDir;

    TransactionLogCatchUpWriter( File storeDir, FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider )
            throws IOException
    {
        this.neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
        this.fs = fs;
        this.logFiles = new PhysicalLogFiles( storeDir, fs );
        ReadOnlyLogVersionRepository logVersionRepository = new ReadOnlyLogVersionRepository( pageCache, storeDir );
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDir );
        LogFile logFile = lifespan.add( new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                () -> readOnlyTransactionIdStore.getLastCommittedTransactionId() - 1, logVersionRepository,
                new Monitors().newMonitor( PhysicalLogFile.Monitor.class ), new LogHeaderCache( 10 ) ) );
        this.writer = new TransactionLogWriter( new LogEntryWriter( logFile.getWriter() ) );
        this.storeDir = storeDir;
    }

    @Override
    public synchronized void onTxReceived( TxPullResponse txPullResponse )
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        try
        {
            long receivedTxId = tx.getCommitEntry().getTxId();

            lastTxId.set( receivedTxId );
            firstTxId.compareAndSet( BASE_TX_ID, receivedTxId );

            writer.append( tx.getTransactionRepresentation(), this.lastTxId.get() );
        }
        catch ( IOException e )
        {
            log.error( "Failed when appending to transaction log", e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        long logVersion = logFiles.getHighestLogVersion();
        writer.checkPoint( new LogPosition( logVersion, LOG_HEADER_SIZE ) );
        lifespan.close();

        File currentLogFile = logFiles.getLogFileForVersion( logVersion );
        writeLogHeader( fs, currentLogFile, logVersion, max( BASE_TX_ID, firstTxId.get() - 1 ) );

        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        MetaDataStore.setRecord(
                pageCache,
                neoStore,
                MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                LOG_HEADER_SIZE );

        if ( lastTxId.get() != -1 )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, lastTxId.get() );
        }
    }
}
