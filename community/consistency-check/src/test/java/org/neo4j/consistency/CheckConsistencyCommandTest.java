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
package org.neo4j.consistency;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( TestDirectoryExtension.class )
class CheckConsistencyCommandTest
{
    @Inject
    private TestDirectory testDir;

    @Test
    void runsConsistencyChecker() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void enablesVerbosity() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void failsWhenInconsistenciesAreFound() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        File databasesFolder = getDatabasesFolder( homeDir );
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );
        DatabaseLayout databaseLayout = DatabaseLayout.of( databasesFolder, "mydb" );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( databaseLayout ), any( Config.class ), any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( true ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.failure( new File( "/the/report/path" ) ) );

        CommandFailed commandFailed =
                assertThrows( CommandFailed.class, () -> checkConsistencyCommand.execute( new String[]{"--database=mydb", "--verbose"} ) );
        assertThat( commandFailed.getMessage(), containsString( new File( "/the/report/path" ).toString() ) );
    }

    @Test
    void shouldWriteReportFileToCurrentDirectoryByDefault()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        eq( new File( "." ).getCanonicalFile() ), any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldWriteReportFileToSpecifiedDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage

    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=some-dir-or-other"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "some-dir-or-other" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldCanonicalizeReportDirectory()
            throws IOException, ConsistencyCheckIncompleteException, CommandFailed, IncorrectUsage
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--report-dir=" + Paths.get( "..", "bar" )} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(),
                        anyBoolean(), eq( new File( "../bar" ).getCanonicalFile() ),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void passesOnCheckParameters() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--database=mydb", "--check-graph=false",
                "--check-indexes=false", "--check-index-structure=false", "--check-label-scan-store=false", "--check-property-owners=true"} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( any(), any(), any(), any(), any(), anyBoolean(),
                        any(), eq( new ConsistencyFlags( false, false, false, false, true ) ) );
    }

    @Test
    void databaseAndBackupAreMutuallyExclusive() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService.runFullConsistencyCheck( any(), any(), any(), any(),
                any(), anyBoolean(), any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        IncorrectUsage incorrectUsage =
                assertThrows( IncorrectUsage.class, () -> checkConsistencyCommand.execute( new String[]{"--database=foo", "--backup=bar"} ) );
        assertEquals( "Only one of '--database' and '--backup' can be specified.", incorrectUsage.getMessage() );
    }

    @Test
    void backupNeedsToBePath()
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        File backupPath = new File( homeDir.toFile(), "dir/does/not/exist" );

        CommandFailed commandFailed = assertThrows( CommandFailed.class, () -> checkConsistencyCommand.execute( new String[]{"--backup=" + backupPath} ) );
        assertEquals( "Specified backup should be a directory: " + backupPath, commandFailed.getMessage() );
    }

    @Test
    void canRunOnBackup() throws Exception
    {
        ConsistencyCheckService consistencyCheckService = mock( ConsistencyCheckService.class );

        DatabaseLayout backupLayout = testDir.databaseLayout( "backup" );
        Path homeDir = testDir.directory( "home" ).toPath();
        CheckConsistencyCommand checkConsistencyCommand =
                new CheckConsistencyCommand( homeDir, testDir.directory( "conf" ).toPath(), consistencyCheckService );

        when( consistencyCheckService
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) ) )
                .thenReturn( ConsistencyCheckService.Result.success( null ) );

        checkConsistencyCommand.execute( new String[]{"--backup=" + backupLayout.databaseDirectory()} );

        verify( consistencyCheckService )
                .runFullConsistencyCheck( eq( backupLayout ), any( Config.class ),
                        any( ProgressMonitorFactory.class ),
                        any( LogProvider.class ), any( FileSystemAbstraction.class ), eq( false ), any(),
                        any( ConsistencyFlags.class ) );
    }

    @Test
    void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new CheckConsistencyCommandProvider(), ps::println );

            assertEquals( String.format( new StringBuilder().append("usage: neo4j-admin check-consistency [--database=<name>]%n").append("                                     [--backup=</path/to/backup>]%n").append("                                     [--verbose[=<true|false>]]%n").append("                                     [--report-dir=<directory>]%n").append("                                     [--additional-config=<config-file-path>]%n").append("                                     [--check-graph[=<true|false>]]%n").append("                                     [--check-indexes[=<true|false>]]%n").append("                                     [--check-index-structure[=<true|false>]]%n")
					.append("                                     [--check-label-scan-store[=<true|false>]]%n").append("                                     [--check-property-owners[=<true|false>]]%n").append("%n").append("environment variables:%n").append("    NEO4J_CONF    Path to directory which contains neo4j.conf.%n").append("    NEO4J_DEBUG   Set to anything to enable debug output.%n").append("    NEO4J_HOME    Neo4j home directory.%n").append("    HEAP_SIZE     Set JVM maximum heap size during command execution.%n").append("                  Takes a number and a unit, for example 512m.%n")
					.append("%n").append("This command allows for checking the consistency of a database or a backup%n").append("thereof. It cannot be used with a database which is currently in use.%n").append("%n").append("All checks except 'check-graph' can be quite expensive so it may be useful to%n").append("turn them off for very large databases. Increasing the heap size can also be a%n").append("good idea. See 'neo4j-admin help' for details.%n").append("%n").append("options:%n")
					.append("  --database=<name>                        Name of database. [default:").append(GraphDatabaseSettings.DEFAULT_DATABASE_NAME).append("]%n").append("  --backup=</path/to/backup>               Path to backup to check consistency%n").append("                                           of. Cannot be used together with%n").append("                                           --database. [default:]%n").append("  --verbose=<true|false>                   Enable verbose output.%n")
					.append("                                           [default:false]%n").append("  --report-dir=<directory>                 Directory to write report file in.%n").append("                                           [default:.]%n").append("  --additional-config=<config-file-path>   Configuration file to supply%n").append("                                           additional configuration in. This%n").append("                                           argument is DEPRECATED. [default:]%n").append("  --check-graph=<true|false>               Perform checks between nodes,%n").append("                                           relationships, properties, types and%n").append("                                           tokens. [default:true]%n")
					.append("  --check-indexes=<true|false>             Perform checks on indexes.%n").append("                                           [default:true]%n").append("  --check-index-structure=<true|false>     Perform structure checks on indexes.%n").append("                                           [default:false]%n").append("  --check-label-scan-store=<true|false>    Perform checks on the label scan%n").append("                                           store. [default:true]%n").append("  --check-property-owners=<true|false>     Perform additional checks on property%n").append("                                           ownership. This check is *very*%n").append("                                           expensive in time and memory.%n")
					.append("                                           [default:false]%n").toString() ),
                    baos.toString() );
        }
    }

    private static File getDatabasesFolder( Path homeDir )
    {
        return Config.defaults( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath().toString() ).get( GraphDatabaseSettings.databases_root_path );
    }
}
