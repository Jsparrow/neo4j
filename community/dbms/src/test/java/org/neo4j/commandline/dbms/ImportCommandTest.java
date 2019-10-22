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
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.NullOutsideWorld;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ImportCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void defaultsToCsvWhenModeNotSpecified() throws Exception
    {
        File homeDir = testDir.directory( "home" );
        ImporterFactory mockImporterFactory = mock( ImporterFactory.class );
        Importer importer = mock( Importer.class );
        when( mockImporterFactory
                .getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ), any( OutsideWorld.class ) ) )
                .thenReturn( importer );

        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ) )
        {
            ImportCommand importCommand =
                    new ImportCommand( homeDir.toPath(), testDir.directory( "conf" ).toPath(), outsideWorld,
                            mockImporterFactory );

            String[] arguments = {"--database=foo", "--from=bar"};

            importCommand.execute( arguments );

            verify( mockImporterFactory ).getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ),
                    any( OutsideWorld.class ) );
        }
    }

    @Test
    void acceptsNodeMetadata() throws Exception
    {
        File homeDir = testDir.directory( "home" );
        ImporterFactory mockImporterFactory = mock( ImporterFactory.class );
        Importer importer = mock( Importer.class );
        when( mockImporterFactory
                .getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ), any( OutsideWorld.class ) ) )
                .thenReturn( importer );

        ImportCommand importCommand =
                new ImportCommand( homeDir.toPath(), testDir.directory( "conf" ).toPath(),
                        new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ), mockImporterFactory );

        String[] arguments = {"--database=foo", "--from=bar", "--nodes:PERSON:FRIEND=mock.csv"};

        importCommand.execute( arguments );

        verify( mockImporterFactory )
                .getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ), any( OutsideWorld.class ) );
    }

    @Test
    void acceptsRelationshipsMetadata() throws Exception
    {
        File homeDir = testDir.directory( "home" );
        ImporterFactory mockImporterFactory = mock( ImporterFactory.class );
        Importer importer = mock( Importer.class );
        when( mockImporterFactory
                .getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ), any( OutsideWorld.class ) ) )
                .thenReturn( importer );

        ImportCommand importCommand =
                new ImportCommand( homeDir.toPath(), testDir.directory( "conf" ).toPath(),
                        new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ), mockImporterFactory );

        String[] arguments = {"--database=foo", "--from=bar", "--relationships:LIKES:HATES=mock.csv"};

        importCommand.execute( arguments );

        verify( mockImporterFactory )
                .getImporterForMode( eq( "csv" ), any( Args.class ), any( Config.class ), any( OutsideWorld.class ) );
    }

    @Test
    void requiresDatabaseArgument()
    {
        try ( NullOutsideWorld outsideWorld = new NullOutsideWorld() )
        {
            ImportCommand importCommand =
                    new ImportCommand( testDir.directory( "home" ).toPath(), testDir.directory( "conf" ).toPath(),
                            outsideWorld );

            String[] arguments = {"--mode=database", "--from=bar"};
            IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> importCommand.execute( arguments ) );
            assertThat( incorrectUsage.getMessage(), containsString( "database" ) );
        }
    }

    @Test
    void failIfInvalidModeSpecified()
    {
        try ( NullOutsideWorld outsideWorld = new NullOutsideWorld() )
        {
            ImportCommand importCommand =
                    new ImportCommand( testDir.directory( "home" ).toPath(), testDir.directory( "conf" ).toPath(),
                            outsideWorld );

            String[] arguments = {"--mode=foo", "--database=bar", "--from=baz"};
            IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> importCommand.execute( arguments ) );
            assertThat( incorrectUsage.getMessage(), containsString( "foo" ) );
        }
    }

    @Test
    void letImporterDecideAboutDatabaseExistence() throws Exception
    {
        File report = testDir.file( "report" );
        Path homeDir = testDir.directory( "home" ).toPath();
        PrintStream nullOutput = new PrintStream( NULL_OUTPUT_STREAM );
        OutsideWorld outsideWorld = new RealOutsideWorld( nullOutput, nullOutput, new ByteArrayInputStream( new byte[0] ) );
        Path confPath = testDir.directory( "conf" ).toPath();
        ImportCommand importCommand = new ImportCommand( homeDir, confPath, outsideWorld );
        File nodesFile = createTextFile( "nodes.csv", ":ID", "1", "2" );
        String[] arguments = {"--mode=csv", "--database=existing.db", "--nodes=" + nodesFile.getAbsolutePath(), "--report-file=" + report.getAbsolutePath()};

        // First run an import so that a database gets created
        importCommand.execute( arguments );

        // When
        ImporterFactory importerFactory = mock( ImporterFactory.class );
        Importer importer = mock( Importer.class );
        when( importerFactory.getImporterForMode( any(), any(), any(), any() ) ).thenReturn( importer );
        new ImportCommand( homeDir, confPath, outsideWorld, importerFactory ).execute( arguments );

        // Then no exception about database existence should be thrown
    }

    @Test
    void shouldUseArgumentsFoundInside_f_Argument() throws FileNotFoundException, CommandFailed, IncorrectUsage
    {
        // given
        File report = testDir.file( "report" );
        ImportCommand importCommand =
                new ImportCommand( testDir.directory( "home" ).toPath(), testDir.directory( "conf" ).toPath(),
                        new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ) );
        File nodesFile = createTextFile( "nodes.csv", ":ID", "1", "2" );
        String pathWithEscapedSpaces = escapeSpaces( nodesFile.getAbsolutePath() );
        String reportEscapedPath = escapeSpaces( report.getAbsolutePath() );
        File argFile = createTextFile( "args.txt", "--database=foo", "--nodes=" + pathWithEscapedSpaces, "--report-file=" + reportEscapedPath );
        String[] arguments = {"-f", argFile.getAbsolutePath()};

        // when
        importCommand.execute( arguments );

        // then
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "IMPORT DONE" ) );
        assertTrue( suppressOutput.getErrorVoice().containsMessage( nodesFile.getAbsolutePath() ) );
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "2 nodes" ) );
    }

    @Test
    void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new ImportCommandProvider(), ps::println );

            assertEquals( String.format( new StringBuilder().append("usage: neo4j-admin import [--mode=csv] [--database=<name>]%n").append("                          [--additional-config=<config-file-path>]%n").append("                          [--report-file=<filename>]%n").append("                          [--nodes[:Label1:Label2]=<\"file1,file2,...\">]%n").append("                          [--relationships[:RELATIONSHIP_TYPE]=<\"file1,file2,...\">]%n").append("                          [--id-type=<STRING|INTEGER|ACTUAL>]%n").append("                          [--input-encoding=<character-set>]%n").append("                          [--ignore-extra-columns[=<true|false>]]%n")
					.append("                          [--ignore-duplicate-nodes[=<true|false>]]%n").append("                          [--ignore-missing-nodes[=<true|false>]]%n").append("                          [--multiline-fields[=<true|false>]]%n").append("                          [--delimiter=<delimiter-character>]%n").append("                          [--array-delimiter=<array-delimiter-character>]%n").append("                          [--quote=<quotation-character>]%n").append("                          [--max-memory=<max-memory-that-importer-can-use>]%n").append("                          [--f=<File containing all arguments to this import>]%n").append("                          [--high-io=<true/false>]%n")
					.append("usage: neo4j-admin import --mode=database [--database=<name>]%n").append("                          [--additional-config=<config-file-path>]%n").append("                          [--from=<source-directory>]%n").append("%n").append("environment variables:%n").append("    NEO4J_CONF    Path to directory which contains neo4j.conf.%n").append("    NEO4J_DEBUG   Set to anything to enable debug output.%n").append("    NEO4J_HOME    Neo4j home directory.%n").append("    HEAP_SIZE     Set JVM maximum heap size during command execution.%n")
					.append("                  Takes a number and a unit, for example 512m.%n").append("%n").append("Import a collection of CSV files with --mode=csv (default), or a database from a%n").append("pre-3.0 installation with --mode=database.%n").append("%n").append("options:%n").append("  --database=<name>%n").append("      Name of database. [default:")
					.append(GraphDatabaseSettings.DEFAULT_DATABASE_NAME).append("]%n").append("  --additional-config=<config-file-path>%n").append("      Configuration file to supply additional configuration in. [default:]%n").append("  --mode=<database|csv>%n").append("      Import a collection of CSV files or a pre-3.0 installation. [default:csv]%n").append("  --from=<source-directory>%n")
					.append("      The location of the pre-3.0 database (e.g. <neo4j-root>/data/graph.db).%n").append("      [default:]%n").append("  --report-file=<filename>%n").append("      File in which to store the report of the csv-import.%n").append("      [default:import.report]%n").append("  --nodes[:Label1:Label2]=<\"file1,file2,...\">%n").append("      Node CSV header and data. Multiple files will be logically seen as one big%n").append("      file from the perspective of the importer. The first line must contain the%n").append("      header. Multiple data sources like these can be specified in one import,%n")
					.append("      where each data source has its own header. Note that file groups must be%n").append("      enclosed in quotation marks. [default:]%n").append("  --relationships[:RELATIONSHIP_TYPE]=<\"file1,file2,...\">%n").append("      Relationship CSV header and data. Multiple files will be logically seen as%n").append("      one big file from the perspective of the importer. The first line must%n").append("      contain the header. Multiple data sources like these can be specified in%n").append("      one import, where each data source has its own header. Note that file%n").append("      groups must be enclosed in quotation marks. [default:]%n").append("  --id-type=<STRING|INTEGER|ACTUAL>%n")
					.append("      Each node must provide a unique id. This is used to find the correct nodes%n").append("      when creating relationships. Possible values are:%n").append("        STRING: arbitrary strings for identifying nodes,%n").append("        INTEGER: arbitrary integer values for identifying nodes,%n").append("        ACTUAL: (advanced) actual node ids.%n").append("      For more information on id handling, please see the Neo4j Manual:%n").append("      https://neo4j.com/docs/operations-manual/current/tools/import/%n").append("      [default:STRING]%n").append("  --input-encoding=<character-set>%n")
					.append("      Character set that input data is encoded in. [default:UTF-8]%n").append("  --ignore-extra-columns=<true|false>%n").append("      If un-specified columns should be ignored during the import.%n").append("      [default:false]%n").append("  --ignore-duplicate-nodes=<true|false>%n").append("      If duplicate nodes should be ignored during the import. [default:false]%n").append("  --ignore-missing-nodes=<true|false>%n").append("      If relationships referring to missing nodes should be ignored during the%n").append("      import. [default:false]%n")
					.append("  --multiline-fields=<true|false>%n").append("      Whether or not fields from input source can span multiple lines, i.e.%n").append("      contain newline characters. [default:false]%n").append("  --delimiter=<delimiter-character>%n").append("      Delimiter character between values in CSV data. [default:,]%n").append("  --array-delimiter=<array-delimiter-character>%n").append("      Delimiter character between array elements within a value in CSV data.%n").append("      [default:;]%n").append("  --quote=<quotation-character>%n")
					.append("      Character to treat as quotation character for values in CSV data. Quotes%n").append("      can be escaped as per RFC 4180 by doubling them, for example \"\" would be%n").append("      interpreted as a literal \". You cannot escape using \\. [default:\"]%n").append("  --max-memory=<max-memory-that-importer-can-use>%n").append("      Maximum memory that neo4j-admin can use for various data structures and%n").append("      caching to improve performance. Values can be plain numbers, like 10000000%n").append("      or e.g. 20G for 20 gigabyte, or even e.g. 70%%. [default:90%%]%n").append("  --f=<File containing all arguments to this import>%n").append("      File containing all arguments, used as an alternative to supplying all%n")
					.append("      arguments on the command line directly.Each argument can be on a separate%n").append("      line or multiple arguments per line separated by space.Arguments%n").append("      containing spaces needs to be quoted.Supplying other arguments in addition%n").append("      to this file argument is not supported. [default:]%n").append("  --high-io=<true/false>%n").append("      Ignore environment-based heuristics, and assume that the target storage%n").append("      subsystem can support parallel IO with high throughput. [default:null]%n").toString() ),
                    baos.toString() );
        }
    }

    private static void putStoreInDirectory( Path databaseDirectory ) throws IOException
    {
        Files.createDirectories( databaseDirectory );
        Path storeFile = DatabaseLayout.of( databaseDirectory.toFile() ).metadataStore().toPath();
        Files.createFile( storeFile );
    }

    private File createTextFile( String name, String... lines ) throws FileNotFoundException
    {
        File file = testDir.file( name );
        try ( PrintStream out = new PrintStream( file ) )
        {
            for ( String line : lines )
            {
                out.println( line );
            }
        }
        return file;
    }

    private static String escapeSpaces( String pathForFile )
    {
        return pathForFile.replaceAll( " ", "\\\\ " );
    }
}
