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
package org.neo4j.tooling;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.Args.Option;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.MissingRelationshipDataException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.SpectrumExecutionMonitor;

import static java.lang.String.format;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Arrays.asList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.helpers.Exceptions.throwIfUnchecked;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Strings.TAB;
import static org.neo4j.helpers.TextUtil.tokenizeStringWithQuotes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.fs.FileUtils.readTextFile;
import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;
import static org.neo4j.kernel.impl.util.Converters.withDefault;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.BAD_FILE_NAME;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT_MAX_MEMORY_PERCENT;
import static org.neo4j.unsafe.impl.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.unsafe.impl.batchimport.Configuration.canDetectFreeMemory;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.collect;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

/**
 * User-facing command line tool around a {@link BatchImporter}.
 */
public class ImportTool
{
    private static final String INPUT_FILES_DESCRIPTION =
            new StringBuilder().append("Multiple files will be logically seen as one big file ").append("from the perspective of the importer. ").append("The first line must contain the header. ").append("Multiple data sources like these can be specified in one import, ").append("where each data source has its own header. ").append("Note that file groups must be enclosed in quotation marks. ").append("Each file can be a regular expression and will then include all matching files. ").append("The file matching is done with number awareness such that e.g. files:")
			.append("'File1Part_001.csv', 'File12Part_003' will be ordered in that order for a pattern like: 'File.*'").toString();

    private static final String UNLIMITED = "true";

	/**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";

	private static final Function<String,IdType> TO_ID_TYPE = from -> IdType.valueOf( from.toUpperCase() );

	private static final Function<String,Character> CHARACTER_CONVERTER = new CharacterConverter();

	private ImportTool()
    {
    }

	/**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     */
    public static void main( String[] incomingArguments ) throws IOException
    {
        main( incomingArguments, false );
    }

	/**
     * Runs the import tool given the supplied arguments.
     *
     * @param incomingArguments arguments for specifying input and configuration for the import.
     * @param defaultSettingsSuitableForTests default configuration geared towards unit/integration
     * test environments, for example lower default buffer sizes.
     */
    public static void main( String[] incomingArguments, boolean defaultSettingsSuitableForTests ) throws IOException
    {
        System.err.println( format( "WARNING: neo4j-import is deprecated and support for it will be removed in a future%n" +
                "version of Neo4j; please use neo4j-admin import instead." ) );

        PrintStream out = System.out;
        PrintStream err = System.err;
        Args args = Args.parse( incomingArguments );

        if ( ArrayUtil.isEmpty( incomingArguments ) || asksForUsage( args ) )
        {
            printUsage( out );
            return;
        }

        File storeDir;
        Collection<Option<File[]>> nodesFiles;
        Collection<Option<File[]>> relationshipsFiles;
        boolean enableStacktrace;
        Number processors;
        Input input;
        long badTolerance;
        Charset inputEncoding;
        boolean skipBadRelationships;
        boolean skipDuplicateNodes;
        boolean ignoreExtraColumns;
        boolean skipBadEntriesLogging;
        Config dbConfig;
        OutputStream badOutput = null;
        IdType idType;
        org.neo4j.unsafe.impl.batchimport.Configuration configuration;
        File badFile = null;
        Long maxMemory;
        Boolean defaultHighIO;
        InputStream in;

        boolean success = false;
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            args = useArgumentsFromFileArgumentIfPresent( args );

            storeDir = args.interpretOption( Options.STORE_DIR.key(), Converters.mandatory(),
                    Converters.toFile(), Validators.DIRECTORY_IS_WRITABLE );

            skipBadEntriesLogging = args.getBoolean( Options.SKIP_BAD_ENTRIES_LOGGING.key(),
                    (Boolean) Options.SKIP_BAD_ENTRIES_LOGGING.defaultValue(), false);
            if ( !skipBadEntriesLogging )
            {
                badFile = new File( storeDir, BAD_FILE_NAME );
                badOutput = new BufferedOutputStream( fs.openAsOutputStream( badFile, false ) );
            }
            nodesFiles = extractInputFiles( args, Options.NODE_DATA.key(), err );
            relationshipsFiles = extractInputFiles( args, Options.RELATIONSHIP_DATA.key(), err );
            String maxMemoryString = args.get( Options.MAX_MEMORY.key(), null );
            maxMemory = parseMaxMemory( maxMemoryString );

            validateInputFiles( nodesFiles, relationshipsFiles );
            enableStacktrace = args.getBoolean( Options.STACKTRACE.key(), Boolean.FALSE, Boolean.TRUE );
            processors = args.getNumber( Options.PROCESSORS.key(), null );
            idType = args.interpretOption( Options.ID_TYPE.key(),
                    withDefault( (IdType)Options.ID_TYPE.defaultValue() ), TO_ID_TYPE );
            badTolerance = parseNumberOrUnlimited( args, Options.BAD_TOLERANCE );
            inputEncoding = Charset.forName( args.get( Options.INPUT_ENCODING.key(), defaultCharset().name() ) );

            skipBadRelationships = args.getBoolean( Options.SKIP_BAD_RELATIONSHIPS.key(),
                    (Boolean)Options.SKIP_BAD_RELATIONSHIPS.defaultValue(), true );
            skipDuplicateNodes = args.getBoolean( Options.SKIP_DUPLICATE_NODES.key(),
                    (Boolean)Options.SKIP_DUPLICATE_NODES.defaultValue(), true );
            ignoreExtraColumns = args.getBoolean( Options.IGNORE_EXTRA_COLUMNS.key(),
                    (Boolean)Options.IGNORE_EXTRA_COLUMNS.defaultValue(), true );
            defaultHighIO = args.getBoolean( Options.HIGH_IO.key(),
                    (Boolean)Options.HIGH_IO.defaultValue(), true );

            Collector badCollector = getBadCollector( badTolerance, skipBadRelationships, skipDuplicateNodes, ignoreExtraColumns,
                    skipBadEntriesLogging, badOutput );

            dbConfig = loadDbConfig( args.interpretOption( Options.DATABASE_CONFIG.key(), Converters.optional(),
                    Converters.toFile(), Validators.REGEX_FILE_EXISTS ) );
            dbConfig.augment( loadDbConfig( args.interpretOption( Options.ADDITIONAL_CONFIG.key(), Converters.optional(),
                    Converters.toFile(), Validators.REGEX_FILE_EXISTS ) ) );
            dbConfig.augment( GraphDatabaseSettings.neo4j_home, storeDir.getCanonicalFile().getParentFile().getAbsolutePath() );
            boolean allowCacheOnHeap = args.getBoolean( Options.CACHE_ON_HEAP.key(),
                    (Boolean) Options.CACHE_ON_HEAP.defaultValue() );
            configuration = importConfiguration(
                    processors, defaultSettingsSuitableForTests, dbConfig, maxMemory, storeDir,
                    allowCacheOnHeap, defaultHighIO );
            input = new CsvInput( nodeData( inputEncoding, nodesFiles ), defaultFormatNodeFileHeader(),
                    relationshipData( inputEncoding, relationshipsFiles ), defaultFormatRelationshipFileHeader(),
                    idType, csvConfiguration( args, defaultSettingsSuitableForTests ), badCollector,
                    new CsvInput.PrintingMonitor( out ) );
            in = defaultSettingsSuitableForTests ? new ByteArrayInputStream( EMPTY_BYTE_ARRAY ) : System.in;
            boolean detailedPrinting = args.getBoolean( Options.DETAILED_PROGRESS.key(), (Boolean) Options.DETAILED_PROGRESS.defaultValue() );

            doImport( out, err, in, DatabaseLayout.of( storeDir ), badFile, fs, nodesFiles, relationshipsFiles,
                    enableStacktrace, input, dbConfig, badOutput, configuration, detailedPrinting );

            success = true;
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false, err );
        }
        catch ( IOException | UncheckedIOException e )
        {
            throw andPrintError( "File error", e, false, err );
        }
        finally
        {
            if ( !success && badOutput != null )
            {
                badOutput.close();
            }
        }
    }

	public static Args useArgumentsFromFileArgumentIfPresent( Args args ) throws IOException
    {
        String fileArgument = args.get( Options.FILE.key(), null );
        if ( fileArgument != null )
        {
            // Are there any other arguments supplied, in addition to this -f argument?
            if ( args.asMap().size() > 1 )
            {
                throw new IllegalArgumentException(
                        new StringBuilder().append("Supplying arguments in addition to ").append(Options.FILE.argument()).append(" isn't supported.").toString() );
            }

            // Read the arguments from the -f file and use those instead
            args = Args.parse( parseFileArgumentList( new File( fileArgument ) ) );
        }
        return args;
    }

	public static String[] parseFileArgumentList( File file ) throws IOException
    {
        List<String> arguments = new ArrayList<>();
        readTextFile( file, line -> arguments.addAll( asList( tokenizeStringWithQuotes( line, true, true, false ) ) ) );
        return arguments.toArray( new String[arguments.size()] );
    }

	static Long parseMaxMemory( String maxMemoryString )
    {
        if (maxMemoryString == null) {
			return null;
		}
		maxMemoryString = maxMemoryString.trim();
		if ( maxMemoryString.endsWith( "%" ) )
		{
		    int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
		    long result = calculateMaxMemoryFromPercent( percent );
		    if ( !canDetectFreeMemory() )
		    {
		        System.err.println( new StringBuilder().append("WARNING: amount of free memory couldn't be detected so defaults to ").append(bytes( result )).append(". For optimal performance instead explicitly specify amount of ").append("memory that importer is allowed to use using ").append(Options.MAX_MEMORY.argument()).toString() );
		    }
		    return result;
		}
		return Settings.parseLongWithUnit( maxMemoryString );
    }

	public static void doImport( PrintStream out, PrintStream err, InputStream in, DatabaseLayout databaseLayout, File badFile,
                                 FileSystemAbstraction fs, Collection<Option<File[]>> nodesFiles,
                                 Collection<Option<File[]>> relationshipsFiles, boolean enableStacktrace, Input input,
                                 Config dbConfig, OutputStream badOutput,
                                 org.neo4j.unsafe.impl.batchimport.Configuration configuration, boolean detailedProgress ) throws IOException
    {
        boolean success;
        LifeSupport life = new LifeSupport();

        File internalLogFile = dbConfig.get( store_internal_log_path );
        LogService logService = life.add( StoreLogService.withInternalLog( internalLogFile ).build( fs ) );
        final JobScheduler jobScheduler = life.add( createScheduler() );

        life.start();
        ExecutionMonitor executionMonitor = detailedProgress
                        ? new SpectrumExecutionMonitor( 2, TimeUnit.SECONDS, out, SpectrumExecutionMonitor.DEFAULT_WIDTH )
                        : ExecutionMonitors.defaultVisible( in, jobScheduler );
        BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate( databaseLayout,
                fs,
                null, // no external page cache
                configuration,
                logService, executionMonitor,
                EMPTY,
                dbConfig,
                RecordFormatSelector.selectForConfig( dbConfig, logService.getInternalLogProvider() ),
                new PrintingImportLogicMonitor( out, err ), jobScheduler );
        printOverview( databaseLayout.databaseDirectory(), nodesFiles, relationshipsFiles, configuration, out );
        success = false;
        try
        {
            importer.doImport( input );
            success = true;
        }
        catch ( Exception e )
        {
            throw andPrintError( "Import error", e, enableStacktrace, err );
        }
        finally
        {
            Collector collector = input.badCollector();
            long numberOfBadEntries = collector.badEntries();
            collector.close();
            IOUtils.closeAll( badOutput );

            boolean condition = badFile != null && numberOfBadEntries > 0;
			if ( condition ) {
			    System.out.println( "There were bad entries which were skipped and logged into " +
			            badFile.getAbsolutePath() );
			}

            life.shutdown();

            if ( !success )
            {
                err.println( new StringBuilder().append("WARNING Import failed. The store files in ").append(databaseLayout.databaseDirectory().getAbsolutePath()).append(" are left as they are, although they are likely in an unusable state. ").append("Starting a database on these store files will likely fail or observe inconsistent records so ").append("start at your own risk or delete the store manually").toString() );
            }
        }
    }

	public static Collection<Option<File[]>> extractInputFiles( Args args, String key, PrintStream err )
    {
        return args
                .interpretOptionsWithMetadata( key, Converters.optional(),
                        Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ),
                        filesExist( err ),
                        Validators.atLeast( "--" + key, 1 ) );
    }

	private static Validator<File[]> filesExist( PrintStream err )
    {
        return files ->
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    err.println( new StringBuilder().append("It looks like you're trying to specify default label or relationship type (").append(file.getName()).append("). Please put such directly on the key, f.ex. ").append(Options.NODE_DATA.argument()).append(":MyLabel").toString() );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        };
    }

	private static Collector getBadCollector( long badTolerance, boolean skipBadRelationships,
            boolean skipDuplicateNodes, boolean ignoreExtraColumns, boolean skipBadEntriesLogging,
            OutputStream badOutput )
    {
        int collect = collect( skipBadRelationships, skipDuplicateNodes, ignoreExtraColumns );
        return skipBadEntriesLogging ? silentBadCollector( badTolerance, collect ) : badCollector( badOutput, badTolerance, collect );
    }

	private static long parseNumberOrUnlimited( Args args, Options option )
    {
        String value = args.get( option.key(), option.defaultValue().toString() );
        return UNLIMITED.equals( value ) ? BadCollector.UNLIMITED_TOLERANCE : Long.parseLong( value );
    }

	private static Config loadDbConfig( File file )
    {
        return Config.fromFile( file ).build();
    }

	static void printOverview( File storeDir, Collection<Option<File[]>> nodesFiles,
            Collection<Option<File[]>> relationshipsFiles,
            org.neo4j.unsafe.impl.batchimport.Configuration configuration, PrintStream out )
    {
        out.println( "Neo4j version: " + Version.getNeo4jVersion() );
        out.println( new StringBuilder().append("Importing the contents of these files into ").append(storeDir).append(":").toString() );
        printInputFiles( "Nodes", nodesFiles, out );
        printInputFiles( "Relationships", relationshipsFiles, out );
        out.println();
        out.println( "Available resources:" );
        printIndented( "Total machine memory: " + bytes( OsBeanUtil.getTotalPhysicalMemory() ), out );
        printIndented( "Free machine memory: " + bytes( OsBeanUtil.getFreePhysicalMemory() ), out );
        printIndented( "Max heap memory : " + bytes( Runtime.getRuntime().maxMemory() ), out );
        printIndented( "Processors: " + configuration.maxNumberOfProcessors(), out );
        printIndented( "Configured max memory: " + bytes( configuration.maxMemoryUsage() ), out );
        printIndented( "High-IO: " + configuration.highIO(), out );
        out.println();
    }

	private static void printInputFiles( String name, Collection<Option<File[]>> files, PrintStream out )
    {
        if ( files.isEmpty() )
        {
            return;
        }

        out.println( name + ":" );
        int i = 0;
        for ( Option<File[]> group : files )
        {
            if ( i++ > 0 )
            {
                out.println();
            }
            if ( group.metadata() != null )
            {
                printIndented( ":" + group.metadata(), out );
            }
            for ( File file : group.value() )
            {
                printIndented( file, out );
            }
        }
    }

	private static void printIndented( Object value, PrintStream out )
    {
        out.println( "  " + value );
    }

	public static void validateInputFiles( Collection<Option<File[]>> nodesFiles,
            Collection<Option<File[]>> relationshipsFiles )
    {
        if (!nodesFiles.isEmpty()) {
			return;
		}
		if ( relationshipsFiles.isEmpty() )
		{
		    throw new IllegalArgumentException( "No input specified, nothing to import" );
		}
		throw new IllegalArgumentException( "No node input specified, cannot import relationships without nodes" );
    }

	public static org.neo4j.unsafe.impl.batchimport.Configuration importConfiguration(
            Number processors, boolean defaultSettingsSuitableForTests, Config dbConfig, File storeDir, Boolean defaultHighIO )
    {
        return importConfiguration(
                processors, defaultSettingsSuitableForTests, dbConfig, null, storeDir,
                DEFAULT.allowCacheAllocationOnHeap(), defaultHighIO );
    }

	public static org.neo4j.unsafe.impl.batchimport.Configuration importConfiguration(
            Number processors, boolean defaultSettingsSuitableForTests, Config dbConfig, Long maxMemory, File storeDir,
            boolean allowCacheOnHeap, Boolean defaultHighIO )
    {
        return new org.neo4j.unsafe.impl.batchimport.Configuration()
        {
            @Override
            public long pageCacheMemory()
            {
                return defaultSettingsSuitableForTests ? mebiBytes( 8 ) : DEFAULT.pageCacheMemory();
            }

            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : DEFAULT.maxNumberOfProcessors();
            }

            @Override
            public int denseNodeThreshold()
            {
                return dbConfig.get( GraphDatabaseSettings.dense_node_threshold );
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory : DEFAULT.maxMemoryUsage();
            }

            @Override
            public boolean highIO()
            {
                return defaultHighIO != null ? defaultHighIO : FileUtils.highIODevice( storeDir.toPath(), false );
            }

            @Override
            public boolean allowCacheAllocationOnHeap()
            {
                return allowCacheOnHeap;
            }
        };
    }

	private static String manualReference( ManualPage page, Anchor anchor )
    {
        // Docs are versioned major.minor-suffix, so drop the patch version.
        String[] versionParts = Version.getNeo4jVersion().split("-");
        versionParts[0] = versionParts[0].substring(0, 3);
        String docsVersion = String.join("-", versionParts);

        return new StringBuilder().append(" https://neo4j.com/docs/operations-manual/").append(docsVersion).append("/").append(page.getReference( anchor )).toString();
    }

	/**
     * Method name looks strange, but look at how it's used and you'll see why it's named like that.
     * @param stackTrace whether or not to also print the stack trace of the error.
     * @param err
     */
    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace,
            PrintStream err )
    {
        // List of common errors that can be explained to the user
        if ( DuplicateInputIdException.class.equals( e.getClass() ) )
        {
            printErrorMessage( new StringBuilder().append("Duplicate input ids that would otherwise clash can be put into separate id space, ").append("read more about how to use id spaces in the manual:").append(manualReference( ManualPage.IMPORT_TOOL_FORMAT, Anchor.ID_SPACES )).toString(), e, stackTrace,
                    err );
        }
        else if ( MissingRelationshipDataException.class.equals( e.getClass() ) )
        {
            printErrorMessage( new StringBuilder().append("Relationship missing mandatory field '").append(((MissingRelationshipDataException) e).getFieldType()).append("', read more about ").append("relationship format in the manual: ").append(manualReference( ManualPage.IMPORT_TOOL_FORMAT, Anchor.RELATIONSHIP )).toString(), e, stackTrace,
                    err );
        }
        // This type of exception is wrapped since our input code throws InputException consistently,
        // and so IllegalMultilineFieldException comes from the csv component, which has no access to InputException
        // therefore it's wrapped.
        else if ( Exceptions.contains( e, IllegalMultilineFieldException.class ) )
        {
            printErrorMessage( new StringBuilder().append("Detected field which spanned multiple lines for an import where ").append(Options.MULTILINE_FIELDS.argument()).append("=false. If you know that your input data ").append("include fields containing new-line characters then import with this option set to ").append("true.").toString(), e, stackTrace, err );
        }
        else if ( Exceptions.contains( e, InputException.class ) )
        {
            printErrorMessage( "Error in input data", e, stackTrace, err );
        }
        // Fallback to printing generic error and stack trace
        else
        {
            printErrorMessage( new StringBuilder().append(typeOfError).append(": ").append(e.getMessage()).toString(), e, true, err );
        }
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( ( t, e1 ) ->
        {
            /* Shhhh */
        } );
        throwIfUnchecked( e );
        return new RuntimeException( e ); // throw in order to have process exit with !0
    }

	private static void printErrorMessage( String string, Exception e, boolean stackTrace, PrintStream err )
    {
        err.println( string );
        err.println( "Caused by:" + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( err );
        }
    }

	public static Iterable<DataFactory>
            relationshipData( final Charset encoding, Collection<Option<File[]>> relationshipsFiles )
    {
        return new IterableWrapper<DataFactory,Option<File[]>>( relationshipsFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Option<File[]> group )
            {
                return data( defaultRelationshipType( group.metadata() ), encoding, group.value() );
            }
        };
    }

	public static Iterable<DataFactory> nodeData( final Charset encoding,
            Collection<Option<File[]>> nodesFiles )
    {
        return new IterableWrapper<DataFactory,Option<File[]>>( nodesFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Option<File[]> input )
            {
                Decorator decorator = input.metadata() != null
                        ? additiveLabels( input.metadata().split( ":" ) )
                        : NO_DECORATOR;
                return data( decorator, encoding, input.value() );
            }
        };
    }

	private static void printUsage( PrintStream out )
    {
        out.println( "Neo4j Import Tool" );
        for ( String line : Args.splitLongLine( new StringBuilder().append("neo4j-import is used to create a new Neo4j database ").append("from data in CSV files. ").append("See the chapter \"Import Tool\" in the Neo4j Manual for details on the CSV file format ").append("- a special kind of header is required.").toString(), 80 ) )
        {
            out.println( "\t" + line );
        }
        out.println( "Usage:" );
        for ( Options option : Options.values() )
        {
            option.printUsage( out );
        }

        out.println( "Example:");
        out.print( Strings.joinAsLines(
                TAB + "bin/neo4j-import --into retail.db --id-type string --nodes:Customer customers.csv ",
                TAB + "--nodes products.csv --nodes orders_header.csv,orders1.csv,orders2.csv ",
                TAB + "--relationships:CONTAINS order_details.csv ",
                TAB + "--relationships:ORDERED customer_orders_header.csv,orders1.csv,orders2.csv" ) );
    }

	private static boolean asksForUsage( Args args )
    {
        return args.orphans().stream().filter(ImportTool::isHelpKey).findFirst().map(orphan -> true)
				.orElse(args.asMap().entrySet().stream().anyMatch(option -> isHelpKey(option.getKey())));
    }

	private static boolean isHelpKey( String key )
    {
        return "?".equals( key ) || "help".equals( key );
    }

	public static Configuration csvConfiguration( Args args, final boolean defaultSettingsSuitableForTests )
    {
        final Configuration defaultConfiguration = COMMAS;
        final Character specificDelimiter = args.interpretOption( Options.DELIMITER.key(),
                Converters.optional(), CHARACTER_CONVERTER );
        final Character specificArrayDelimiter = args.interpretOption( Options.ARRAY_DELIMITER.key(),
                Converters.optional(), CHARACTER_CONVERTER );
        final Character specificQuote = args.interpretOption( Options.QUOTE.key(), Converters.optional(),
                CHARACTER_CONVERTER );
        final Boolean multiLineFields = args.getBoolean( Options.MULTILINE_FIELDS.key(), null );
        final Boolean emptyStringsAsNull = args.getBoolean( Options.IGNORE_EMPTY_STRINGS.key(), null );
        final Boolean trimStrings = args.getBoolean( Options.TRIM_STRINGS.key(), null);
        final Boolean legacyStyleQuoting = args.getBoolean( Options.LEGACY_STYLE_QUOTING.key(), null );
        final Number bufferSize = args.has( Options.READ_BUFFER_SIZE.key() )
                ? parseLongWithUnit( args.get( Options.READ_BUFFER_SIZE.key(), null ) )
                : null;
        return new Configuration.Default()
        {
            @Override
            public char delimiter()
            {
                return specificDelimiter != null
                        ? specificDelimiter
                        : defaultConfiguration.delimiter();
            }

            @Override
            public char arrayDelimiter()
            {
                return specificArrayDelimiter != null
                        ? specificArrayDelimiter
                        : defaultConfiguration.arrayDelimiter();
            }

            @Override
            public char quotationCharacter()
            {
                return specificQuote != null
                        ? specificQuote
                        : defaultConfiguration.quotationCharacter();
            }

            @Override
            public boolean multilineFields()
            {
                return multiLineFields != null
                        ? multiLineFields
                        : defaultConfiguration.multilineFields();
            }

            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return emptyStringsAsNull != null
                        ? emptyStringsAsNull
                        : defaultConfiguration.emptyQuotedStringsAsNull();
            }

            @Override
            public int bufferSize()
            {
                return bufferSize != null
                        ? bufferSize.intValue()
                        : defaultSettingsSuitableForTests ? 10_000 : super.bufferSize();
            }

            @Override
            public boolean trimStrings()
            {
                return trimStrings != null
                       ? trimStrings
                       : defaultConfiguration.trimStrings();
            }

            @Override
            public boolean legacyStyleQuoting()
            {
                return legacyStyleQuoting != null
                        ? legacyStyleQuoting
                        : defaultConfiguration.legacyStyleQuoting();
            }
        };
    }

	enum Options
    {
        FILE( "f", null,
                "<file name>",
                new StringBuilder().append("File containing all arguments, used as an alternative to supplying all arguments on the command line directly.").append("Each argument can be on a separate line or multiple arguments per line separated by space.").append("Arguments containing spaces needs to be quoted.").append("Supplying other arguments in addition to this file argument is not supported.").toString() ),
        STORE_DIR( "into", null,
                "<store-dir>",
                "Database directory to import into. " + "Must not contain existing database." ),
        DB_NAME( "database", null,
                "<database-name>",
                "Database name to import into. " + "Must not contain existing database.", true ),
        NODE_DATA( "nodes", null,
                new StringBuilder().append("[:Label1:Label2] \"<file1>").append(MULTI_FILE_DELIMITER).append("<file2>").append(MULTI_FILE_DELIMITER).append("...\"").toString(),
                "Node CSV header and data. " + INPUT_FILES_DESCRIPTION,
                        true, true ),
        RELATIONSHIP_DATA( "relationships", null,
                new StringBuilder().append("[:RELATIONSHIP_TYPE] \"<file1>").append(MULTI_FILE_DELIMITER).append("<file2>").append(MULTI_FILE_DELIMITER).append("...\"").toString(),
                "Relationship CSV header and data. " + INPUT_FILES_DESCRIPTION,
                        true, true ),
        DELIMITER( "delimiter", null,
                "<delimiter-character>",
                new StringBuilder().append("Delimiter character, or 'TAB', between values in CSV data. The default option is `").append(COMMAS.delimiter()).append("`.").toString() ),
        ARRAY_DELIMITER( "array-delimiter", null,
                "<array-delimiter-character>",
                new StringBuilder().append("Delimiter character, or 'TAB', between array elements within a value in CSV data. ").append("The default option is `").append(COMMAS.arrayDelimiter()).append("`.").toString() ),
        QUOTE( "quote", null,
                "<quotation-character>",
                new StringBuilder().append("Character to treat as quotation character for values in CSV data. ").append("The default option is `").append(COMMAS.quotationCharacter()).append("`. ").append("Quotes inside quotes escaped like `\"\"\"Go away\"\", he said.\"` and ").append("`\"\\\"Go away\\\", he said.\"` are supported. ").append("If you have set \"`'`\" to be used as the quotation character, ")
						.append("you could write the previous example like this instead: ").append("`'\"Go away\", he said.'`").toString() ),
        MULTILINE_FIELDS( "multiline-fields", org.neo4j.csv.reader.Configuration.DEFAULT.multilineFields(),
                "<true/false>",
                "Whether or not fields from input source can span multiple lines, i.e. contain newline characters." ),

        TRIM_STRINGS( "trim-strings", org.neo4j.csv.reader.Configuration.DEFAULT.trimStrings(),
                "<true/false>",
                "Whether or not strings should be trimmed for whitespaces." ),

        INPUT_ENCODING( "input-encoding", null,
                "<character set>",
                new StringBuilder().append("Character set that input data is encoded in. Provided value must be one out of the available ").append("character sets in the JVM, as provided by Charset#availableCharsets(). ").append("If no input encoding is provided, the default character set of the JVM will be used.").toString(),
                true ),
        IGNORE_EMPTY_STRINGS( "ignore-empty-strings", org.neo4j.csv.reader.Configuration.DEFAULT.emptyQuotedStringsAsNull(),
                "<true/false>",
                "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null." ),
        ID_TYPE( "id-type", IdType.STRING,
                "<id-type>",
                new StringBuilder().append("One out of ").append(Arrays.toString( IdType.values() )).append(" and specifies how ids in node/relationship ").append("input files are treated.\n").append(IdType.STRING)
						.append(": arbitrary strings for identifying nodes.\n").append(IdType.INTEGER).append(": arbitrary integer values for identifying nodes.\n").append(IdType.ACTUAL).append(": (advanced) actual node ids. The default option is `")
						.append(IdType.STRING).append("`.").toString(), true ),
        PROCESSORS( "processors", null,
                "<max processor count>",
                new StringBuilder().append("(advanced) Max number of processors used by the importer. Defaults to the number of ").append("available processors reported by the JVM").append(availableProcessorsHint()).append(". There is a certain amount of minimum threads needed so for that reason there ").append("is no lower bound for this value. For optimal performance this value shouldn't be ").append("greater than the number of available processors.").toString() ),
        STACKTRACE( "stacktrace", false,
                "<true/false>",
                "Enable printing of error stack traces." ),
        BAD_TOLERANCE( "bad-tolerance", 1000,
                new StringBuilder().append("<max number of bad entries, or ").append(UNLIMITED).append(" for unlimited>").toString(),
                new StringBuilder().append("Number of bad entries before the import is considered failed. This tolerance threshold is ").append("about relationships referring to missing nodes. Format errors in input data are ").append("still treated as errors").toString() ),
        SKIP_BAD_ENTRIES_LOGGING( "skip-bad-entries-logging", Boolean.FALSE, "<true/false>",
                "Whether or not to skip logging bad entries detected during import." ),
        SKIP_BAD_RELATIONSHIPS( "skip-bad-relationships", Boolean.TRUE,
                "<true/false>",
                new StringBuilder().append("Whether or not to skip importing relationships that refers to missing node ids, i.e. either ").append("start or end node id/group referring to node that wasn't specified by the ").append("node input data. ").append("Skipped nodes will be logged").append(", containing at most number of entities specified by ").append(BAD_TOLERANCE.key()).append(", unless ")
						.append("otherwise specified by ").append(SKIP_BAD_ENTRIES_LOGGING.key()).append(" option.").toString() ),
        SKIP_DUPLICATE_NODES( "skip-duplicate-nodes", Boolean.FALSE,
                "<true/false>",
                new StringBuilder().append("Whether or not to skip importing nodes that have the same id/group. In the event of multiple ").append("nodes within the same group having the same id, the first encountered will be imported ").append("whereas consecutive such nodes will be skipped. ").append("Skipped nodes will be logged").append(", containing at most number of entities specified by ").append(BAD_TOLERANCE.key()).append(", unless ")
						.append("otherwise specified by ").append(SKIP_BAD_ENTRIES_LOGGING.key()).append("option.").toString() ),
        IGNORE_EXTRA_COLUMNS( "ignore-extra-columns", Boolean.FALSE,
                "<true/false>",
                new StringBuilder().append("Whether or not to ignore extra columns in the data not specified by the header. ").append("Skipped columns will be logged, containing at most number of entities specified by ").append(BAD_TOLERANCE.key()).append(", unless ").append("otherwise specified by ").append(SKIP_BAD_ENTRIES_LOGGING.key())
						.append("option.").toString() ),
        DATABASE_CONFIG( "db-config", null, new StringBuilder().append("<path/to/").append(Config.DEFAULT_CONFIG_FILE_NAME).append(">").toString(),
                "(advanced) Option is deprecated and replaced by 'additional-config'. " ),
        ADDITIONAL_CONFIG( "additional-config", null,
                new StringBuilder().append("<path/to/").append(Config.DEFAULT_CONFIG_FILE_NAME).append(">").toString(),
                new StringBuilder().append("(advanced) File specifying database-specific configuration. For more information consult ").append("manual about available configuration options for a neo4j configuration file. ").append("Only configuration affecting store at time of creation will be read. ").append("Examples of supported config are:\n").append(GraphDatabaseSettings.dense_node_threshold.name()).append("\n")
						.append(GraphDatabaseSettings.string_block_size.name()).append("\n").append(GraphDatabaseSettings.array_block_size.name()).toString(), true ),
        LEGACY_STYLE_QUOTING( "legacy-style-quoting", Configuration.DEFAULT_LEGACY_STYLE_QUOTING,
                "<true/false>",
                "Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner quote." ),
        READ_BUFFER_SIZE( "read-buffer-size", org.neo4j.csv.reader.Configuration.DEFAULT.bufferSize(),
                "<bytes, e.g. 10k, 4M>",
                "Size of each buffer for reading input data. It has to at least be large enough to hold the " +
                "biggest single value in the input data." ),
        MAX_MEMORY( "max-memory", null,
                "<max memory that importer can use>",
                new StringBuilder().append("(advanced) Maximum memory that importer can use for various data structures and caching ").append("to improve performance. If left as unspecified (null) it is set to ").append(DEFAULT_MAX_MEMORY_PERCENT).append("% of (free memory on machine - max JVM memory). ").append("Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%.").toString() ),
        CACHE_ON_HEAP( "cache-on-heap",
                DEFAULT.allowCacheAllocationOnHeap(),
                "Whether or not to allow allocating memory for the cache on heap",
                new StringBuilder().append("(advanced) Whether or not to allow allocating memory for the cache on heap. ").append("If 'false' then caches will still be allocated off-heap, but the additional free memory ").append("inside the JVM will not be allocated for the caches. This to be able to have better control ").append("over the heap memory").toString() ),
        HIGH_IO( "high-io", null, "Assume a high-throughput storage subsystem",
                "(advanced) Ignore environment-based heuristics, and assume that the target storage subsystem can " +
                "support parallel IO with high throughput." ),
        DETAILED_PROGRESS( "detailed-progress", false, "true/false", "Use the old detailed 'spectrum' progress printing" );

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;
        private final boolean keyAndUsageGoTogether;
        private final boolean supported;

        Options( String key, Object defaultValue, String usage, String description )
        {
            this( key, defaultValue, usage, description, false, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported )
        {
            this( key, defaultValue, usage, description, supported, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported, boolean keyAndUsageGoTogether )
        {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
            this.supported = supported;
            this.keyAndUsageGoTogether = keyAndUsageGoTogether;
        }

        String key()
        {
            return key;
        }

        String argument()
        {
            return "--" + key();
        }

        void printUsage( PrintStream out )
        {
            out.println( new StringBuilder().append(argument()).append(spaceInBetweenArgumentAndUsage()).append(usage).toString() );
            for ( String line : Args.splitLongLine( descriptionWithDefaultValue().replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        private String spaceInBetweenArgumentAndUsage()
        {
            return keyAndUsageGoTogether ? "" : " ";
        }

        String descriptionWithDefaultValue()
        {
            String result = description;
            if ( defaultValue != null )
            {
                if ( !result.endsWith( "." ) )
                {
                    result += ".";
                }
                result += " Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry()
        {
            String filteredDescription = descriptionWithDefaultValue().replace( availableProcessorsHint(), "" );
            String usageString = (usage.length() > 0) ? spaceInBetweenArgumentAndUsage() + usage : "";
            return new StringBuilder().append("*").append(argument()).append(usageString).append("*::\n").append(filteredDescription).append("\n\n")
					.toString();
        }

        String manualEntry()
        {
            return new StringBuilder().append("[[import-tool-option-").append(key()).append("]]\n").append(manPageEntry()).append("//^\n\n").toString();
        }

        Object defaultValue()
        {
            return defaultValue;
        }

        private static String availableProcessorsHint()
        {
            return new StringBuilder().append(" (in your case ").append(Runtime.getRuntime().availableProcessors()).append(")").toString();
        }

        public boolean isSupportedOption()
        {
            return this.supported;
        }
    }

	private enum ManualPage
    {
        IMPORT_TOOL_FORMAT( "tools/import/file-header-format/" );

        private final String page;

        ManualPage( String page )
        {
            this.page = page;
        }

        public String getReference( Anchor anchor )
        {
            // As long as the the operations manual is single-page we only use the anchor.
            return new StringBuilder().append(page).append("#").append(anchor.anchor).toString();
        }
    }

	private enum Anchor
    {
        ID_SPACES( "import-tool-id-spaces" ),
        RELATIONSHIP( "import-tool-header-format-rels" );

        private final String anchor;

        Anchor( String anchor )
        {
            this.anchor = anchor;
        }
    }
}
