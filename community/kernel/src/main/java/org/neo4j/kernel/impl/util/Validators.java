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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;

public class Validators
{
    public static final Validator<File> REGEX_FILE_EXISTS = file ->
    {
        if ( matchingFiles( file ).isEmpty() )
        {
            throw new IllegalArgumentException( new StringBuilder().append("File '").append(file).append("' doesn't exist").toString() );
        }
    };

	public static final Validator<File> DIRECTORY_IS_WRITABLE = value ->
    {
        if ( value.mkdirs() )
        {   // It's OK, we created the directory right now, which means we have write access to it
            return;
        }

        File test = new File( value, "_______test___" );
        try
        {
            test.createNewFile();
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( new StringBuilder().append("Directory '").append(value).append("' not writable: ").append(e.getMessage()).toString() );
        }
        finally
        {
            test.delete();
        }
    };

	public static final Validator<File> CONTAINS_NO_EXISTING_DATABASE = value ->
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            if ( isExistingDatabase( fileSystem, DatabaseLayout.of( value ) ) )
            {
                throw new IllegalArgumentException( new StringBuilder().append("Directory '").append(value).append("' already contains a database").toString() );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    };

	public static final Validator<File> CONTAINS_EXISTING_DATABASE = dbDir ->
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            if ( !isExistingDatabase( fileSystem, DatabaseLayout.of( dbDir ) ) )
            {
                throw new IllegalArgumentException( new StringBuilder().append("Directory '").append(dbDir).append("' does not contain a database").toString() );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    };

	private Validators()
    {
    }

	static List<File> matchingFiles( File fileWithRegexInName )
    {
        File parent = fileWithRegexInName.getAbsoluteFile().getParentFile();
        if ( parent == null || !parent.exists() )
        {
            throw new IllegalArgumentException( new StringBuilder().append("Directory of ").append(fileWithRegexInName).append(" doesn't exist").toString() );
        }
        final Pattern pattern = Pattern.compile( fileWithRegexInName.getName() );
        List<File> files = new ArrayList<>();
        for ( File file : parent.listFiles() )
        {
            if ( pattern.matcher( file.getName() ).matches() )
            {
                files.add( file );
            }
        }
        return files;
    }

	private static boolean isExistingDatabase( FileSystemAbstraction fileSystem, DatabaseLayout layout )
    {
        return fileSystem.fileExists( layout.metadataStore() );
    }

	public static Validator<String> inList( String[] validStrings )
    {
        return value ->
        {
            if ( Arrays.stream( validStrings ).noneMatch( s -> s.equals( value ) ) )
            {
                throw new IllegalArgumentException( new StringBuilder().append("'").append(value).append("' found but must be one of: ").append(Arrays.toString( validStrings )).append(".").toString() );
            }
        };
    }

	public static <T> Validator<T[]> atLeast( final String key, final int length )
    {
        return value ->
        {
            if ( value.length < length )
            {
                throw new IllegalArgumentException( new StringBuilder().append("Expected '").append(key).append("' to have at least ").append(length).append(" valid item")
						.append(length == 1 ? "" : "s").append(", but had ").append(value.length).append(" ")
						.append(Arrays.toString( value )).toString() );
            }
        };
    }

	public static <T> Validator<T> emptyValidator()
    {
        return value -> {};
    }

	public static <T> Validator<T> all( Validator<T>... validators )
    {
        return value ->
        {
            for ( Validator<T> validator : validators )
            {
                validator.validate( value );
            }
        };
    }
}
