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
package org.neo4j.server.rest.domain;

import java.util.Collection;
import java.util.Map;

/**
 * This is just a simple test of how a HTML renderer could be like
 */
public class HtmlHelper
{
    private static final String STYLE_LOCATION = "http://resthtml.neo4j.org/style/";

    private HtmlHelper()
    {
    }

    public static String from( final Object object, final ObjectType objectType )
    {
        StringBuilder builder = start( objectType, null );
        append( builder, object, objectType );
        return end( builder );
    }

    public static StringBuilder start( final ObjectType objectType, final String additionalCodeInHead )
    {
        return start( objectType.getCaption(), additionalCodeInHead );
    }

    public static StringBuilder start( final String title, final String additionalCodeInHead )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n" );
        builder.append( new StringBuilder().append("<html><head><title>").append(title).append("</title>").toString() );
        if ( additionalCodeInHead != null )
        {
            builder.append( additionalCodeInHead );
        }
        builder.append( new StringBuilder().append("<meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\">\n").append("<link href='").append(STYLE_LOCATION).append("rest.css' rel='stylesheet' type='text/css'>\n").append("</head>\n<body onload='javascript:neo4jHtmlBrowse.start();' id='").append(title.toLowerCase()).append("'>\n")
				.append("<div id='content'>").append("<div id='header'>").append("<h1><a title='Neo4j REST interface' href='/'><span>Neo4j REST interface</span></a></h1>").append("</div>").append("\n<div id='page-body'>\n").toString() );
        return builder;
    }

    public static String end( final StringBuilder builder )
    {
        builder.append( "<div class='break'>&nbsp;</div>" + "</div></div></body></html>" );
        return builder.toString();
    }

    public static void appendMessage( final StringBuilder builder, final String message )
    {
        builder.append( new StringBuilder().append("<p class=\"message\">").append(message).append("</p>").toString() );
    }

    public static void append( final StringBuilder builder, final Object object, final ObjectType objectType )
    {
        if ( object instanceof Collection )
        {
            builder.append( "<ul>\n" );
            ((Collection<?>) object).forEach(item -> {
                builder.append( "<li>" );
                append( builder, item, objectType );
                builder.append( "</li>\n" );
            });
            builder.append( "</ul>\n" );
        }
        else if ( object instanceof Map )
        {
            Map<?, ?> map = (Map<?, ?>) object;
            String htmlClass = objectType.getHtmlClass();
            String caption = objectType.getCaption();
            if ( !map.isEmpty() )
            {
                boolean isNodeOrRelationship = ObjectType.NODE == objectType
                                               || ObjectType.RELATIONSHIP == objectType;
                if ( isNodeOrRelationship )
                {
                    builder.append( new StringBuilder().append("<h2>").append(caption).append("</h2>\n").toString() );
                    append( builder, map.get( "data" ), ObjectType.PROPERTIES );
                    htmlClass = "meta";
                    caption += " info";
                }
                if ( ObjectType.NODE == objectType && map.size() == 1 )
                {
                    // there's only properties, so we're finished here
                    return;
                }
                builder.append( new StringBuilder().append("<table class=\"").append(htmlClass).append("\"><caption>").toString() );
                builder.append( caption );
                builder.append( "</caption>\n" );
                boolean odd = true;
                for ( Map.Entry<?, ?> entry : map.entrySet() )
                {
                    if ( isNodeOrRelationship && "data".equals( entry.getKey() ) )
                    {
                        continue;
                    }
                    builder.append( new StringBuilder().append("<tr").append(odd ? " class='odd'" : "").append(">").toString() );
                    odd = !odd;
                    builder.append( new StringBuilder().append("<th>").append(entry.getKey()).append("</th><td>").toString() );
                    // TODO We always assume that an inner map is for
                    // properties, correct?
                    append( builder, entry.getValue(), ObjectType.PROPERTIES );
                    builder.append( "</td></tr>\n" );
                }
                builder.append( "</table>\n" );
            }
            else
            {
                builder.append( new StringBuilder().append("<table class=\"").append(htmlClass).append("\"><caption>").toString() );
                builder.append( caption );
                builder.append( "</caption>" );
                builder.append( "<tr><td></td></tr>" );
                builder.append( "</table>" );
            }
        }
        else
        {
            builder.append( object != null ? embedInLinkIfClickable( object.toString() ) : "" );
        }
    }

    private static String embedInLinkIfClickable( String string )
    {
        // TODO Hardcode "http://" string?
        if ( string.startsWith( "http://" ) || string.startsWith( "https://" ) )
        {
            String anchoredString = new StringBuilder().append("<a href=\"").append(string).append("\"").toString();

            // TODO Hardcoded /node/, /relationship/ string?
            String anchorClass = null;
            if ( string.contains( "/node/" ) )
            {
                anchorClass = "node";
            }
            else if ( string.contains( "/relationship/" ) )
            {
                anchorClass = "relationship";
            }
            if ( anchorClass != null )
            {
                anchoredString += new StringBuilder().append(" class=\"").append(anchorClass).append("\"").toString();
            }
            anchoredString += new StringBuilder().append(">").append(escapeHtml( string )).append("</a>").toString();
            string = anchoredString;
        }
        else
        {
            string = escapeHtml( string );
        }
        return string;
    }

    private static String escapeHtml( final String string )
    {
        if ( string == null )
        {
            return null;
        }
        String res = string.replace( "&", "&amp;" );
        res = res.replace( "\"", "&quot;" );
        res = res.replace( "<", "&lt;" );
        res = res.replace( ">", "&gt;" );
        return res;
    }

    public enum ObjectType
    {
        NODE,
        RELATIONSHIP,
        PROPERTIES,
        ROOT,
        INDEX_ROOT,

        ;

        String getCaption()
        {
            return name().substring( 0, 1 )
                    .toUpperCase() + name().substring( 1 )
                    .toLowerCase();
        }

        String getHtmlClass()
        {
            return getCaption().toLowerCase();
        }
    }
}
