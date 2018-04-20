package org.apache.rya.indexing.accumulo.geo;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.log4j.Logger;
import org.apache.rya.indexing.GeoConstants;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.xml.sax.SAXException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * 
 * parsing RDF oriented gml and well known text (WKT) into a geometry 
 * This is abstract because of its depenendence on geo tools.  
 * Your implementation can use whatever version you like.
 */
public class GeoParseUtils {
    static final Logger logger = Logger.getLogger(GeoParseUtils.class);
    /**
     * @deprecated  Not needed since geo literals may be WKT or GML.
     *
     *    This method warns on a condition that must already be tested.  Replaced by
     *    {@link #getLiteral(Statement)} and {@link #getGeometry(Statement}
     *    and getLiteral(statement).toString()
     *    and getLiteral(statement).getDatatype()
     */
    @Deprecated
	public static String getWellKnownText(final Statement statement) throws ParseException {
	    final Literal lit = getLiteral(statement);
	    if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
	        logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
	    }
	    return lit.getLabel().toString();
	}

    public static Literal getLiteral(final Statement statement) throws ParseException {
        final Value v = statement.getObject();
        if (!(v instanceof Literal)) {
            throw new ParseException("Statement does not contain Literal: " + statement.toString());
        }
        final Literal lit = (Literal) v;
        return lit;
    }

    /**
     * Parse GML/wkt literal to Geometry
     *
     * @param statement
     * @return
     * @throws ParseException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static Geometry getGeometry(final Statement statement, GmlToGeometryParser gmlToGeometryParser) throws ParseException {
        // handle GML or WKT
        final Literal lit = getLiteral(statement);
        if (GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
            final String wkt = lit.getLabel().toString();
            return (new WKTReader()).read(wkt);
        } else if (GeoConstants.XMLSCHEMA_OGC_GML.equals(lit.getDatatype())) {
            final String gml = lit.getLabel().toString();
            try {
                return getGeometryGml(gml, gmlToGeometryParser);
            } catch (IOException | SAXException | ParserConfigurationException e) {
                throw new ParseException(e);
            }
        } else {
            throw new ParseException("Literal is unknown geo type, expecting WKT or GML: " + statement.toString());
        }
    }
    /**
     * Convert GML/XML string into a geometry that can be indexed.
     * @param gmlString
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public static Geometry getGeometryGml(final String gmlString, final GmlToGeometryParser gmlToGeometryParser) throws IOException, SAXException, ParserConfigurationException {
        final Reader reader = new StringReader(gmlString);
        final Geometry geometry = gmlToGeometryParser.parse(reader);
        // This sometimes gets populated with the SRS/CRS: geometry.getUserData()
        // Always returns 0 : geometry.getSRID()
        //TODO geometry.setUserData(some default CRS); OR geometry.setSRID(some default CRS)

        return geometry;
    }


    /**
     * Extracts the arguments used in a {@link FunctionCall}.
     * @param matchName - The variable name to match to arguments used in the {@link FunctionCall}.
     * @param call - The {@link FunctionCall} to match against.
     * @return - The {@link Value}s matched.
     */
    public static Object[] extractArguments(final String matchName, final FunctionCall call) {
        final Object[] args = new Object[call.getArgs().size() - 1];
        int argI = 0;
        for (int i = 0; i != call.getArgs().size(); ++i) {
            final ValueExpr arg = call.getArgs().get(i);
            if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName())) {
                continue;
            }
            if (arg instanceof ValueConstant) {
                args[argI] = ((ValueConstant)arg).getValue();
            } else if (arg instanceof Var && ((Var)arg).hasValue()) {
                args[argI] = ((Var)arg).getValue();
            } else {
                args[argI] = arg;
            }
            ++argI;
        }
        return args;
    }

	/**
	 * Wrap the geotools or whatever parser.
	 */
	public interface GmlToGeometryParser {
		/**
		 * Implemented code should look like this: 
		 *     import org.geotools.gml3.GMLConfiguration;
		 *     import org.geotools.xml.Parser;
		 * 	   final GmlToGeometryParser gmlParser = new GmlToGeometryParser(new GMLConfiguration()); return (Geometry)
		 *     gmlParser.parse(reader);
		 * @param reader
		 *            contains the gml to parse. use StringReader to adapt.
		 * @return a JTS geometry
		 * @throws ParserConfigurationException 
		 * @throws SAXException 
		 * @throws IOException 
		 * 
		 */
		public abstract Geometry parse(final Reader reader) throws IOException, SAXException, ParserConfigurationException;
	
	}
	
}
