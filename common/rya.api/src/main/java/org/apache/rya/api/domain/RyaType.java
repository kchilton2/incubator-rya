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
package org.apache.rya.api.domain;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

/**
 * Base Rya Type
 * Date: 7/16/12
 * Time: 11:45 AM
 */
public class RyaType implements Comparable {

    private IRI dataType;
    private String data;

    public RyaType() {
        setDataType(XMLSchema.STRING);
    }

    public RyaType(final String data) {
        this(XMLSchema.STRING, data);
    }


    public RyaType(final IRI dataType, final String data) {
        setDataType(dataType);
        setData(data);
    }

    /**
     * TODO: Can we get away without using the RDF4J IRI
     *
     * @return
     */
    public IRI getDataType() {
        return dataType;
    }

    public String getData() {
        return data;
    }

    public void setDataType(final IRI dataType) {
        this.dataType = dataType;
    }

    public void setData(final String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("RyaType");
        sb.append("{dataType=").append(dataType);
        sb.append(", data='").append(data).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Determine equality based on string representations of data and datatype.
     * @param o The object to compare with
     * @return true if the other object is also a RyaType and both data and datatype match.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof RyaType)) {
            return false;
        }
        final RyaType other = (RyaType) o;
        final EqualsBuilder builder = new EqualsBuilder()
                .append(getData(), other.getData())
                .append(getDataType(), other.getDataType());
        return builder.isEquals();
    }

    /**
     * Generate a hash based on the string representations of both data and datatype.
     * @return A hash consistent with equals.
     */
    @Override
    public int hashCode() {
        int result = dataType != null ? dataType.hashCode() : 0;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    /**
     * Define a natural ordering based on data and datatype.
     * @param o The object to compare with
     * @return 0 if both the data string and the datatype string representation match between the objects,
     *          where matching is defined by string comparison or both being null;
     *          Otherwise, an integer whose sign yields a consistent ordering.
     */
    @Override
    public int compareTo(final Object o) {
        int result = -1;
        if (o != null && o instanceof RyaType) {
            result = 0;
            final RyaType other = (RyaType) o;
            if (this.data != other.data) {
                if (this.data == null) {
                    return 1;
                }
                if (other.data == null) {
                    return -1;
                }
                result = this.data.compareTo(other.data);
            }
            if (result == 0 && this.dataType != other.dataType) {
                if (this.dataType == null) {
                    return 1;
                }
                if (other.dataType == null) {
                    return -1;
                }
                result = this.dataType.toString().compareTo(other.dataType.toString());
            }
        }
        return result;
    }
}
