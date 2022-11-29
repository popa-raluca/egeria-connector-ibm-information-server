/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Attribute;
import org.odpi.openmetadata.accessservices.dataengine.model.SchemaType;

import java.util.List;

/**
 * Mappings for creating a set of Attributes.
 */
public class SchemaTypeMapper {
    public static SchemaType getSchemaType(String qualifiedName, String displayName, List<Attribute> attributeList) {
        SchemaType schemaType = new SchemaType();
        schemaType.setQualifiedName(qualifiedName);
        schemaType.setDisplayName(displayName);
        schemaType.setAttributeList(attributeList);

        return schemaType;
    }

    public static SchemaType getSchemaType(String qualifiedName, String displayName, String author, List<Attribute> attributes) {
       SchemaType schemaType = getSchemaType(qualifiedName, displayName, attributes);
       schemaType.setAuthor(author);

       return  schemaType;
    }
}
