/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Database;
import org.odpi.openmetadata.accessservices.dataengine.model.DatabaseSchema;

/**
 * Mappings for creating an Attribute
 */
public class DatabaseSchemaMapper {

    public static DatabaseSchema getDatabaseSchema(String qualifiedName, String displayName) {
        DatabaseSchema databaseSchema = new DatabaseSchema();
        databaseSchema.setQualifiedName(qualifiedName);
        databaseSchema.setDisplayName(displayName);
        return databaseSchema;
    }
}
