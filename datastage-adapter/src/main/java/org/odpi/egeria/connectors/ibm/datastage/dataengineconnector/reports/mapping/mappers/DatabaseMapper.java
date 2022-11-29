/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Attribute;
import org.odpi.openmetadata.accessservices.dataengine.model.Database;

/**
 * Mappings for creating an Attribute
 */
public class DatabaseMapper {

    public static Database getDatabase(String qualifiedName, String displayName) {
        Database database = new Database();
        database.setQualifiedName(qualifiedName);
        database.setDisplayName(displayName);

        return database;
    }
}
