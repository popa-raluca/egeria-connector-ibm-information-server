/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.DataFile;
import org.odpi.openmetadata.accessservices.dataengine.model.RelationalTable;

/**
 * Mappings for creating an Attribute
 */
public class RelationalTableMapper {
    public static RelationalTable getRelationalTable(String qualifiedName, String displayName) {
        RelationalTable relationalTable = new RelationalTable();
        relationalTable.setQualifiedName(qualifiedName);
        relationalTable.setDisplayName(displayName);
        return relationalTable;
    }
}
