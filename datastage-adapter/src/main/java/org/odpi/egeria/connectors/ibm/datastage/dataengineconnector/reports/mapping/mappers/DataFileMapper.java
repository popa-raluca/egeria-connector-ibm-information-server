/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.DataFile;

/**
 * Mappings for creating an Attribute
 */
public class DataFileMapper {
    public static DataFile getDataFile(String qualifiedName, String displayName) {
        DataFile dataFile = new DataFile();
        dataFile.setQualifiedName(qualifiedName);
        dataFile.setDisplayName(displayName);
        return dataFile;
    }
}
