/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Process;

/**
 * Mappings for creating an Attribute
 */
public class ProcessMapper {
    public static Process getProcess(String qualifiedName, String displayName, String name, String description, String owner) {
        Process process = new Process();
        process.setQualifiedName(qualifiedName);
        process.setDisplayName(displayName);
        process.setName(name);
        process.setDescription(description);
        process.setOwner(owner);
        return process;
    }
}
