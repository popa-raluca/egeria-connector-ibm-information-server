/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.PortImplementation;
import org.odpi.openmetadata.accessservices.dataengine.model.PortType;
import org.odpi.openmetadata.accessservices.dataengine.model.SchemaType;

/**
 * Mappings for creating an Attribute
 */
public class PortImplementationMapper {
    public static PortImplementation getPortImplementation(String qualifiedName, String displayName, PortType portType, SchemaType schemaType) {
        PortImplementation portImplementation = new PortImplementation();
        portImplementation.setQualifiedName(qualifiedName);
        portImplementation.setDisplayName(displayName);
        portImplementation.setPortType(portType);
        portImplementation.setSchemaType(schemaType);
        return portImplementation;
    }
}
