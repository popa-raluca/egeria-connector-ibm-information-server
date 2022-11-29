/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.PortAlias;
import org.odpi.openmetadata.accessservices.dataengine.model.PortType;

/**
 * Mappings for creating an Attribute
 */
public class PortAliasMapper {
    public static PortAlias getPortAlias(String qualifiedName, String displayName, PortType portType, String delegatesTo) {
        PortAlias portAlias = new PortAlias();
        portAlias.setQualifiedName(qualifiedName);
        portAlias.setDisplayName(displayName);
        portAlias.setPortType(portType);
        portAlias.setDelegatesTo(delegatesTo);
        return portAlias;
    }
}
