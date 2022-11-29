/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Attribute;

/**
 * Mappings for creating an Attribute
 */
public class AttributeMapper {
    public static Attribute getAttribute(String qualifiedName, String displayName, String dataType, int position) {
        Attribute attribute = new Attribute();
        attribute.setQualifiedName(qualifiedName);
        attribute.setDisplayName(displayName);
        attribute.setDataType(dataType);
        attribute.setPosition(position);
        return  attribute;
    }

    public static Attribute getAttribute(String fullyQualifiedName, String name, String dataType, int position, String defaultValue) {
        Attribute attribute = getAttribute(fullyQualifiedName, name, dataType, position);
        attribute.setDefaultValue(defaultValue);
        return attribute;
    }
}
