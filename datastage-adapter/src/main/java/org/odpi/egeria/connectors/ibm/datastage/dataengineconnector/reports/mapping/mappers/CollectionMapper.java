/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers;

import org.odpi.openmetadata.accessservices.dataengine.model.Collection;

/**
 * Mappings for creating an Attribute
 */
public class CollectionMapper {
    public static Collection getCollection(String qualifiedName, String name) {
        Collection collection = new Collection();
        collection.setQualifiedName(qualifiedName);
        collection.setName(name);

        return collection;
    }
}
