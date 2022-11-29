/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.CollectionMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Reference;
import org.odpi.openmetadata.accessservices.dataengine.model.Collection;

/**
 * Mappings for creating a TransformationObject.
 */
public class CollectionService extends BaseService {

    private static final String TRANSFORMATION_PROJECT_KEY = "transformation_project";

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    CollectionService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Retrieves the transformation project from an asset's context and returns it or null if it does not exists
     *
     * @param igcObj the asset for which to obtain the transformation project
     *
     * @return Collection
     */
    Collection getCollection(InformationAsset igcObj) throws IGCException {
        if (igcObj == null) {
            return null;
        }

        for (Reference reference : igcObj.getContext()) {
            if (TRANSFORMATION_PROJECT_KEY.equals(reference.getType())) {
                String qualifiedName = getFullyQualifiedName(reference);
                return CollectionMapper.getCollection(qualifiedName, reference.getName());
            }
        }
        return null;
    }
}
