/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.DataFileMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.openmetadata.accessservices.dataengine.model.DataFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mappings for creating a DataFile.
 */
public class DataFileService extends BaseService {
    private static final Logger log = LoggerFactory.getLogger(DataFileService.class);

    public DataFileService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a DataFile for the provided data store and field information.
     *
     * @param storeIdentity the store identity for which to create the data file
     *
     * @return DataFile
     */
    DataFile getForDataStore(Identity storeIdentity) {
        if (storeIdentity == null) {
            return null;
        }

        String qualifiedName = getFullyQualifiedName(storeIdentity, null);
        if (qualifiedName != null) {
            log.debug("Constructing DataFile for data store: {}", qualifiedName);
            return DataFileMapper.getDataFile(qualifiedName, storeIdentity.getName());
        } else {
            log.error("Unable to determine identity of store: {}", storeIdentity);
            return null;
        }
    }
}
