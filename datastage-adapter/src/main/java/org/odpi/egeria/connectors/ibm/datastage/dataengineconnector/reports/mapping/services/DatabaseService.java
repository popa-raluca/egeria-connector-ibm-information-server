/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.DatabaseMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.openmetadata.accessservices.dataengine.model.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mappings for creating a Database.
 */
public class DatabaseService extends BaseService {
    private static final Logger log = LoggerFactory.getLogger(DatabaseService.class);

    public DatabaseService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a Database for the provided data store and field information.
     *
     * @param storeIdentity the store identity for which to create the database
     *
     * @return Database
     */
    Database getForDataStore(Identity storeIdentity) {
        if (storeIdentity == null) {
            return null;
        }
        String qualifiedName = getFullyQualifiedName(storeIdentity, null);
        if (qualifiedName != null) {
            log.debug("Constructing Database for data store: {}", qualifiedName);
            return DatabaseMapper.getDatabase(qualifiedName, storeIdentity.getName());
        } else {
            log.error("Unable to determine identity of store: {}", storeIdentity);
            return null;
        }
    }

}
