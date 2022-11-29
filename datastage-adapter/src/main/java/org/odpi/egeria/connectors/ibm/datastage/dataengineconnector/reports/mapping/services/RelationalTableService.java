/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.RelationalTableMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.openmetadata.accessservices.dataengine.model.RelationalTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mappings for creating a RelationalTable.
 */
public class RelationalTableService extends BaseService {
    private static final Logger log = LoggerFactory.getLogger(RelationalTableService.class);

    public RelationalTableService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a RelationalTable for the provided data store and field information.
     *
     * @param storeIdentity the store identity for which to create the virtual table
     *
     * @return RelationalTable
     */
    public RelationalTable getForDataStore(Identity storeIdentity) {
        if (storeIdentity == null) {
            return null;
        }
        String relationalTableQN = getFullyQualifiedName(storeIdentity, null);
        if (relationalTableQN != null) {
            log.debug("Constructing RelationalTable for data store: {}", relationalTableQN);
            return RelationalTableMapper.getRelationalTable(relationalTableQN, storeIdentity.getName());
        } else {
            log.error("Unable to determine identity of store: {}", storeIdentity);
            return null;
        }
    }

}

