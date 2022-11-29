/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.apache.commons.collections4.CollectionUtils;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.PortImplementationMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.openmetadata.accessservices.dataengine.model.PortImplementation;
import org.odpi.openmetadata.accessservices.dataengine.model.PortType;
import org.odpi.openmetadata.accessservices.dataengine.model.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Mappings for creating a PortImplementation.
 */
class PortImplementationService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(PortImplementationService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    PortImplementationService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a PortImplementation for the provided job and link information.
     *
     * @param link                    the link containing details for the PortImplementation
     * @param job                     the job for which to create the PortImplementation
     * @param portType                the port type to use
     * @param fullyQualifiedStageName to ensure each schema is unique
     *
     * @return PortImplementation
     */
    PortImplementation getForLink(Link link, DataStageJob job, PortType portType, String fullyQualifiedStageName) throws IGCException {
        if (link == null) {
            return null;
        }

        String linkQN = getFullyQualifiedName(link, fullyQualifiedStageName);
        if (linkQN != null) {
            log.debug("Constructing PortImplementation for: {}", linkQN);
            SchemaTypeService schemaTypeService = new SchemaTypeService(cache);
            SchemaType schemaType = schemaTypeService.getForLink(link, job, fullyQualifiedStageName);
            return PortImplementationMapper.getPortImplementation(linkQN, link.getName(), portType, schemaType);
        } else {
            log.error("Unable to determine identity for link -- not including: {}", link);
            return null;
        }
    }

    /**
     * Creates a PortImplementation for the provided data store field information, for the given stage.
     *
     * @param fields                  the data store fields from which to create the PortImplementation's schema
     * @param stage                   the stage for which to create the PortImplementation
     * @param portType                the port type to use
     * @param fullyQualifiedStageName the qualified name of the stage to ensure each schema is unique
     *
     * @return PortImplementation
     */
    PortImplementation getForDataStoreFields(List<Classificationenabledgroup> fields, Stage stage,
                                             PortType portType, String fullyQualifiedStageName) throws IGCException {
        if (stage != null && CollectionUtils.isNotEmpty(fields)) {
            Identity storeIdentity = getParentIdentity(fields.get(0));
            String storeName = getParentDisplayName(fields.get(0));
            SchemaTypeService schemaTypeService = new SchemaTypeService(cache);
            SchemaType schemaType = schemaTypeService.getForDataStoreFields(fields, storeIdentity, stage, fullyQualifiedStageName);
            return PortImplementationMapper.getPortImplementation(getFullyQualifiedName(storeIdentity, fullyQualifiedStageName), storeName, portType,
                    schemaType);
        }
        return null;
    }

    /**
     * Creates a PortImplementation for the provided stage variable information, for the given stage.
     *
     * @param stageVariables          the stage variables to include in the PortImplementation
     * @param job                     the job for which to create the Attributes
     * @param stage                   the stage for which to create the PortImplementation
     * @param fullyQualifiedStageName the qualified name of the stage to ensure the schema is unique
     *
     * @return PortImplementation
     */
    PortImplementation getForStageVariables(List<StageVariable> stageVariables, DataStageJob job, Stage stage, String fullyQualifiedStageName) throws
                                                                                                                                               IGCException {
        if (stage != null) {
            SchemaTypeService schemaTypeService = new SchemaTypeService(cache);
            SchemaType schemaType = schemaTypeService.getForStageVariables(stageVariables, job, stage, fullyQualifiedStageName);

            return PortImplementationMapper.getPortImplementation(fullyQualifiedStageName, stage.getName(), PortType.OTHER, schemaType);
        }
        return null;
    }
}
