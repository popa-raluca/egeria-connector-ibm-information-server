/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.SchemaTypeMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.openmetadata.accessservices.dataengine.model.Attribute;
import org.odpi.openmetadata.accessservices.dataengine.model.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Mappings for creating a SchemaType.
 */
public class SchemaTypeService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(SchemaTypeService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    public SchemaTypeService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a SchemaType for the provided data store and field information.
     *
     * @param storeIdentity the store identity for which to create the schema type
     *
     * @return SchemaType
     */
    public SchemaType getForDataStore(Identity storeIdentity) throws IGCException {
        if (storeIdentity == null) {
            return null;
        }
        String schemaTypeQN = getFullyQualifiedName(storeIdentity, null);
        if (schemaTypeQN != null) {
            log.debug("Constructing SchemaType for data store & fields: {}", schemaTypeQN);
            InformationAsset store = new InformationAsset();
            store.setId(storeIdentity.getRid());
            store.setType(storeIdentity.getAssetType());
            store.setName(storeIdentity.getName());
            List<Classificationenabledgroup> fields = cache.getFieldsForStore(store);
            AttributeService attributeService = new AttributeService(cache);
            List<Attribute> attributes = attributeService.getForDataStoreFields(fields);

            return SchemaTypeMapper.getSchemaType(schemaTypeQN, storeIdentity.getName(), attributes);
        } else {
            log.error("Unable to determine identity of store: {}", storeIdentity);
            return null;
        }
    }

    /**
     * Creates a SchemaType for the provided job and link information.
     *
     * @param link                    the link from which to retrieve stage columns for the SchemaType's attributes
     * @param job                     the job for which to create the SchemaType
     * @param fullyQualifiedStageName to ensure each attribute is unique
     *
     * @return SchemaType
     */
    SchemaType getForLink(Link link, DataStageJob job, String fullyQualifiedStageName) throws IGCException {
        if (link == null) {
            return null;
        }
        String schemaTypeQN = getFullyQualifiedName(link, fullyQualifiedStageName);
        if (schemaTypeQN != null) {
            log.debug("Constructing SchemaType for job & link: {}", schemaTypeQN);
            AttributeService attributeService = new AttributeService(cache);
            List<Attribute> attributes = attributeService.getForLink(link, job, fullyQualifiedStageName);
            return SchemaTypeMapper.getSchemaType(schemaTypeQN, link.getId(), link.getModifiedBy(), attributes);
        } else {
            log.error("Unable to determine identity of link: {}", link);
            return null;
        }
    }

    /**
     * Creates a SchemaType for the provided data store field information, for the provided stage.
     *
     * @param fields                  the fields from the data store to use in creating the SchemaType
     * @param storeIdentity           the store identity for which to create the SchemaType
     * @param stage                   the stage for which to create the SchemaType
     * @param fullyQualifiedStageName the fully-qualified name of the stage
     *
     * @return SchemaType
     */
    SchemaType getForDataStoreFields(List<Classificationenabledgroup> fields, Identity storeIdentity, Stage stage,
                                     String fullyQualifiedStageName) throws IGCException {
        if (stage == null) {
            return null;
        }
        String schemaTypeQN = getFullyQualifiedName(storeIdentity, fullyQualifiedStageName);
        if (schemaTypeQN != null) {
            log.debug("Constructing SchemaType for store & fields, within stage: {}", schemaTypeQN);
            AttributeService attributeService = new AttributeService(cache);
            List<Attribute> attributes = attributeService.getForDataStoreFields(fields, fullyQualifiedStageName);
            return SchemaTypeMapper.getSchemaType(schemaTypeQN, storeIdentity.getName(), stage.getModifiedBy(), attributes);

        } else {
            log.error("Unable to determine identity of store: {}", storeIdentity);
            return null;
        }
    }

    /**
     * Creates a SchemaType for the provided list of stage variables, for the provided stage.
     *
     * @param stageVariables          the stage variables to include as attributes in the SchemaType
     * @param job                     the job for which to create the Attributes
     * @param stage                   the stage for which to create the SchemaType
     * @param fullyQualifiedStageName the fully-qualified name of the stage
     *
     * @return SchemaType
     */
    SchemaType getForStageVariables(List<StageVariable> stageVariables, DataStageJob job, Stage stage, String fullyQualifiedStageName) throws
                                                                                                                                       IGCException {
        if (stage == null) {
            return null;
        }
        log.debug("Constructing SchemaType for stage variables of: {}", fullyQualifiedStageName);
        AttributeService attributeService = new AttributeService(cache);
        List<Attribute> attributes = attributeService.getForStageVariables(stageVariables, job, fullyQualifiedStageName);
        return SchemaTypeMapper.getSchemaType(fullyQualifiedStageName, stage.getName(), stage.getModifiedBy(), attributes);
    }
}
