package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.apache.commons.collections4.CollectionUtils;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.AttributeMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.DataItem;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageColumn;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.ItemList;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.interfaces.ColumnLevelLineage;
import org.odpi.openmetadata.accessservices.dataengine.model.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AttributeService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(AttributeService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    public AttributeService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates a set of Attributes for the provided job and link information.
     *
     * @param link                    the link containing stage column detail for the Attributes
     * @param job                     the job for which to create the attributes
     * @param fullyQualifiedStageName to ensure each attribute is unique
     *
     * @return {@code List<Attribute>}
     */
    List<Attribute> getForLink(Link link, DataStageJob job, String fullyQualifiedStageName) throws IGCException {
        log.debug("Creating new Attributes from job and link...");
        List<Attribute> attributes = new ArrayList<>();
        if (link != null) {
            List<StageColumn> allStageColumns = job.getStageColumnsForLink(link.getId());
            int index = 0;
            for (DataItem stageColumn : allStageColumns) {
                log.debug("... calculating from detailed stage column: {}", stageColumn);
                String colId = stageColumn.getId();
                ColumnLevelLineage stageColumnObj = job.getColumnLevelLineageByRid(colId);
                String stageColumnQN = getFullyQualifiedName(stageColumnObj);
                if (stageColumnQN != null) {
                    attributes.add(AttributeMapper.getAttribute(getFullyQualifiedName(stageColumnObj, fullyQualifiedStageName),
                            stageColumnObj.getName(), stageColumnObj.getOdbcType(), index++));
                } else {
                    log.error("Unable to determine identity for linked column -- not including it: {}", stageColumn);
                }
            }
        }
        return attributes;
    }

    /**
     * Creates a set of Attributes for the provided data store field information.
     *
     * @param fields                  the data store fields containing detail for the Attributes
     * @param fullyQualifiedStageName the qualified name of the stage to ensure each attribute is unique
     *
     * @return {@code List<Attribute>}
     */
    List<Attribute> getForDataStoreFields(List<Classificationenabledgroup> fields, String fullyQualifiedStageName) throws IGCException {
        log.debug("Creating new Attributes from job and fields...");
        List<Attribute> attributes = new ArrayList<>();
        if (fields != null && !fields.isEmpty()) {
            for (Classificationenabledgroup field : fields) {
                String fieldQN = getFullyQualifiedName(field);
                if (fieldQN != null) {
                    String dataType = field.getDataType() != null ? field.getDataType() : field.getOdbcType();
                    int position = field.getPosition() != null ? field.getPosition().intValue() : 0;
                    attributes.add(AttributeMapper.getAttribute(getFullyQualifiedName(field, fullyQualifiedStageName), field.getName(),
                            dataType, position, field.getDefaultValue()));
                } else {
                    log.error("Unable to determine identity for field -- not including it: {}", field);
                }
            }
        }
        return attributes;
    }

    /**
     * Creates a set of Attributes for the provided data store field information (for virtual assets).
     *
     * @param fields the data store fields containing detail for the Attributes
     *
     * @return {@code List<Attribute>}
     */
    List<Attribute> getForDataStoreFields(List<Classificationenabledgroup> fields) throws IGCException {
        return getForDataStoreFields(fields, null);
    }

    /**
     * Creates a set of Attributes for the provided stage variable information.
     *
     * @param stageVariables          for which to create a set of attributes
     * @param job                     the job for which to create the Attributes
     * @param fullyQualifiedStageName (always empty)
     *
     * @return {@code List<Attribute>}
     */
    List<Attribute> getForStageVariables(List<StageVariable> stageVariables, DataStageJob job,
                                         String fullyQualifiedStageName) throws IGCException {
        log.debug("Creating new Attributes from stage variables...");
        List<Attribute> attributes = new ArrayList<>();
        if (stageVariables != null && !stageVariables.isEmpty()) {
            for (StageVariable var : stageVariables) {
                ColumnLevelLineage stageVar = job.getColumnLevelLineageByRid(var.getId());
                String varQN = getFullyQualifiedName(stageVar, fullyQualifiedStageName);
                if (varQN != null) {
                    attributes.add(AttributeMapper.getAttribute(varQN, var.getName(), var.getOdbcType(), 0));
                } else {
                    log.error("Unable to determine identity for variable -- not including it: {}", var);
                }
            }
        }
        return attributes;
    }
}
