/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.LineageMappingMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCConnectivityException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCIOException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCParsingException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.DataItem;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageColumn;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.ItemList;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Reference;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.interfaces.ColumnLevelLineage;
import org.odpi.openmetadata.accessservices.dataengine.model.LineageMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mappings for creating a set of LineageMappings.
 */
class LineageMappingsService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(LineageMappingsService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    LineageMappingsService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Creates LineageMappings for stages that have links as input and output.
     * - {@code STAGEB ( -> processing -> )}
     * - {@code DSLink1_STAGEB to DSLink2_STAGEB (INPUT_PORT to OUTPUT_PORT)}
     *
     * @param link                    the link for which to create the LineageMappings
     * @param job                     the job for which to create the LineageMappings
     * @param stageRid                the RID of the stage for which we are building mappings
     * @param knownLinks              set of known link RIDs
     * @param fullyQualifiedStageName the stage name for which to create the LineageMappings
     * @param bSource                 true if processing a source link, false if a target link
     *
     * @return {@code Set<LineageMapping>}
     */
    Set<LineageMapping> getForLinkInStage(Link link, DataStageJob job, String stageRid, Set<String> knownLinks, String fullyQualifiedStageName,
                                          boolean bSource) throws IGCException {
        Set<LineageMapping> lineageMappings = new HashSet<>();
        List<StageColumn> allStageColumns = job.getStageColumnsForLink(link.getId());
        log.debug("Constructing LineageMappings for stage columns: {}", allStageColumns);
        // For each stage column defined on the link...
        for (DataItem stageColumnRef : allStageColumns) {
            String colId = stageColumnRef.getId();
            ColumnLevelLineage stageColumnFull = job.getColumnLevelLineageByRid(colId);
            log.debug(" ... introspecting stage column: {}", stageColumnFull);
            String stageColumnFullQN = getFullyQualifiedName(stageColumnFull, fullyQualifiedStageName);
            if (stageColumnFullQN != null) {
                // The previous / next columns COULD refer to stage columns that are not actually part of the job's
                // input or output links (this seems to be the case with 'lookup' stages in particular). In such cases
                // the specific stage column that uses such a non-job-related link should be ignored (it will be
                // covered elsewhere by the job that actually DOES related to that link).
                if (!bSource) {
                    // Create a LineageMapping from each previous stage column to this stage column
                    lineageMappings.addAll(
                            getPreviousStageLineageMappings(job, stageRid, knownLinks, fullyQualifiedStageName, stageColumnFull, stageColumnFullQN));
                } else {
                    // Create a LineageMapping from this stage column to each next stage column
                    lineageMappings.addAll(
                            getNextStageLineageMappings(job, stageRid, knownLinks, fullyQualifiedStageName, stageColumnFull, stageColumnFullQN));
                }
            } else {
                log.error("Unable to determine identity for stage column -- not including (full was {}): {}",
                        stageColumnFull == null ? "null" : "non-null",
                        stageColumnRef);
            }
        }
        return lineageMappings;
    }

    /**
     * Creates LineageMappings between stages.
     * - {@code STAGEA (data source -> )}
     * - {@code DSLink1_STAGEA to DSLink1_STAGEB (cross-process mapping)}
     * - {@code STAGEB ( -> processing -> )}
     * - {@code DSLink2_STAGEB to DSLink2_STAGEC (cross-process mapping)}
     *
     * @param link the link for which to create the LineageMappings
     * @param job  the job for which to create the LineageMappings
     *
     * @return {@code Set<LineageMapping>}
     */
    Set<LineageMapping> getForLink(Link link, DataStageJob job) throws IGCException {
        Set<LineageMapping> lineageMappings = new HashSet<>();
        // Despite the plural name, a link can only have one input and one output stage so these are singular
        Stage inputStage = job.getStageByRid(link.getInputStages().getId());
        Stage outputStage = job.getStageByRid(link.getOutputStages().getId());

        List<StageColumn> allStageColumns = job.getStageColumnsForLink(link.getId());
        log.debug("Constructing LineageMappings between stages: {}", link);
        // For each stage column defined on the link...
        for (DataItem stageColRef : allStageColumns) {
            ColumnLevelLineage stageColFull = job.getColumnLevelLineageByRid(stageColRef.getId());
            String inputQN = getFullyQualifiedName(inputStage);
            String outputQN = getFullyQualifiedName(outputStage);
            String stageColNameIn = getFullyQualifiedName(stageColFull, inputQN);
            String stageColNameOut = getFullyQualifiedName(stageColFull, outputQN);
            if (stageColNameIn != null && stageColNameOut != null) {
                // Create a single mapping between the input stage and the output stage that use this link
                LineageMapping lineageMapping = LineageMappingMapper.getLineageMapping(stageColNameIn, stageColNameOut);
                lineageMappings.add(lineageMapping);
            } else {
                if (stageColNameIn == null) {
                    log.error("Unable to determine identity for input stage column -- not including (full was {}): {}",
                            stageColFull == null ? "null" : "non-null",
                            inputStage);
                }
                if (stageColNameOut == null) {
                    log.error("Unable to determine identity for output stage column -- not including (full was {}): {}",
                            stageColFull == null ? "null" : "non-null",
                            outputStage);
                }
            }
        }
        return lineageMappings;
    }

    /**
     * Creates LineageMappings at the job-level (only).
     * - {@code INPUTs -> Job (data source(s) -> process)}
     * - {@code Job -> OUTPUTs (process -> data target(s)}
     *
     * @param job the job for which to create the LineageMappings
     *
     * @return {@code Set<LineageMapping>}
     */
    Set<LineageMapping> getForJob(DataStageJob job) throws IGCException {
        Set<LineageMapping> lineageMappings = new HashSet<>();
        List<String> inputs = job.getInputStores();
        List<String> outputs = job.getOutputStores();

        String jobQN = getFullyQualifiedName(job.getJobObject());
        for (String rid : inputs) {
            Identity input = cache.getStoreIdentityFromRid(rid);
            String inputQualifiedName = this.getFullyQualifiedName(input, null);
            LineageMapping lineageMapping = LineageMappingMapper.getLineageMapping(inputQualifiedName, jobQN);
            lineageMappings.add(lineageMapping);
        }
        for (String rid : outputs) {
            Identity output = cache.getStoreIdentityFromRid(rid);
            String outputQualifiedName = this.getFullyQualifiedName(output, null);
            LineageMapping lineageMapping = LineageMappingMapper.getLineageMapping(jobQN, outputQualifiedName);
            lineageMappings.add(lineageMapping);
        }

        return lineageMappings;
    }

    /**
     * Creates LineageMappings between data stores and stages.
     * - {@code STAGEA (data source -> )}
     * - {@code StoreX to StoreX_STAGEA (reads_from_(design) to INPUT_PORT)}
     * - {@code StoreX_STAGEA to DSLink1_STAGEA (INPUT_PORT to OUTPUT_PORT)}
     * - {@code STAGEC ( -> data store)}
     * - {@code DSLink2_STAGEC to StoreY_STAGEC (INPUT_PORT to OUTPUT_PORT)}
     * - {@code StoreY_STAGEC to StoreY (OUTPUT_PORT to written_by_(design))}
     *
     * @param fields                  list of IGC field objects (data_file_field or database_column)
     * @param job                     the job for which to create the LineageMappings
     * @param stageRid                the RID of the stage for which we are building mappings
     * @param knownLinks              set of known link RIDs
     * @param bSource                 true if processing a source link, false if a target link
     * @param fullyQualifiedStageName the fully qualifiedName of the stage itself
     *
     * @return {@code Set<LineageMapping>}
     */
    Set<LineageMapping> getForDataStoreFieldsInStage(List<Classificationenabledgroup> fields,
                                                     DataStageJob job,
                                                     String stageRid,
                                                     Set<String> knownLinks,
                                                     boolean bSource,
                                                     String fullyQualifiedStageName) throws IGCException {
        Set<LineageMapping> lineageMappings = new HashSet<>();
        // For each field in the data store...
        if (fields != null) {
            for (Classificationenabledgroup fieldObj : fields) {
                String field1QN = getFullyQualifiedName(fieldObj);
                if (field1QN != null) {
                    // relatedStageCols could have more than just this stage's inputs / outputs (a given data store
                    // may be written or read by many stages)
                    ItemList<InformationAsset> relatedStageCols;
                    String propertyName;
                    if (bSource) {
                        relatedStageCols = fieldObj.getReadByDesign();
                        propertyName = "read_by_(design)";
                    } else {
                        relatedStageCols = fieldObj.getWrittenByDesign();
                        propertyName = "written_by_(design)";
                    }
                    log.debug("Constructing LineageMappings between store field and stages' {}: {}", bSource ? "source" : "target", fieldObj);
                    if (relatedStageCols != null) {
                        List<InformationAsset> allRelatedStageCols = igcRestClient.getAllPages(propertyName, relatedStageCols);
                        // For each object that reads / writes to that field...
                        for (InformationAsset stageColRef : allRelatedStageCols) {
                            ColumnLevelLineage stageColFull = job.getColumnLevelLineageByRid(stageColRef.getId());
                            // Limit the details we capture to only the stage we're processing
                            if (isStageColumnForKnownLink(stageColFull, stageRid, knownLinks)) {
                                String field1EmbeddedQN = getFullyQualifiedName(fieldObj, fullyQualifiedStageName);
                                String field2EmbeddedQN = getFullyQualifiedName(stageColFull, fullyQualifiedStageName);
                                if (bSource) {
                                    // StoreX to StoreX_STAGEA (reads_from_(design) to INPUT_PORT)
                                    LineageMapping oneToOne = LineageMappingMapper.getLineageMapping(field1QN, field1EmbeddedQN);
                                    lineageMappings.add(oneToOne);
                                    // StoreX_STAGEA to DSLink1_STAGEA (INPUT_PORT to OUTPUT_PORT)
                                    LineageMapping portToPort = LineageMappingMapper.getLineageMapping(field1EmbeddedQN, field2EmbeddedQN);
                                    lineageMappings.add(portToPort);
                                } else {
                                    // DSLink2_STAGEC to StoreY_STAGEC (INPUT_PORT to OUTPUT_PORT)
                                    LineageMapping portToPort = LineageMappingMapper.getLineageMapping(field2EmbeddedQN, field1EmbeddedQN);
                                    lineageMappings.add(portToPort);
                                    // StoreY_STAGEC to StoreY (OUTPUT_PORT to written_by_(design))
                                    LineageMapping oneToOne = LineageMappingMapper.getLineageMapping(field1EmbeddedQN, field1QN);
                                    lineageMappings.add(oneToOne);
                                }
                            } else {
                                log.debug("Found a stage column for a link not listed as a known link for this stage -- ignoring: {}", stageColFull);
                            }
                        }
                    } else {
                        log.info("No fields were found for lineage mapping of: {}", fieldObj);
                    }
                } else {
                    log.error("Unable to determine identity for field -- not including: {}", fieldObj);
                }
            }
        } else {
            log.warn("No fields were found for a data store for stage: {}", fullyQualifiedStageName);
        }
        return lineageMappings;
    }

    /**
     * Creates LineageMappings between stage variables and columns (should all be mappings that are internal to the
     * stage).
     * - {@code previous_stage_columns -> this stage variable }
     * - {@code this stage variable -> next_stage_columns }
     *
     * @param stageVariables          list of stage variables
     * @param job                     the job for which to create the LineageMappings
     * @param fullyQualifiedStageName the fully qualifiedName of the stage itself
     *
     * @return {@code Set<LineageMapping>}
     */
    Set<LineageMapping> getForStageVariables(List<StageVariable> stageVariables,
                                             DataStageJob job, String fullyQualifiedStageName) throws IGCException {
        final String methodName = "getForStageVariables";
        Set<LineageMapping> lineageMappings = new HashSet<>();
        // For each stage variable...
        if (stageVariables != null) {
            for (StageVariable varObj : stageVariables) {
                ColumnLevelLineage stageVar = job.getColumnLevelLineageByRid(varObj.getId());
                String stageVarQN = getFullyQualifiedName(stageVar, fullyQualifiedStageName);
                if (stageVarQN != null) {
                    ItemList<DataItem> previousStageColumns = varObj.getPreviousStageColumns();
                    List<DataItem> inputs = igcRestClient.getAllPages("previous_stage_columns", previousStageColumns);
                    for (DataItem input : inputs) {
                        ColumnLevelLineage columnFull = job.getColumnLevelLineageByRid(input.getId());
                        String inputColumnFullQN = getFullyQualifiedName(columnFull, fullyQualifiedStageName);
                        LineageMapping inbound = LineageMappingMapper.getLineageMapping(inputColumnFullQN, stageVarQN);
                        lineageMappings.add(inbound);
                    }
                    ItemList<DataItem> nextStageColumns = varObj.getNextStageColumns();
                    List<DataItem> outputs = igcRestClient.getAllPages("next_stage_columns", nextStageColumns);
                    for (DataItem output : outputs) {
                        ColumnLevelLineage columnFull = job.getColumnLevelLineageByRid(output.getId());
                        String outputColumnFullQN = getFullyQualifiedName(columnFull, fullyQualifiedStageName);
                        LineageMapping outbound = LineageMappingMapper.getLineageMapping(stageVarQN, outputColumnFullQN);
                        lineageMappings.add(outbound);
                    }
                } else {
                    log.error("Unable to determine identity for stage variable -- not including: {}", varObj);
                }
            }
        } else {
            log.warn("No fields were found for a data store for stage: {}", fullyQualifiedStageName);
        }
        return lineageMappings;
    }

    private List<LineageMapping> getNextStageLineageMappings(DataStageJob job, String stageRid, Set<String> knownLinks,
                                                             String fullyQualifiedStageName,
                                                             ColumnLevelLineage stageColumnFull, String stageColumnFullQN) throws
                                                                                                                           IGCConnectivityException,
                                                                                                                           IGCParsingException,
                                                                                                                           IGCIOException {
        ItemList<DataItem> nextColumns = stageColumnFull.getNextStageColumns();
        List<DataItem> allNextColumns = igcRestClient.getAllPages("next_stage_columns", nextColumns);
        log.debug(" ...... iterating through next columns: {}", allNextColumns);
        List<LineageMapping> lineageMappings = new ArrayList<>();
        for (DataItem nextColumnRef : allNextColumns) {
            ColumnLevelLineage nextColumnFull = job.getColumnLevelLineageByRid(nextColumnRef.getId());
            if (isStageColumnForKnownLink(nextColumnFull, stageRid, knownLinks)) {
                String nextColumnFullQN = getFullyQualifiedName(nextColumnFull, fullyQualifiedStageName);
                if (nextColumnFullQN != null) {
                    LineageMapping lineageMapping = LineageMappingMapper.getLineageMapping(stageColumnFullQN, nextColumnFullQN);
                    lineageMappings.add(lineageMapping);
                } else {
                    log.error("Unable to determine identity for next column -- not including (full was {}): {}",
                            nextColumnFull == null ? "null" : "non-null",
                            nextColumnRef);
                }
            } else {
                log.warn("Found a stage column for a link not listed as an output link for this stage -- ignoring: {}", nextColumnFull);
            }
        }
        return lineageMappings;
    }

    private List<LineageMapping> getPreviousStageLineageMappings(DataStageJob job, String stageRid, Set<String> knownLinks,
                                                                 String fullyQualifiedStageName, ColumnLevelLineage stageColumnFull,
                                                                 String stageColumnFullQN) throws
                                                                                           IGCConnectivityException,
                                                                                           IGCParsingException,
                                                                                           IGCIOException {
        ItemList<DataItem> previousColumns = stageColumnFull.getPreviousStageColumns();
        List<DataItem> allPreviousColumns = igcRestClient.getAllPages("previous_stage_columns", previousColumns);
        log.debug(" ...... iterating through previous columns: {}", allPreviousColumns);
        List<LineageMapping> lineageMappings = new ArrayList<>();
        for (DataItem previousColumnRef : allPreviousColumns) {
            ColumnLevelLineage previousColumnFull = job.getColumnLevelLineageByRid(previousColumnRef.getId());
            if (isStageColumnForKnownLink(previousColumnFull, stageRid, knownLinks)) {
                String previousColumnFullQN = getFullyQualifiedName(previousColumnFull, fullyQualifiedStageName);
                if (previousColumnFullQN != null) {
                    LineageMapping lineageMapping = LineageMappingMapper.getLineageMapping(previousColumnFullQN, stageColumnFullQN);
                    lineageMappings.add(lineageMapping);
                } else {
                    log.error("Unable to determine identity for previous column -- not including (full was {}): {}",
                            previousColumnFull == null ? "null" : "non-null",
                            previousColumnRef);
                }
            } else {
                log.warn("Found a stage column for a link not listed as an input link for this stage -- ignoring: {}", previousColumnFull);
            }
        }
        return lineageMappings;
    }

    /**
     * Determine whether the provided stage column is part of a known input / output link for the particular
     * stage for which we are generating a lineage mapping.
     *
     * @param column     the stage column to check whether it is part of a known link
     * @param stageRid   the RID of the stage in which to check for known-ness
     * @param knownLinks set of RIDs of links known as inputs / outputs from the stage
     *
     * @return boolean
     */
    private boolean isStageColumnForKnownLink(ColumnLevelLineage column, String stageRid, Set<String> knownLinks) {
        if (stageRid == null) {
            log.error("Unable to verify stage column as stage is null -- skipping: {}", column);
            return false;
        }
        List<Reference> context = column.getContext();
        Reference link = context.get(context.size() - 1);
        if (link.getType().equals("link")) {
            return knownLinks.contains(link.getId());
        } else if (link.getType().equals("stage")) {
            return stageRid.equals(link.getId());
        } else {
            log.warn("Unknown parent type '{}' for column-level lineage -- skipping: {}", link.getType(), column);
            return false;
        }
    }
}
