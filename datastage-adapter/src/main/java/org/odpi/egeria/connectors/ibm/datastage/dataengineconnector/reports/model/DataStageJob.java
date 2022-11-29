/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model;

import org.apache.commons.collections4.CollectionUtils;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.DataStageConstants;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.IGCRestClient;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCConnectivityException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCParsingException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Dsjob;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageColumn;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.ItemList;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Reference;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.interfaces.ColumnLevelLineage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Class for interacting with DataStage Job objects.
 */
public class DataStageJob {

    private static final Logger log = LoggerFactory.getLogger(DataStageJob.class);

    private IGCRestClient igcRestClient;
    private Dsjob job;
    private JobType type;
    private final Map<String, Stage> stageMap;
    private final Map<String, Link> linkMap;
    private final Map<String, StageColumn> columnMap;
    private final Map<String, StageVariable> varMap;
    private final Map<String, Set<String>> stageToVarsMap;
    private final Map<String, Set<String>> linkToColumnsMap;
    private final List<String> inputStoreRIDs;
    private final List<String> outputStoreRIDs;
    private final List<String> inputStageRIDs;
    private final List<String> outputStageRIDs;

    public enum JobType {
        JOB, SEQUENCE
    }

    /**
     * Create a new detailed DataStage job object.
     *
     * @param cache cache and connectivity to the IGC environment
     * @param job   the job for which to retrieve details
     */
    public DataStageJob(DataStageReportsCache cache, Dsjob job) throws IGCException {

        this.igcRestClient = cache.getIgcRestClient();
        this.job = job;
        this.type = job.getType().equals("sequence_job") ? JobType.SEQUENCE : JobType.JOB;
        this.stageMap = new TreeMap<>();
        this.linkMap = new TreeMap<>();
        this.columnMap = new TreeMap<>();
        this.varMap = new TreeMap<>();
        this.stageToVarsMap = new TreeMap<>();
        this.linkToColumnsMap = new TreeMap<>();
        this.inputStoreRIDs = new ArrayList<>();
        this.outputStoreRIDs = new ArrayList<>();
        this.inputStageRIDs = new ArrayList<>();
        this.outputStageRIDs = new ArrayList<>();

        log.debug("Retrieving job details for: {}", job.getId());

        if (cache.getMode() == LineageMode.GRANULAR) {
            loadStageDetailsForJob();
            loadLinkDetailsForJob();
            loadStageVariablesForJob();
            loadStageColumnDetailsForLinks();
        }
        loadDataAssets(cache);
    }

    public Stage getStageByRid(String rid) {
        return stageMap.getOrDefault(rid, null);
    }

    /**
     * Retrieve a listing of the stages within this particular DataStage job.
     */
    private void loadStageDetailsForJob() throws IGCException {
        String jobRid = job.getId();
        log.debug("Retrieving stage details for job: {}", jobRid);
        List<Stage> allStages = igcRestClient.getStages(jobRid, DataStageConstants.getStageSearchProperties());
        for (Stage stage : allStages) {
            stageToVarsMap.put(stage.getId(), new TreeSet<>());
        }
        buildMap(stageMap, allStages);
        loadStages(stageMap.values());
    }

    /**
     * Retrieve a listing of the links within this particular DataStage job.
     */
    private void loadLinkDetailsForJob() throws IGCException {
        String jobRid = job.getId();
        log.debug("Retrieving link details for job: {}", jobRid);
        List<Link> allPages = igcRestClient.getLinks(jobRid, DataStageConstants.getLinkSearchProperties());
        for (Link link : allPages) {
            linkToColumnsMap.put(link.getId(), new TreeSet<>());
        }
        buildMap(linkMap, allPages);
    }

    /**
     * Retrieve a listing of the stage variables within this particular DataStage job.
     */
    private void loadStageVariablesForJob() throws IGCException {
        String jobRid = job.getId();
        log.debug("Retrieving stage variables for job: {}", jobRid);
        List<StageVariable> allPages = igcRestClient.getStageVariables(jobRid, DataStageConstants.getStageVariableSearchProperties());
        buildStageVariableMaps(allPages);
    }

    /**
     * Retrieve a listing of the stage columns within this particular DataStage job.
     */
    private void loadStageColumnDetailsForLinks() throws IGCException {
        String jobRid = job.getId();
        log.debug("Retrieving stage column details for job: {}", jobRid);
        List<StageColumn> stageCols =
                igcRestClient.getStageColumnDetailsForLinks("stage_column", jobRid, DataStageConstants.getStageColumnSearchProperties());
        if (stageCols == null) {
            log.info("Unable to identify stage columns for job by 'stage_column', reverting to 'ds_stage_column'.");
            stageCols = igcRestClient.getStageColumnDetailsForLinks("ds_stage_column", jobRid, DataStageConstants.getStageColumnSearchProperties());
        }
        if (stageCols != null) {
            buildMap(columnMap, stageCols);
            buildLinkStageColumnMap(stageCols);
        } else if (type.equals(JobType.JOB)) {
            // Only warn about finding no columns if this is a Job (Sequences in general will not have stage columns)
            log.warn("Unable to identify any stage columns for job: {}", jobRid);
        }
    }

    private void buildLinkStageColumnMap(List<StageColumn> stageCols) {
        for(StageColumn column : stageCols) {
            List<Reference> context = column.getContext();
            Reference link = context.get(context.size() - 1);
            if (link.getType().equals("link")) {
                String linkRid = link.getId();
                Set<String> linkCols = linkToColumnsMap.getOrDefault(linkRid, null);
                if (linkCols != null) {
                    linkCols.add(column.getId());
                    linkToColumnsMap.put(linkRid, linkCols);
                }
            }
        }
    }

    /**
     * Retrieve a listing of all the data assets (to field-level granularity) this particular DataStage job reads
     * from or writes to.
     *
     * @param cache cache and connectivity to IGC environment
     */
    private void loadDataAssets(DataStageReportsCache cache) throws IGCException {
        if (!getType().equals(JobType.SEQUENCE)) {
            loadDataStoreDetailsForJob(cache, "reads_from_(design)", job.getReadsFromDesign());
            loadDataStoreDetailsForJob(cache, "writes_to_(design)", job.getWritesToDesign());
        }
    }

    /**
     * Map the data store details used by the job into the provided map.
     *
     * @param cache        cache and connectivity to IGC environment
     * @param propertyName the name of the property from which the candidates were retrieved
     * @param candidates   the list of candidate data store details to cache
     */
    private void loadDataStoreDetailsForJob(DataStageReportsCache cache, String propertyName, ItemList<InformationAsset> candidates) throws
                                                                                                                                     IGCException {
        if (candidates != null) {
            List<InformationAsset> allCandidates = igcRestClient.getAllPages(propertyName, candidates);
            for (InformationAsset candidate : allCandidates) {
                String storeId = candidate.getId();
                if (propertyName.equals("reads_from_(design)")) {
                    inputStoreRIDs.add(storeId);
                } else if (propertyName.equals("writes_to_(design)")) {
                    outputStoreRIDs.add(storeId);
                }
                cache.loadFieldsForStore(candidate);
            }
        }
    }

    /**
     * Group the set of stages for this particular job according to whether they are input or output.
     *
     * @param stages the collection of stages to classify
     */
    private void loadStages(Collection<Stage> stages) {
        for (Stage candidateStage : stages) {
            String rid = candidateStage.getId();
            if (isInputStage(candidateStage)) {
                inputStageRIDs.add(rid);
            }
            if (isOutputStage(candidateStage)) {
                outputStageRIDs.add(rid);
            }
        }
    }

    /**
     * Retrieve the type of job represented by this instance.
     *
     * @return JobType
     */
    public JobType getType() {
        return type;
    }

    /**
     * Retrieve a list of the input stages for this job.
     *
     * @return {@code List<Stage>}
     */
    public List<Stage> getInputStages() {
        return inputStageRIDs.stream().map(stageMap::get).collect(Collectors.toList());
    }

    /**
     * Retrieve a list of the output stages for this job.
     *
     * @return {@code List<Stage>}
     */
    public List<Stage> getOutputStages() {
        return outputStageRIDs.stream().map(stageMap::get).collect(Collectors.toList());
    }

    /**
     * Retrieve all the stages used within the job.
     *
     * @return {@code Collection<Stage>}
     */
    public Collection<Stage> getAllStages() {
        return stageMap.values();
    }

    /**
     * Retrieve all the links used within the job.
     *
     * @return {@code Collection<Reference>}
     */
    public Collection<Link> getAllLinks() {
        return linkMap.values();
    }

    /**
     * Retrieve the job object itself.
     *
     * @return Dsjob
     */
    public Dsjob getJobObject() {
        return job;
    }

    /**
     * Retrieve a list of the input data stores for this job.
     *
     * @return {@code List<String>}
     */
    public List<String> getInputStores() {
        return inputStoreRIDs;
    }

    /**
     * Retrieve a list of the output data stores for this job.
     *
     * @return {@code List<String>}
     */
    public List<String> getOutputStores() {
        return outputStoreRIDs;
    }

    /**
     * Retrieve the set of data store RIDs that are used by this job.
     *
     * @return {@code Set<String>}
     */
    public Set<String> getStoreRids() {
        Set<String> set = new TreeSet<>();
        set.addAll(inputStoreRIDs);
        set.addAll(outputStoreRIDs);
        return set;
    }

    /**
     * Retrieve the complete 'link' object based on its RID.
     *
     * @param rid the RID of the link object
     *
     * @return Link
     */
    public Link getLinkByRid(String rid) throws IGCException {
        final String methodName = "getLinkByRid";
        log.debug("Looking up cached link: {}", rid);
        return linkMap.getOrDefault(rid, null);
    }

    /**
     * Retrieve the complete column-level lineage object ('stage_column' or 'stage_variable') based on its RID.
     *
     * @param rid the RID of the column-level lineage object
     *
     * @return ColumnLevelLineage
     */
    public ColumnLevelLineage getColumnLevelLineageByRid(String rid) {
        log.debug("Looking up cached stage column / variable: {}", rid);
        return columnMap.getOrDefault(rid, null);
    }

    /**
     * Retrieve the complete list of stage variables for the provided stage based on its RID.
     *
     * @param rid the RID of the stage for which to retrieve all stage variables
     *
     * @return {@code List<StageVariable>}
     */
    public List<StageVariable> getStageVarsForStage(String rid) {
        log.debug("Looking up cache stage variables for stage: {}", rid);
        Set<String> stageVarRids = stageToVarsMap.getOrDefault(rid, null);
        if (CollectionUtils.isEmpty(stageVarRids)) {
            return Collections.emptyList();
        }
        return stageVarRids.stream().map(varMap::get).collect(Collectors.toList());
    }

    public List<Link> getInputLinks(String stageRid) {
        return getAllLinks().stream().filter(link -> link.getInputStages().getId().equalsIgnoreCase(stageRid)).collect(Collectors.toList());
    }

    public List<Link> getOutputLinks(String stageRid) {
        return getAllLinks().stream().filter(link -> link.getOutputStages().getId().equalsIgnoreCase(stageRid)).collect(Collectors.toList());
    }

    /**
     * Retrieve the complete list of stage variables for the provided stage based on its RID.
     *
     * @param rid the RID of the stage for which to retrieve all stage variables
     *
     * @return {@code List<StageVariable>}
     */
    public List<StageColumn> getStageColumnsForLink(String rid) {
        log.debug("Looking up cache stage columns for link: {}", rid);
        Set<String> stageColumnRids = linkToColumnsMap.getOrDefault(rid, null);
        if (CollectionUtils.isEmpty(stageColumnRids)) {
            return Collections.emptyList();
        }
        return stageColumnRids.stream().map(columnMap::get).collect(Collectors.toList());
    }
    /**
     * Cache relationships for all the stage variables provided.
     *
     * @param allStageVars the list of stage variables to cache
     */
    private void buildStageVariableMaps(List<StageVariable> allStageVars) throws IGCConnectivityException, IGCParsingException {
        for (StageVariable stageVar : allStageVars) {
            String rid = stageVar.getId();
            // If the modification details are empty, likely we did not get this from a search but from paging
            // within a stage itself (cache miss), and therefore must retrieve the details of each variable one-by-one
            // as well
            if (stageVar.getModifiedBy() == null) {
                log.debug("...... retrieving stage variable by RID: {}", rid);
                stageVar = igcRestClient.getAssetWithSubsetOfProperties(rid, "stage_variable", DataStageConstants.getStageVariableSearchProperties());
            }
            log.debug("...... caching RID: {}", rid);
            varMap.put(rid, stageVar);
            String stageRid = stageVar.getStage().getId();
            Set<String> stageVars = stageToVarsMap.getOrDefault(stageRid, null);
            if (stageVars != null) {
                stageVars.add(rid);
                stageToVarsMap.put(stageRid, stageVars);
            } else {
                log.error("Stage variables were null for stage RID: {}", stageRid);
            }
        }
    }

    /**
     * Cache the provided ItemList of objects into the provided map, keyed by the RID of the object.
     *
     * @param map     the map into which to store the cache
     * @param objects the list of objects to cache
     * @param <T>     the type of object to cache
     */
    private <T extends Reference> void buildMap(Map<String, T> map, List<T> objects) {
        for (T candidateObject : objects) {
            String rid = candidateObject.getId();
            log.debug("...... caching RID: {}", rid);
            map.put(rid, candidateObject);
        }
    }

    /**
     * Check whether the provided stage is an input stage (true) or not (false).
     *
     * @param stage the stage to check
     *
     * @return boolean
     */
    private static boolean isInputStage(Stage stage) {
        ItemList<Link> inputLinks = stage.getInputLinks();
        return inputLinks != null && inputLinks.getPaging().getNumTotal() == 0;
    }

    /**
     * Check whether the provided stage is an output stage (true) or not (false).
     *
     * @param stage the stage to check
     *
     * @return boolean
     */
    private static boolean isOutputStage(Stage stage) {
        ItemList<Link> outputLinks = stage.getOutputLinks();
        return outputLinks != null && outputLinks.getPaging().getNumTotal() == 0;
    }
}
