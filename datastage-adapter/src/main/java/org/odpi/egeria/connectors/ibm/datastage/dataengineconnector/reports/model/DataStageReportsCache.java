/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model;

import org.apache.commons.collections4.CollectionUtils;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.DataStageConstants;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.mapping.ProcessMapping;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.services.ReportService;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.IGCRestClient;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.cache.ObjectCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCConnectivityException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCParsingException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Dsjob;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Reference;
import org.odpi.openmetadata.accessservices.dataengine.model.Process;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class to cache DataStage information for use by multiple steps in the Data Engine processing.
 */
public class DataStageReportsCache {
    private static final Logger log = LoggerFactory.getLogger(DataStageReportsCache.class);
    private final Map<String, DataStageJob> ridToJob;
    private final Map<String, Process> ridToProcess;
    private final Map<String, Identity> storeToIdentity;
    private final Map<String, List<Classificationenabledgroup>> storeToColumns;
    private IGCRestClient igcRestClient;
    private final ObjectCache igcCache;
    private final LineageMode mode;

    private ReportService reportService;

    /**
     * Create a new cache for changes between the times provided.
     *
     * @param mode the mode of operation for the connector, indicating the level of detail to include for lineage
     */
    public DataStageReportsCache(LineageMode mode) {
        this.igcCache = new ObjectCache();
        this.ridToJob = new HashMap<>();
        this.ridToProcess = new HashMap<>();
        this.storeToIdentity = new HashMap<>();
        this.storeToColumns = new HashMap<>();
        this.mode = mode;
    }

    public boolean initializeWithReportJobs(IGCRestClient igcRestClient, List<String> assetRIDs, boolean detectLineage) throws IGCException {
        this.igcRestClient = igcRestClient;
        this.reportService = new ReportService(igcRestClient);
        if (CollectionUtils.isNotEmpty(assetRIDs)) {
            List<String> jobsFromReports = reportService.getJobsFromReports(assetRIDs);
            cacheReportJobs(jobsFromReports, detectLineage);
            return true;
        }
        return false;
    }

    private void cacheReportJobs(List<String> jobRids, boolean detectLineage) throws IGCConnectivityException, IGCParsingException {
        for (String jobRid : jobRids) {
            Dsjob dsjob = (Dsjob) igcRestClient.getAssetById(jobRid);
            cacheReportJob(dsjob, detectLineage);
        }
    }

    private void cacheReportJob(Dsjob dsjob, boolean detectLineage) {
        String jobRid = dsjob.getId();
        if (!ridToJob.containsKey(jobRid)) {
            try {
                log.debug("Detecting lineage on dsjob: {}", jobRid);
                // Detect lineage on each dsjob to ensure its details are fully populated before proceeding
                boolean lineageDetected = true;
                if (detectLineage) {
                    lineageDetected = igcRestClient.detectLineage(jobRid);
                }
                if (lineageDetected) {
                    // We then need to re-retrieve the dsjob's details, as they may have changed since lineage
                    // detection (following call will be a no-op if the dsjob is already in the cache)
                    DataStageJob job = new DataStageJob(this, dsjob);
                    this.ridToJob.put(jobRid, job);
                } else {
                    log.warn("Unable to detect lineage for dsjob -- not including: {}", jobRid);
                }
            } catch (IGCException e) {
                log.error("Failed to detect lineage for dsjob -- not including: {}, check system log for exception details.", jobRid, e);
            }
        }
    }

    /**
     * Retrieve the mode of operation of the cache (level of detail to inclue for lineage).
     *
     * @return LineageMode
     */
    public LineageMode getMode() {
        return mode;
    }

    /**
     * Retrieve the embedded cache of IGC objects.
     *
     * @return ObjectCache
     */
    public ObjectCache getIgcCache() {
        return igcCache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStageReportsCache that = (DataStageReportsCache) o;
        return Objects.equals(ridToJob, that.ridToJob) && Objects.equals(ridToProcess, that.ridToProcess) &&
                Objects.equals(storeToIdentity, that.storeToIdentity) && Objects.equals(storeToColumns, that.storeToColumns) &&
                Objects.equals(igcRestClient, that.igcRestClient) && Objects.equals(igcCache, that.igcCache) && mode == that.mode &&
                Objects.equals(reportService, that.reportService);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ridToJob, ridToProcess, storeToIdentity, storeToColumns, igcRestClient, igcCache, mode, reportService);
    }

    /**
     * Retrieve the IGC connectivity used to populate the cache.
     *
     * @return IGCRestClient
     */
    public IGCRestClient getIgcRestClient() {
        return igcRestClient;
    }

    /**
     * Retrieve all cached jobs.
     *
     * @return {@code Collection<DataStageJob>}
     */
    public Collection<DataStageJob> getAllJobs() {
        return ridToJob.values();
    }

    /**
     * Retrieve the Process representation of the provided DataStage job object's (sequence or job) RID.
     *
     * @param rid of the DataStage job object (sequence or job)
     *
     * @return Process representation of the job (or sequence)
     */
    public Process getProcessByRid(String rid) throws IGCException {
        return ridToProcess.getOrDefault(rid, null);
//        org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.model.DataStageJob job = getJobByRid(rid);
//        if (job != null) {
//            ProcessMapping processMapping = new ProcessMapping(this);
//            process = processMapping.getForJob(job);
//            if (process != null) {
//                ridToProcess.put(rid, process);
//            }
//        }
    }

    /**
     * Retrieve the set of process RIDs that are currently cached.
     *
     * @return {@code Set<String>}
     */
    public Set<String> getCachedProcessRids() {
        return new HashSet<>(ridToProcess.keySet());
    }

    /**
     * Loads the list of fields for the provided data store ('database_table', 'view', or 'data_file_record')
     * Repository ID (RID).
     *
     * @param store the InformationAsset representing the data store for which to retrieve the fields
     */
    public void loadFieldsForStore(InformationAsset store) throws IGCException {
        // First try to retrieve it from the cache directly
        String rid = store.getId();
        String storeType = store.getType();

        List<Classificationenabledgroup> fields = null;

        // Regardless of mode, we at least need the store identity
        Identity storeIdentity = storeToIdentity.getOrDefault(rid, null);
        if (storeIdentity == null) {
            // If not there, retrieve it and cache it
            log.debug("(cache miss) -- retrieving data store details for {}: {}", storeType, rid);
            if (!store.isVirtualAsset()) {
                // For non-virtual assets the most efficient way of retrieving this information is via a search (by RID)
                storeIdentity = store.getIdentity(igcRestClient, igcCache);
                storeToIdentity.put(rid, storeIdentity);
            } else {
                // For virtual assets, we must retrieve the full object (search by RID is not possible)
                Reference virtualStore = igcRestClient.getAssetById(rid, igcCache);
                storeToIdentity.put(rid, virtualStore.getIdentity(igcRestClient, igcCache));
            }
        }

        if (mode == LineageMode.GRANULAR) {
            fields = storeToColumns.getOrDefault(rid, null);
            if (fields == null) {
                // If not there, run a search to retrieve it
                log.debug("(cache miss) -- retrieving data field details for {}: {}", storeType, rid);
                if (!store.isVirtualAsset()) {
                    // For non-virtual assets the most efficient way of retrieving this information is via a search (by RID)
                    fields = igcRestClient.getDataStoreFields(store, rid, storeType, DataStageConstants.getDataFieldSearchProperties());
                } else {
                    Reference virtualStore = igcRestClient.getAssetById(rid, igcCache);
                    // For virtual assets, we must retrieve the full object and page through its fields (search by RID is not possible)
                    fields = igcRestClient.getVirtualDataStoreFields(virtualStore, igcCache);
                }
                // Add them to the cache once they've been retrieved
                if (CollectionUtils.isNotEmpty(fields)) {
                    storeToColumns.put(rid, fields);
                    storeIdentity = fields.get(0).getIdentity(igcRestClient, igcCache).getParentIdentity();
                    String storeId = storeIdentity.getRid();
                    storeToIdentity.put(storeId, storeIdentity);
                }
            }
        }
    }

    /**
     * Retrieve the list of fields for the provided data store ('database_table', 'view', or 'data_file_record')
     * Repository ID (RID).
     *
     * @param store the InformationAsset representing the data store for which to retrieve the fields
     *
     * @return {@code List<Classificationenabledgroup>} of fields in that data store
     */
    public List<Classificationenabledgroup> getFieldsForStore(InformationAsset store)  {
        String rid = store.getId();
        List<Classificationenabledgroup> fields = null;

        if (mode == LineageMode.GRANULAR) {
            fields = storeToColumns.getOrDefault(rid, null);
        }
        return fields;
    }

    /**
     * Retrieve the store identity for the provided data store RID, or null if it cannot be found.
     *
     * @param rid the data store RID for which to retrieve an identity
     *
     * @return Identity
     */
    public Identity getStoreIdentityFromRid(String rid) {
        return storeToIdentity.getOrDefault(rid, null);
    }
}
