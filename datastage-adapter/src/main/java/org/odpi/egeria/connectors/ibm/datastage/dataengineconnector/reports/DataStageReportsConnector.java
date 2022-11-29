/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.ConnectorHelper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.auditlog.DataStageErrorCode;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.LineageMode;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.IGCRestClient;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.openmetadata.accessservices.dataengine.model.Engine;
import org.odpi.openmetadata.accessservices.dataengine.model.LineageMapping;
import org.odpi.openmetadata.accessservices.dataengine.model.Process;
import org.odpi.openmetadata.accessservices.dataengine.model.ProcessHierarchy;
import org.odpi.openmetadata.accessservices.dataengine.model.Referenceable;
import org.odpi.openmetadata.accessservices.dataengine.model.SchemaType;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;
import org.odpi.openmetadata.frameworks.connectors.properties.EndpointProperties;
import org.odpi.openmetadata.governanceservers.dataengineproxy.connectors.DataEngineConnectorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.DataStageReportsConnectorProvider.CREATE_DATA_STORE_SCHEMAS;
import static org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.DataStageReportsConnectorProvider.INCLUDE_VIRTUAL_ASSETS;
import static org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.DataStageReportsConnectorProvider.MODE;
import static org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.DataStageReportsConnectorProvider.PAGE_SIZE;

public class DataStageReportsConnector extends DataEngineConnectorBase {

    private static final Logger log = LoggerFactory.getLogger(DataStageReportsConnector.class);
    private IGCRestClient igcRestClient;
    private Engine dataEngine;
    private DataStageReportsCache dataStageCache;
    private List<ProcessHierarchy> processHierarchies;

    private boolean includeVirtualAssets = true;
    private boolean createDataStoreSchemas = false;
    private boolean detectLineage = false;
    private final List<String> startAssetRIDs = new ArrayList<>();

    private LineageMode mode = LineageMode.GRANULAR;

    @Override
    public boolean requiresPolling() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String connectorInstanceId,
                           ConnectionProperties connectionProperties) {
        super.initialize(connectorInstanceId, connectionProperties);
        this.dataStageCache = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized void start() throws ConnectorCheckedException {

        super.start();
        final String methodName = "start";

        log.info("Initializing " + this.getClass().getSimpleName() + "...");

        EndpointProperties endpointProperties = connectionProperties.getEndpoint();
        if (endpointProperties == null) {
            ConnectorHelper.raiseConnectorCheckedException(this.getClass(), methodName, null,
                    DataStageErrorCode.CONNECTION_FAILURE.getMessageDefinition());
        } else {
            String address = endpointProperties.getAddress();
            if (address == null || address.length() == 0) {
                ConnectorHelper.raiseConnectorCheckedException(this.getClass(), methodName, null,
                        DataStageErrorCode.CONNECTION_FAILURE.getMessageDefinition(address));
            } else {

                String igcUser = connectionProperties.getUserId();
                String igcPass = connectionProperties.getClearPassword();

                Map<String, Object> proxyProperties = this.connectionBean.getConfigurationProperties();
                Integer igcPage = null;
                if (proxyProperties != null) {
                    igcPage = (Integer) proxyProperties.get(PAGE_SIZE);
                    includeVirtualAssets = (Boolean) proxyProperties.getOrDefault(INCLUDE_VIRTUAL_ASSETS, true);
                    createDataStoreSchemas = (Boolean) proxyProperties.getOrDefault(CREATE_DATA_STORE_SCHEMAS, false);
                    detectLineage = (Boolean) proxyProperties.getOrDefault(DataStageReportsConnectorProvider.DETECT_LINEAGE, false);

                    Object reportRids = proxyProperties.getOrDefault(DataStageReportsConnectorProvider.START_ASSET_RIDS, null);
                    if (reportRids instanceof String) {
                        startAssetRIDs.add((String) reportRids);
                    } else if (reportRids != null) {
                        startAssetRIDs.addAll((List<String>) reportRids);
                    }
                    Object lineageMode = proxyProperties.getOrDefault(MODE, null);
                    if (lineageMode != null) {
                        try {
                            mode = LineageMode.valueOf((String) lineageMode);
                        } catch (IllegalArgumentException e) {
                            log.warn("Lineage mode specified in the configuration is not a known value.", e);
                        }
                    }
                }

                try {
                    igcRestClient = new IGCRestClient("https://" + address, igcUser, igcPass);
                    dataEngine = new Engine();
                    ConnectorHelper.connectIGC(this.getClass(), igcRestClient, dataEngine, address, igcPage);
                } catch (IGCException e) {
                    ConnectorHelper.raiseConnectorCheckedException(this.getClass(), methodName, e,
                            DataStageErrorCode.CONNECTION_FAILURE.getMessageDefinition(address));
                }

            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void disconnect() throws ConnectorCheckedException {
        final String methodName = "disconnect";
        try {
            // Close the session on the IGC REST client
            this.igcRestClient.disconnect();
        } catch (IGCException e) {
            ConnectorHelper.raiseConnectorCheckedException(this.getClass(), methodName, e,
                    DataStageErrorCode.CONNECTION_FAILURE.getMessageDefinition());
        }
    }

    /**
     * Initialize the cache of changed job details based on the provided dates and times.
     */
    private void initializeCache() throws IGCException {
        this.dataStageCache = new DataStageReportsCache(mode);
        dataStageCache.initializeWithReportJobs(igcRestClient, startAssetRIDs, detectLineage);
        processHierarchies = new ArrayList<>();
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public Engine getDataEngineDetails() {
        return dataEngine;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProcessingStateSyncKey() {

        String rids = String.join(", ", startAssetRIDs);
        return "by reports, mode = " + mode.getName() + ", startAssetRIDs = [" + rids + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<SchemaType> getChangedSchemaTypes(Date from, Date to) throws ConnectorCheckedException, PropertyServerException {

//        final String methodName = "getChangedSchemaTypes";
//        log.debug("Looking for changed SchemaTypes...");
//        Map<String, SchemaType> schemaTypeMap = new HashMap<>();
//
//        if (mode == LineageMode.JOB_LEVEL) {
//            return Collections.emptyList();
//        }
//
//        try {
//            initializeCache(from, to);
//            schemaTypeMap = ConnectorHelper.mapChangedSchemaTypes(dataStageCache, includeVirtualAssets, createDataStoreSchemas);
//        } catch (IGCException e) {
//            ConnectorHelper.handleIGCException(this.getClass().getName(), methodName, e);
//        }
//        return new ArrayList<>(schemaTypeMap.values());
        //TODO create incomplete schema types along with the incomplete table/file (check what is needed to  needed for having incomplete
        // classification on column and schema)
        return null;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<? super Referenceable> getChangedDataStores(Date from, Date to) throws ConnectorCheckedException, PropertyServerException {

//        final String methodName = "getChangedDataStores";
//        log.debug("Looking for changed DataStores...");
//        Map<String, ? super Referenceable> dataStoreMap = new HashMap<>();
//
//        try {
//            initializeCache(from, to);
//            dataStoreMap = ConnectorHelper.mapChangedDataStores(dataStageCache, includeVirtualAssets);
//        } catch (IGCException e) {
//            ConnectorHelper.handleIGCException(this.getClass().getName(), methodName, e);
//        }
//        return new ArrayList<>(dataStoreMap.values());
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Process> getChangedProcesses(Date from, Date to) throws ConnectorCheckedException, PropertyServerException {

//        final String methodName = "getChangedProcesses";
//        List<Process> processes = new ArrayList<>();
//
//        try {
//            initializeCache(from, to);
//            processes = ConnectorHelper.mapChangedProcesses(dataStageCache, processHierarchies, mode);
//
//        } catch (IGCException e) {
//            ConnectorHelper.handleIGCException(this.getClass().getName(), methodName, e);
//        }
//
//        return processes;
        return null;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<ProcessHierarchy> getChangedProcessHierarchies(Date from, Date to) {
        // Output list of changed process hierarchies from the cached information
        return processHierarchies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<LineageMapping> getChangedLineageMappings(Date from, Date to) {
        // do nothing -- lineage mappings will always be handled by other methods
        return Collections.emptyList();
    }

    public void load() throws ConnectorCheckedException, PropertyServerException {
        final String methodName = "load";
        try {

            initializeCache();
            List<Process> processes = new ArrayList<>();
            List<DataStageJob> seqList = new ArrayList<>();

            for (DataStageJob detailedJob : dataStageCache.getAllJobs()) {

            }
        } catch (IGCException e) {
            ConnectorHelper.handleIGCException(this.getClass().getName(), methodName, e);
        }
    }
}
