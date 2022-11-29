/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.PortAliasMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Dsjob;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.Identity;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.ItemList;
import org.odpi.openmetadata.accessservices.dataengine.model.PortAlias;
import org.odpi.openmetadata.accessservices.dataengine.model.PortType;
import org.odpi.openmetadata.accessservices.dataengine.model.Process;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Mappings for creating a set of PortAliases.
 */
class PortAliasService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(PortAliasService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    PortAliasService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Create a list of PortAliases from the provided job and stage information.
     *
     * @param stages   the stages from which to create PortAliases
     * @param portType the type of port to map (input or output)
     *
     * @return {@code List<PortAlias>}
     */
    List<PortAlias> getForStages(List<Stage> stages, PortType portType) throws IGCException {
        Set<PortAlias> portAliases = new HashSet<>();
        for (Stage stage : stages) {
            if (portType.equals(PortType.INPUT_PORT)) {
                portAliases.addAll(getInputPortAliases(stage));
            } else if (portType.equals(PortType.OUTPUT_PORT)) {
                portAliases.addAll(getOutputPortAliases(stage));
            }
        }
        return new ArrayList<>(portAliases);
    }

    /**
     * Create a list of PortAliases from the provided sequence job.
     *
     * @param sequence the sequence for which to create PortAliases
     *
     * @return {@code List<PortAlias>}
     */
    List<PortAlias> getForSequence(DataStageJob sequence) throws IGCException {
        Set<PortAlias> portAliases = new HashSet<>();
        if (sequence.getType().equals(DataStageJob.JobType.SEQUENCE)) {
            for (Stage stage : sequence.getAllStages()) {
                Dsjob runsJob = stage.getRunsSequencesJobs();
                if (runsJob != null) {
                    String jobId = runsJob.getId();
                    if (jobId != null) {
                        Process jobProcess = cache.getProcessByRid(jobId);
                        if (jobProcess != null) {
                            List<PortAlias> jobPortAliases = jobProcess.getPortAliases();
                            // Create a new PortAlias at the sequence level, for each underlying PortAlias of jobs
                            // that are executed, that delegateTo the underlying job's PortAlias
                            for (PortAlias delegateTo : jobPortAliases) {
                                String stageQN = getFullyQualifiedName(stage, delegateTo.getQualifiedName());
                                if (stageQN != null) {
                                    String displayName = stage.getName() + "_" + delegateTo.getDisplayName();
                                    portAliases.add(PortAliasMapper.getPortAlias(stageQN, displayName, delegateTo.getPortType(),
                                            delegateTo.getQualifiedName()));
                                } else {
                                    log.error("Unable to determine identity for stage -- not including: {}", stage);
                                }
                            }
                        } else {
                            log.warn("Unable to find existing process to use for alias: {}", jobId);
                        }
                    }
                }
            }
        }
        return new ArrayList<>(portAliases);
    }

    private Set<PortAlias> getInputPortAliases(Stage stage) throws IGCException {
        return getPortAliases(stage, "reads_from_(design)", stage.getReadsFromDesign(), PortType.INPUT_PORT);
    }

    private Set<PortAlias> getOutputPortAliases(Stage stage) throws IGCException {
        return getPortAliases(stage, "writes_to_(design)", stage.getWritesToDesign(), PortType.OUTPUT_PORT);
    }

    private Set<PortAlias> getPortAliases(Stage stage, String propertyName, ItemList<InformationAsset> relations, PortType portType) throws
                                                                                                                                     IGCException {

        Set<PortAlias> portAliases = new HashSet<>();
        List<InformationAsset> allRelations = igcRestClient.getAllPages(propertyName, relations);
        int index = 0;
        for (InformationAsset relation : allRelations) {
            index++;
            Identity storeIdentity = cache.getStoreIdentityFromRid(relation.getId());
            String fullyQualifiedStageName = getFullyQualifiedName(stage);
            // Use the index to at least make each PortAlias qualifiedName for this stage unique (without risking
            // overlapping with the PortImplementation qualifiedNames that we need to delegateTo)
            portAliases.add(PortAliasMapper.getPortAlias(fullyQualifiedStageName + "_" + index, stage.getName(), portType,
                    getFullyQualifiedName(storeIdentity, fullyQualifiedStageName)));
        }
        return portAliases;
    }
}
