/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.services;

import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.mapping.mappers.ProcessMapper;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageJob;
import org.odpi.egeria.connectors.ibm.datastage.dataengineconnector.reports.model.DataStageReportsCache;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCConnectivityException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCIOException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.errors.IGCParsingException;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Classificationenabledgroup;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Dsjob;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.InformationAsset;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Link;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.SequenceJob;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.Stage;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.base.StageVariable;
import org.odpi.egeria.connectors.ibm.igc.clientlibrary.model.common.ItemList;
import org.odpi.openmetadata.accessservices.dataengine.model.Collection;
import org.odpi.openmetadata.accessservices.dataengine.model.LineageMapping;
import org.odpi.openmetadata.accessservices.dataengine.model.ParentProcess;
import org.odpi.openmetadata.accessservices.dataengine.model.PortAlias;
import org.odpi.openmetadata.accessservices.dataengine.model.PortImplementation;
import org.odpi.openmetadata.accessservices.dataengine.model.PortType;
import org.odpi.openmetadata.accessservices.dataengine.model.Process;
import org.odpi.openmetadata.accessservices.dataengine.model.ProcessContainmentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mappings for creating a Process.
 */
public class ProcessService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(ProcessService.class);

    /**
     * Default constructor to pass in the cache for re-use.
     *
     * @param cache used by this mapping
     */
    public ProcessService(DataStageReportsCache cache) {
        super(cache);
    }

    /**
     * Create a new process from a DataStageJob that represents either a job or a sequence.
     *
     * @param job the job or sequence from which to create a process
     *
     * @return Process
     */
    public Process getForJob(DataStageJob job) throws IGCException {
        Dsjob jobObj = job.getJobObject();
        Process process = getSkeletonProcess(jobObj);
        if (process == null) {
            return null;
        }
        process.setCollection(getCollection(jobObj));
        if (job.getType().equals(DataStageJob.JobType.SEQUENCE)) {
            process.setPortAliases(getSequencePortAliases(job));
        } else {
            switch (cache.getMode()) {
                case JOB_LEVEL:
                    // Setup job-level lineage mappings
                    process.setLineageMappings(getJobLevelLineageMappings(job));
                    break;
                case GRANULAR:
                    ItemList<SequenceJob> sequencedBy = job.getJobObject().getSequencedByJobs();
                    if (sequencedBy != null) {
                        // Setup a parent process relationship to any sequences that happen to call this job
                        // as only APPEND parents (not OWNED), since removal of the sequence does not result in removal
                        // of the job itself
                        List<SequenceJob> allSequences = igcRestClient.getAllPages("sequenced_by_jobs", sequencedBy);
                        process.setParentProcesses(getSequenceParentProcesses(allSequences));
                    }
                    process.setPortAliases(getJobPortAliases(job));
                    process.setLineageMappings(getCrossStageLineageMappings(job));
            }
        }
        return process;
    }

    /**
     * Create a new process from the provided DataStage stage.
     *
     * @param stage the stage from which to create a process
     * @param job   the job within which the stage exists
     *
     * @return Process
     */
    public Process getForStage(Stage stage, DataStageJob job) throws IGCException {
        Process process = getSkeletonProcess(stage);
        if (process == null) {
            return null;
        }

        Set<PortImplementation> portImplementations = new HashSet<>();
        Set<LineageMapping> lineageMappings = new HashSet<>();

        List<Link> allInputLinks = job.getInputLinks(stage.getId());
        List<Link> allOutputLinks = job.getOutputLinks(stage.getId());
        Set<String> allLinkRids = Stream.concat(allInputLinks.stream(), allOutputLinks.stream()).map(Link::getId).collect(Collectors.toSet());
        log.debug("Adding input links: {}", allInputLinks);
        addImplementationDetails(job, stage, allInputLinks, allLinkRids, PortType.INPUT_PORT, portImplementations, lineageMappings);

        log.debug("Adding input stores: {}", stage.getReadsFromDesign());
        addDataStoreDetails(job, stage, "reads_from_(design)", stage.getReadsFromDesign(), allLinkRids, PortType.INPUT_PORT, portImplementations,
                lineageMappings);
        log.debug("Adding output links: {}", allOutputLinks);
        addImplementationDetails(job, stage, allOutputLinks, allLinkRids, PortType.OUTPUT_PORT, portImplementations, lineageMappings);
        log.debug("Adding output stores: {}", stage.getWritesToDesign());
        addDataStoreDetails(job, stage, "writes_to_(design)", stage.getWritesToDesign(), allLinkRids, PortType.OUTPUT_PORT, portImplementations,
                lineageMappings);
        log.debug("Adding stage variables");
        addStageVariableDetails(job, stage, portImplementations, lineageMappings);

        log.debug("Adding transformation project info ");
        process.setCollection(getCollection(stage));

        process.setPortImplementations(new ArrayList<>(portImplementations));
        process.setLineageMappings(new ArrayList<>(lineageMappings));

        // Stages are owned by the job that contains them, so setup an owned parent process relationship to the
        // job-level
        process.setParentProcesses(Collections.singletonList(getParentProcess(job)));

        return process;
    }

    /**
     * Add implementation details of the job (ports and lineage mappings) to the provided lists, for the provided stage.
     *
     * @param job                 the job within which the stage exists
     * @param stage               the stage for which to add implementation details
     * @param links               the links of the stage from which to draw implementation details
     * @param linkRids            the RIDs of all links known by this stage (both input and output)
     * @param portType            the type of port
     * @param portImplementations the list of PortImplementations to append to with implementation details
     * @param lineageMappings     the list of LineageMappings to append to with implementation details
     */
    private void addImplementationDetails(DataStageJob job,
                                          Stage stage,
                                          List<Link> links,
                                          Set<String> linkRids,
                                          PortType portType,
                                          Set<PortImplementation> portImplementations,
                                          Set<LineageMapping> lineageMappings) throws IGCException {
        final String methodName = "addImplementationDetails";

        String stageQN = getFullyQualifiedName(stage);
        // Setup an x_PORT for each x_link into / out of the stage
        for (Link linkRef : links) {
            Link linkObjFull = job.getLinkByRid(linkRef.getId());
            log.debug("Adding implementation details for link: {}", linkObjFull);
            PortImplementationService portImplementationService = new PortImplementationService(cache);
            portImplementations.add(portImplementationService.getForLink(linkObjFull, job, portType, stageQN));
            log.debug("Adding lineage mappings for link as {}: {}", portType.getName(), linkObjFull);
            LineageMappingsService lineageMappingService = new LineageMappingsService(cache);
            lineageMappings.addAll(
                    lineageMappingService.getForLinkInStage(linkObjFull, job, stage.getId(), linkRids, stageQN, portType == PortType.INPUT_PORT));
        }

    }

    /**
     * Add stage variable details to the provided lists, for the provided stage.
     *
     * @param job                 the job within which the stage exists
     * @param stage               the stage for which to add stage variable details
     * @param portImplementations the set of PortImplementations to append to with implementation details
     * @param lineageMappings     the set of LineageMappings to append to with lineage mapping details
     */
    private void addStageVariableDetails(DataStageJob job,
                                         Stage stage,
                                         Set<PortImplementation> portImplementations,
                                         Set<LineageMapping> lineageMappings) throws IGCException {
        final String methodName = "addStageVariableDetails";

        String stageQN = getFullyQualifiedName(stage);
        List<StageVariable> stageVarsForStage = job.getStageVarsForStage(stage.getId());
        if (stageVarsForStage != null && !stageVarsForStage.isEmpty()) {
            log.debug("Adding implementation details for stage variables of stage: {}", stageQN);
            PortImplementationService portImplementationService = new PortImplementationService(cache);
            portImplementations.add(portImplementationService.getForStageVariables(stageVarsForStage, job, stage, stageQN));
            log.debug("Adding lineage mappings for stage variables of stage: {}", stageQN);
            LineageMappingsService lineageMappingService = new LineageMappingsService(cache);
            lineageMappings.addAll(lineageMappingService.getForStageVariables(stageVarsForStage, job, stageQN));
        } else {
            log.debug("No stage variables present in stage -- skipping: {}", stageQN);
        }

    }

    /**
     * Add implementation details of the job (ports and lineage mappings) to the provided lists, for any data stores
     * used by the specified stage.
     *
     * @param job                 the job within which the stage exists
     * @param stage               the stage for which to add implementation details
     * @param propertyName        the name of the property from which stores were retrieved
     * @param stores              the stores for which to create the implementation details
     * @param linkRids            the RIDs of all links known by this stage (both input and output)
     * @param portType            the type of port
     * @param portImplementations the list of PortImplementations to append to with implementation details
     * @param lineageMappings     the list of LineageMappings to append to with implementation details
     */
    private void addDataStoreDetails(DataStageJob job,
                                     Stage stage,
                                     String propertyName,
                                     ItemList<InformationAsset> stores,
                                     Set<String> linkRids,
                                     PortType portType,
                                     Set<PortImplementation> portImplementations,
                                     Set<LineageMapping> lineageMappings) throws IGCException {
        final String methodName = "addDataStoreDetails";
        // Setup an x_PORT for any data stores that are used by design as sources / targets

        String fullyQualifiedStageName = getFullyQualifiedName(stage);
        if (fullyQualifiedStageName != null) {
            List<InformationAsset> allStores = igcRestClient.getAllPages(propertyName, stores);
            for (InformationAsset storeRef : allStores) {
                List<Classificationenabledgroup> fieldsForStore = cache.getFieldsForStore(storeRef);
                log.debug("Adding implementation details for fields: {}", fieldsForStore);
                PortImplementationService portImplementationService = new PortImplementationService(cache);
                portImplementations.add(portImplementationService.getForDataStoreFields(fieldsForStore, stage, portType, fullyQualifiedStageName));
                log.debug("Adding lineage mappings for fields as {}: {}", portType.getName(), fieldsForStore);
                LineageMappingsService lineageMappingService = new LineageMappingsService(cache);
                lineageMappings.addAll(lineageMappingService.getForDataStoreFieldsInStage(fieldsForStore, job, stage.getId(), linkRids,
                        portType.equals(PortType.INPUT_PORT), fullyQualifiedStageName));
            }
        } else {
            log.error("Unable to determine identity for stage -- not including: {}", stage);
        }

    }

    /**
     * Add transformation project to process, as collection, if it exists in the asset
     *
     * @param asset the asset for which to add transformation project
     */
    private Collection getCollection(InformationAsset asset) throws IGCException {
        CollectionService collectionService = new CollectionService(cache);
        return collectionService.getCollection(asset);
    }

    private List<LineageMapping> getJobLevelLineageMappings(DataStageJob job) throws IGCException {
        LineageMappingsService lineageMappingService = new LineageMappingsService(cache);
        return new ArrayList<>(lineageMappingService.getForJob(job));
    }

    private List<PortAlias> getSequencePortAliases(DataStageJob job) throws IGCException {
        PortAliasService portAliasService = new PortAliasService(cache);
        return portAliasService.getForSequence(job);
    }

    private List<PortAlias> getJobPortAliases(DataStageJob job) throws IGCException {
        PortAliasService portAliasService = new PortAliasService(cache);
        return Stream.concat(portAliasService.getForStages(job.getInputStages(), PortType.INPUT_PORT).stream(),
                        portAliasService.getForStages(job.getOutputStages(), PortType.OUTPUT_PORT).stream())
                .collect(Collectors.toList());
    }

    private List<LineageMapping> getCrossStageLineageMappings(DataStageJob job) throws IGCException {
        Set<LineageMapping> lineageMappings = new HashSet<>();
        for (Link link : job.getAllLinks()) {
            LineageMappingsService lineageMappingService = new LineageMappingsService(cache);
            lineageMappings.addAll(lineageMappingService.getForLink(link, job));
        }
        return new ArrayList<>(lineageMappings);
    }

    private List<ParentProcess> getSequenceParentProcesses(List<SequenceJob> allSequences) throws IGCConnectivityException, IGCParsingException,
                                                                                                  IGCIOException {
        List<ParentProcess> parents = new ArrayList<>();
        for (SequenceJob sequenceJob : allSequences) {
            ParentProcess parent = new ParentProcess();
            String sequenceJobQN = getFullyQualifiedName(sequenceJob);
            if (sequenceJobQN != null) {
                parent.setQualifiedName(sequenceJobQN);
                parent.setProcessContainmentType(ProcessContainmentType.APPEND);
                parents.add(parent);
            } else {
                log.error("Unable to determine identity for sequence -- not including: {}", sequenceJob);
            }
        }
        return parents;
    }


    private ParentProcess getParentProcess(DataStageJob job) throws IGCConnectivityException, IGCParsingException, IGCIOException {
        ParentProcess parent = new ParentProcess();
        String jobQN = getFullyQualifiedName(job.getJobObject());
        if (jobQN != null) {
            parent.setQualifiedName(jobQN);
            parent.setProcessContainmentType(ProcessContainmentType.OWNED);
        } else {
            log.error("Unable to determine identity for job -- not including: {}", job.getJobObject());
        }
        return parent;
    }

    /**
     * Construct a minimal Process object from the provided IGC object (stage, job, sequence, etc).
     *
     * @param igcObj the IGC object from which to construct the skeletal Process
     *
     * @return Process
     */
    private Process getSkeletonProcess(InformationAsset igcObj) throws IGCException {
        if (igcObj == null) {
            return null;
        }
        String qualifiedName = getFullyQualifiedName(igcObj);
        if (qualifiedName != null) {
            log.debug("Constructing process for: {}", qualifiedName);
            return ProcessMapper.getProcess(qualifiedName, igcObj.getName(), igcObj.getName(), getDescription(igcObj), igcObj.getCreatedBy());
        } else {
            log.error("Unable to determine identity for asset -- not including: {}", igcObj);
            return null;
        }
    }
}
