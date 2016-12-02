//package test
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.yarn.api.records.*;
//import org.apache.hadoop.yarn.client.api.AMRMClient;
//import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
//
//import java.util.List;
//
//public class MyCallbackHandler implements AMRMClientAsync.CallbackHandler {
//    private static final Log LOG = LogFactory.getLog(MyCallbackHandler.class);
//    @SuppressWarnings("unchecked")
//    @Override
//    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
//        LOG.info("Got response from RM for container ask, completedCnt="
//                + completedContainers.size());
//        for (ContainerStatus containerStatus : completedContainers) {
//            LOG.info(appAttemptID + " got container status for containerID="
//                    + containerStatus.getContainerId() + ", state="
//                    + containerStatus.getState() + ", exitStatus="
//                    + containerStatus.getExitStatus() + ", diagnostics="
//                    + containerStatus.getDiagnostics());
//
//            // non complete containers should not be here
//            assert (containerStatus.getState() == ContainerState.COMPLETE);
//
//            // increment counters for completed/failed containers
//            int exitStatus = containerStatus.getExitStatus();
//            if (0 != exitStatus) {
//                // container failed
//                if (ContainerExitStatus.ABORTED != exitStatus) {
//                    // shell script failed
//                    // counts as completed
//                    numCompletedContainers.incrementAndGet();
//                    numFailedContainers.incrementAndGet();
//                } else {
//                    // container was killed by framework, possibly preempted
//                    // we should re-try as the container was lost for some reason
//                    numAllocatedContainers.decrementAndGet();
//                    numRequestedContainers.decrementAndGet();
//                    // we do not need to release the container as it would be done
//                    // by the RM
//                }
//            } else {
//                // nothing to do
//                // container completed successfully
//                numCompletedContainers.incrementAndGet();
//                LOG.info("Container completed successfully." + ", containerId="
//                        + containerStatus.getContainerId());
//            }
//            publishContainerEndEvent(
//                    timelineClient, containerStatus, domainId, appSubmitterUgi);
//        }
//
//        // ask for more containers if any failed
//        int askCount = numTotalContainers - numRequestedContainers.get();
//        numRequestedContainers.addAndGet(askCount);
//
//        if (askCount > 0) {
//            for (int i = 0; i < askCount; ++i) {
//                AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
//                amRMClient.addContainerRequest(containerAsk);
//            }
//        }
//
//        if (numCompletedContainers.get() == numTotalContainers) {
//            done = true;
//        }
//    }
//
//    @Override
//    public void onContainersAllocated(List<Container> allocatedContainers) {
//        LOG.info("Got response from RM for container ask, allocatedCnt="
//                + allocatedContainers.size());
//        numAllocatedContainers.addAndGet(allocatedContainers.size());
//        for (Container allocatedContainer : allocatedContainers) {
//            LOG.info("Launching shell command on a new container."
//                    + ", containerId=" + allocatedContainer.getId()
//                    + ", containerNode=" + allocatedContainer.getNodeId().getHost()
//                    + ":" + allocatedContainer.getNodeId().getPort()
//                    + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
//                    + ", containerResourceMemory"
//                    + allocatedContainer.getResource().getMemory()
//                    + ", containerResourceVirtualCores"
//                    + allocatedContainer.getResource().getVirtualCores());
//            // + ", containerToken"
//            // +allocatedContainer.getContainerToken().getIdentifier().toString());
//
//            LaunchContainerRunnable runnableLaunchContainer =
//                    new LaunchContainerRunnable(allocatedContainer, containerListener);
//            Thread launchThread = new Thread(runnableLaunchContainer);
//
//            // launch and start the container on a separate thread to keep
//            // the main thread unblocked
//            // as all containers may not be allocated at one go.
//            launchThreads.add(launchThread);
//            launchThread.start();
//        }
//    }
//
//    @Override
//    public void onShutdownRequest() {
//        done = true;
//    }
//
//    @Override
//    public void onNodesUpdated(List<NodeReport> updatedNodes) {}
//
//    @Override
//    public float getProgress() {
//        // set progress to deliver to RM on next heartbeat
//        float progress = (float) numCompletedContainers.get()
//                / numTotalContainers;
//        return progress;
//    }
//
//    @Override
//    public void onError(Throwable e) {
//        done = true;
//        amRMClient.stop();
//    }
//}