//package hope;
//
///**
// * Created by 我自己 on 2016/7/26.
// */
//import com.google.common.base.Preconditions;
//import com.google.common.base.Supplier;
//import com.google.common.collect.ImmutableList;
//import java.io.IOException;
//import java.util.Collection;
//import java.util.List;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.classification.InterfaceAudience.Private;
//import org.apache.hadoop.classification.InterfaceAudience.Public;
//import org.apache.hadoop.classification.InterfaceStability.Stable;
//import org.apache.hadoop.service.AbstractService;
//import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
//import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
//import org.apache.hadoop.yarn.api.records.ContainerId;
//import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
//import org.apache.hadoop.yarn.api.records.Priority;
//import org.apache.hadoop.yarn.api.records.Resource;
//import org.apache.hadoop.yarn.client.api.NMTokenCache;
//import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
//import org.apache.hadoop.yarn.exceptions.YarnException;
//
//@Public
//@Stable
//public abstract class AMRMClient<T extends AMRMClient.ContainerRequest> extends AbstractService {
//    private static final Log LOG = LogFactory.getLog(AMRMClient.class);
//    private NMTokenCache nmTokenCache = NMTokenCache.getSingleton();
//
//    @Public
//    public static <T extends AMRMClient.ContainerRequest> AMRMClient<T> createAMRMClient() {
//        AMRMClientImpl client = new AMRMClientImpl();
//        return client;
//    }
//
//    @Private
//    protected AMRMClient(String name) {
//        super(name);
//    }
//
//    public abstract RegisterApplicationMasterResponse registerApplicationMaster(String var1, int var2, String var3) throws YarnException, IOException;
//
//    public abstract AllocateResponse allocate(float var1) throws YarnException, IOException;
//
//    public abstract void unregisterApplicationMaster(FinalApplicationStatus var1, String var2, String var3) throws YarnException, IOException;
//
//    public abstract void addContainerRequest(T var1);
//
//    public abstract void removeContainerRequest(T var1);
//
//    public abstract void releaseAssignedContainer(ContainerId var1);
//
//    public abstract Resource getAvailableResources();
//
//    public abstract int getClusterNodeCount();
//
//    public abstract List<? extends Collection<T>> getMatchingRequests(Priority var1, String var2, Resource var3);
//
//    public abstract void updateBlacklist(List<String> var1, List<String> var2);
//
//    public void setNMTokenCache(NMTokenCache nmTokenCache) {
//        this.nmTokenCache = nmTokenCache;
//    }
//
//    public NMTokenCache getNMTokenCache() {
//        return this.nmTokenCache;
//    }
//
//    public void waitFor(Supplier<Boolean> check) throws InterruptedException {
//        this.waitFor(check, 1000);
//    }
//
//    public void waitFor(Supplier<Boolean> check, int checkEveryMillis) throws InterruptedException {
//        this.waitFor(check, checkEveryMillis, 1);
//    }
//
//    public void waitFor(Supplier<Boolean> check, int checkEveryMillis, int logInterval) throws InterruptedException {
//        Preconditions.checkNotNull(check, "check should not be null");
//        Preconditions.checkArgument(checkEveryMillis >= 0, "checkEveryMillis should be positive value");
//        Preconditions.checkArgument(logInterval >= 0, "logInterval should be positive value");
//        int loggingCounter = logInterval;
//
//        while(true) {
//            if(LOG.isDebugEnabled()) {
//                LOG.debug("Check the condition for main loop.");
//            }
//
//            boolean result = ((Boolean)check.get()).booleanValue();
//            if(result) {
//                LOG.info("Exits the main loop.");
//                return;
//            }
//
//            --loggingCounter;
//            if(loggingCounter <= 0) {
//                LOG.info("Waiting in main loop.");
//                loggingCounter = logInterval;
//            }
//
//            Thread.sleep((long)checkEveryMillis);
//        }
//    }
//
//    public static class ContainerRequest {
//        final Resource capability;
//        final List<String> nodes;
//        final List<String> racks;
//        final Priority priority;
//        final boolean relaxLocality;
//        final String nodeLabelsExpression;
//
//        public ContainerRequest(Resource capability, String[] nodes, String[] racks, Priority priority) {
//            this(capability, nodes, racks, priority, true, (String)null);
//        }
//
//        public ContainerRequest(Resource capability, String[] nodes, String[] racks, Priority priority, boolean relaxLocality) {
//            this(capability, nodes, racks, priority, relaxLocality, (String)null);
//        }
//
//        public ContainerRequest(Resource capability, String[] nodes, String[] racks, Priority priority, boolean relaxLocality, String nodeLabelsExpression) {
//            Preconditions.checkArgument(capability != null, "The Resource to be requested for each container should not be null ");
//            Preconditions.checkArgument(priority != null, "The priority at which to request containers should not be null ");
//            Preconditions.checkArgument(relaxLocality || racks != null && racks.length != 0 || nodes != null && nodes.length != 0, "Can\'t turn off locality relaxation on a request with no location constraints");
//            this.capability = capability;
//            this.nodes = nodes != null?ImmutableList.copyOf(nodes):null;
//            this.racks = racks != null?ImmutableList.copyOf(racks):null;
//            this.priority = priority;
//            this.relaxLocality = relaxLocality;
//            this.nodeLabelsExpression = nodeLabelsExpression;
//        }
//
//        public Resource getCapability() {
//            return this.capability;
//        }
//
//        public List<String> getNodes() {
//            return this.nodes;
//        }
//
//        public List<String> getRacks() {
//            return this.racks;
//        }
//
//        public Priority getPriority() {
//            return this.priority;
//        }
//
//        public boolean getRelaxLocality() {
//            return this.relaxLocality;
//        }
//
//        public String getNodeLabelExpression() {
//            return this.nodeLabelsExpression;
//        }
//
//        public String toString() {
//            StringBuilder sb = new StringBuilder();
//            sb.append("Capability[").append(this.capability).append("]");
//            sb.append("Priority[").append(this.priority).append("]");
//            return sb.toString();
//        }
//    }
//}