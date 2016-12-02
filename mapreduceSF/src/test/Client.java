package test;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by 我自己 on 2016/7/27.
 * Client端编程，主要过程：
 * 连接集群
 * 获取程序id
 * 配置AM和程序的各种启动信息
 * 把配置信息提交给RM
 */
public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);
    public static String appName = "xcText";
    public static String AMjar = "AppMaster.jar";
    public static String yarnApplicationjar="yarnApplication.jar";
    public static String MyMR = "testxc,jar";
    private static final long clientStartTime = System.currentTimeMillis();
    private static long clientTimeout = 600000;
    public YarnClient yarnClient;
    public void run()throws Exception{
        //定义一个yarnclient
        yarnClient = YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        yarnClient.init(conf);
        //启动yarn
        yarnClient.start();
        LOG.info("yarnClient启动");
        //prepareTimelineDomain(conf);
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo("default");
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }
        //获得一个新的applicationID,在application里面
        YarnClientApplication application = yarnClient.createApplication();
        //构建ApplicationSubmissionContext用以提交作业
        ApplicationSubmissionContext appContext = application.getApplicationSubmissionContext();
        //获取作业ID
        ApplicationId appId = appContext.getApplicationId();


        //配置信息-----------------------------------------------------------
        //程序id
        appContext.setApplicationId(appId);
        //程序名称
        appContext.setApplicationName("xcText");
        //应用所属队列，不同应用可以属于不同的队列，使用不同的调度算法
        appContext.setQueue("default");
        //设置优先级,数值越小，优先级越高
        appContext.setPriority(Priority.newInstance(10));
        //AM是否由客户端启动（AM既可以运行在YARN平台之上，也可运行在YARN平台之外）
        //运行在YARN平台之上的AM通常用RM启动，其运行所需的资源受YARN控制,true就是不贵RM管，false相反
        appContext.setUnmanagedAM(false);
        //应用完成后是否取消安全令牌，建议true
        appContext.setCancelTokensWhenComplete(true);
        //AM启动失败后，最大的尝试次数
        appContext.setMaxAppAttempts(1);
        //应用类型
        appContext.setApplicationType("YARN");
        //是否保持container连接
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        //节点_标签_表达式
        appContext.setNodeLabelExpression(null);
        //启动AM所需要的资源（内存，cpu内核）,默认是10，1
        appContext.setResource(Resource.newInstance(350, 1));
        //-----------------------------------------------------------------------


        //localResources运行所需的本地资源
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        //拷贝AM的jar包到hdfs
        addToLocalResources(fs, AMjar, AMjar, appId.toString(), localResources, null);
        //拷贝计算程序到hdfs
        addToLocalResources(fs,yarnApplicationjar,yarnApplicationjar,appId.toString(),localResources,null);
        //把计算逻辑的jar也传到hdfs上
//        Path mytest=new Path(MyMR);
//        String hdfsMytest=appName+"/"+appId.toString()+"/"+MyMR;
//        Path hdfMr=new Path(fs.getHomeDirectory(),hdfsMytest);
//        fs.copyFromLocalFile(false,true,mytest,hdfMr);
//        FileStatus jarFileStatus=fs.getFileStatus(hdfMr);
//        long hdfsjarScriptLen=jarFileStatus.getLen();
//        long hdfsjarScriptTimeStamp=jarFileStatus.getModificationTime();
        //-------------------------------------------------------------------------

        //event AM运行时所需要的环境变量,这个也可以自己设置,这个地方要把AM的jar包写到classpath上
        Map<String, String> env = new HashMap<String, String>();
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "./log4j.properties");

//-------------------------------------------------------------------
        // add the runtime classpath needed for tests to work，把testwork添加到classpath里
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        env.put("CLASSPATH",classPathEnv.toString());

        //-----------------------------------------------------------------------
        //comands启动AM的shell命令，这里传输的是AM的参数，可以自己随便定
        List<String> commands = new ArrayList<String>();
        Vector<CharSequence> vargs = new Vector<CharSequence>();
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java ");
//        stringBuffer.append("java ");
        stringBuffer.append("-Xmx350m ");
        //ApplicationMaster的jar包Main类
        stringBuffer.append("xcAppMaster ");
//        stringBuffer.append("--container-memory 350 ");
//        stringBuffer.append("--container-cores 1 ");
//        stringBuffer.append("--num-containers 1 ");
//        stringBuffer.append("--priority 0");
        System.out.println("AM启动命令:" + stringBuffer.toString());
        commands.add(stringBuffer.toString());

//-------------------------------------------------------------------------------
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        Resource capability=Resource.newInstance(350,1);
        appContext.setResource(capability);
        //------------------------------------------------------
        //tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }
            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }
        //-------------------------------------
        appContext.setAMContainerSpec(amContainer);
        //启动AM需要的Container,这里面有好多信息
        /*
            启动AM容器所需要的上下文，主要包括：
            tokens: AM所持有的安全令牌；
            serviceData: 应用私有的数据，是一个Map，键为数据名，值为数据的二进制块；
            environment: AM使用的环境变量；
            commands: 启动AM的命令列表；
            applicationACLs：应程序访问控制列表；
            localResource: AM启动需要的本地资源列表，主要是一些外部文件、压缩包等。
        */


        //AM调度资源和配置信息
//        ResourceRequest amReq= Records.newRecord(ResourceRequest.class);
//        amReq.setResourceName(ResourceRequest.ANY);
//        amReq.setCapability(Resource.newInstance(10, 1));
//        amReq.setRelaxLocality(true);
//        amReq.setNodeLabelExpression(null);
//        appContext.setAMContainerResourceRequest(amReq);
        LOG.info("\n作业配置信息\nApplication Id:" + appContext.getApplicationId() + "\n"
                + "Application Name:" + appContext.getApplicationName() + "\n"
                + "Application Queue:" + appContext.getQueue() + "\n"
                + "Application Type:" + appContext.getApplicationType() + "\n"
                + "Application Keep Containers Across Application Attempts:" + appContext.getKeepContainersAcrossApplicationAttempts() + "\n"
                + "Application Priority:" + appContext.getPriority() + "\n"
                + "Application not on Yarn:" + appContext.getUnmanagedAM() + "\n"
                + "Application Cancel Tokens When Complete:" + appContext.getCancelTokensWhenComplete() + "\n"
                + "Application Max AppAttempts:" + appContext.getMaxAppAttempts() + "\n"
                + "Application Node Label Expression:" + appContext.getNodeLabelExpression() + "\n");
        //将应用提交到ResouceManager
        yarnClient.submitApplication(appContext);
        LOG.info("ApplicationMaster提交");
        monitorApplication(appId,yarnClient);
    }
    public static void main(String[] args) throws Exception {
        new Client().run();

    }


    //用于copy jar包到hdfs
    private  void addToLocalResources(FileSystem fs
            , String fileSrcPath
            , String fileDstPath
            , String appId
            , Map<String, LocalResource> localResource
            , String resources) throws IOException {
        //AMjar包在hdfs的位置
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        //获得路径
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri())
                , LocalResourceType.FILE
                , LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime()
        );
        //AM的jar包名字，AMjar包在HDFS的路径
        localResource.put(fileDstPath, scRsrc);
    }

//监控
    private  boolean monitorApplication(ApplicationId appId, YarnClient yarnClient)
            throws YarnException, IOException {
        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application超时kill程序");
                    forceKillApplication(appId);
                return false;
            }
        }
    }
//    private  void prepareTimelineDomain(Configuration conf) {
//        String domainId=null;
//        TimelineClient timelineClient = null;
//        if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
//                YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
//            timelineClient = TimelineClient.createTimelineClient();
//            timelineClient.init(conf);
//            timelineClient.start();
//        } else {
//            LOG.warn("Cannot put the domain " + domainId +
//                    " because the timeline service is not enabled");
//            return;
//        }
//        try {
//            //TODO: we need to check and combine the existing timeline domain ACLs,
//            //but let's do it once we have client java library to query domains.
//            TimelineDomain domain = new TimelineDomain();
//            domain.setId(domainId);
//            domain.setReaders(" ");
//            domain.setWriters(" ");
//            timelineClient.putDomain(domain);
//        } catch (Exception e) {
//            LOG.error("Error when putting the timeline domain", e);
//        } finally {
//            timelineClient.stop();
//        }
//    }
    private void forceKillApplication(ApplicationId appId)throws YarnException,IOException{
        yarnClient.killApplication(appId);
    }
}
