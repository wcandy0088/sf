//package hope;
//
//import org.apache.commons.cli.*;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.DataOutputBuffer;
//import org.apache.hadoop.io.IOUtils;
//import org.apache.hadoop.security.Credentials;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.token.Token;
//import org.apache.hadoop.util.ExitUtil;
//import org.apache.hadoop.util.Shell;
//import org.apache.hadoop.yarn.api.ApplicationConstants;
//import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
//import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
//import org.apache.hadoop.yarn.api.records.ContainerId;
//import org.apache.hadoop.yarn.api.records.ContainerStatus;
//import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
//import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
//import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
//import org.apache.hadoop.yarn.applications.distributedshell.DSConstants;
//import org.apache.hadoop.yarn.client.api.AMRMClient;
//import org.apache.hadoop.yarn.client.api.TimelineClient;
//import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
//import org.apache.hadoop.yarn.conf.YarnConfiguration;
//import org.apache.hadoop.yarn.exceptions.YarnException;
//import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
//import org.apache.hadoop.yarn.util.ConverterUtils;
//
//import java.io.*;
//import java.lang.reflect.UndeclaredThrowableException;
//import java.nio.ByteBuffer;
//import java.security.PrivilegedExceptionAction;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEvent;
//
///**
// * Created by 我自己 on 2016/7/25.
// */
//public class ApplicationMaster {
//    //配置信息
//    private Configuration conf;
//    //日志文件
//    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
//    //应用程序尝试次数
//    protected ApplicationAttemptId appAttemptID;
//    //shell命令要执行的路径
//    private static final String shellCommandPath = "shellCommands";
//    //shell命令的参数路径
//    private static final String shellArgsPath = "shellArgs";
//    // 要执行的shell命令
//    private String shellCommand = "";
//    // 要执行的shell参数
//    private String shellArgs = "";
//    //存放命令参数的
//    private Map<String, String> shellEnv = new HashMap<String, String>();
//    //shell脚本位置
//    private String scriptPath = "";
//    //时间戳，创建一个本地资源
//    private long shellScriptPathTimestamp = 0;
//    //创建本地资源需要的文件大小
//    private long shellScriptPathLen = 0;
//    //时间域ID？
//    private String domainId = null;
//    //执行shell命令的containers数量
//    protected int numTotalContainers = 1;
//    // container的内存大小
//    private int containerMemory = 10;
//    // Containers的cores的数量
//    private int containerVirtualCores = 1;
//    // 优先级
//    private int requestPriority;
//    // Timeline Client
//    private TimelineClient timelineClient;
//    //用来存令牌的
//    private ByteBuffer allTokens;
//    // 在安全与非安全的模式下，节点job提交
//    private UserGroupInformation appSubmitterUgi;
//    // container完成的计数器(包括成功和失败)
//    private AtomicInteger numCompletedContainers = new AtomicInteger();
//    //container失败的数量
//    private AtomicInteger numFailedContainers = new AtomicInteger();
//    //具体RM分配给我们的container数量
//    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
//    //已经向RM请求的container数量
//    protected AtomicInteger numRequestedContainers = new AtomicInteger();
//    //构造方法
//    public ApplicationMaster(){
//        conf=new YarnConfiguration();
//    }
//    public static void main(String[] args){
//        boolean result=false;
//        try{
//            ApplicationMaster appMaster=new ApplicationMaster();
//            LOG.info("Initializing ApplicationMaster");
//            //初始化
//            boolean doRun=appMaster.init(args);
//            //防止用户只是用下help命令，程序就可以终止了
//            if(!doRun){
//                System.exit(0);
//            }
//            appMaster.run();
//        } catch (Throwable t) {
//            LOG.fatal("Error running ApplicationMaster", t);
//         //  LogManager.shutdown();
//            ExitUtil.terminate(1, t);
//        }
//    }
//    //主要逻辑
//    public void run()throws YarnException,IOException{
//        LOG.info("Starting ApplicationMaster");
//        Credentials credentials= UserGroupInformation.getCurrentUser().getCredentials();
//        DataOutputBuffer dob=new DataOutputBuffer();
//        credentials.writeTokenStorageToStream(dob);
//        //去除AM-》RM令牌，防止containers访问
//        Iterator<Token<?>> iter=credentials.getAllTokens().iterator();
//        LOG.info("drop tokens:");
//        while(iter.hasNext()){
//            Token<?> token=iter.next();
//            LOG.info(token);
//            if(token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)){
//                iter.remove();
//            }
//        }
//        allTokens=ByteBuffer.wrap(dob.getData(),0,dob.getLength());
//
//        //把令牌给程序
//        String appSubmitterUserName=System.getenv(ApplicationConstants.Environment.USER.name());
//        appSubmitterUgi=UserGroupInformation.createRemoteUser(appSubmitterUserName);
//        appSubmitterUgi.addCredentials(credentials);
//        publishApplicationAttemptEvent(timelineClient,appAttemptID.toString(),DSEvent.DS_APP_ATTEMPT_START,domainId,appSubmitterUgi);
//
//        AMRMClientAsync.CallbackHandler allocListener=new RMCallbackHandler();
//
//    }
//    //内部类RMCallbackHandler,RM的回掉程序     am rm client 的异步回掉函数（异步表示类似信箱，可以不用等待）
//    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler{
//        //在container上需要完成的逻辑，接受一个参数container的集合
//        @Override
//            public void onContainersCompleted(List<ContainerStatus> competedContatiners){
//                //从RM得到的container数量
//                LOG.info("container size from RM: "+competedContatiners.size());
//                //遍历所有的container
//                for(ContainerStatus containerStatus: competedContatiners){
//                       //这部分是发送container的各种信息
//                    LOG.info(appAttemptID + " got container status for containerID="
//                            + containerStatus.getContainerId() + ", state="
//                            + containerStatus.getState() + ", exitStatus="
//                            + containerStatus.getExitStatus() + ", diagnostics="
//                            + containerStatus.getDiagnostics());
//                    //判断container是否关闭
//                    int exitStatus=containerStatus.getExitStatus();
//                    if(0!=exitStatus){
//                        //container失败
//                        if(ContainerExitStatus.ABORTED !=exitStatus){
//                                //记录container完成的数量和失败的数量+1
//                            numCompletedContainers.incrementAndGet();
//                            numFailedContainers.incrementAndGet();
//                        }else{
//
//                            numAllocatedContainers.decrementAndGet();
//                            numRequestedContainers.decrementAndGet();
//                        }
//                    }else{
//                        //container顺利完成,日志记录完成的congtainer的信息
//                            numCompletedContainers.incrementAndGet();
//                            LOG.info("Container completed successfully." + ", containerId="
//                                    + containerStatus.getContainerId());
//                    }
//                    publishContainerEndEvent(timelineClient,containerStatus,domainId,appSubmitterUgi);
//                }
//                    //告诉那些container失败了
//            //获取数量
//            int askCount = numTotalContainers - numRequestedContainers.get();
//            numRequestedContainers.addAndGet(askCount);
//            if (askCount > 0) {
//                for (int i = 0; i < askCount; ++i) {
//                    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
//                    amRMClient.addContainerRequest(containerAsk);
//                }
//            }
//
//            if (numCompletedContainers.get() == numTotalContainers) {
//                done = true;
//            }
//            }
//
//    }
//    //发布container结束的动作,记录各种信息
//    private static void publishContainerEndEvent(final TimelineClient timelineClient,ContainerStatus container,String domainId,UserGroupInformation ugi){
//        final TimelineEntity entity = new TimelineEntity();
//        entity.setEntityId(container.getContainerId().toString());
//        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
//        entity.setDomainId(domainId);
//        entity.addPrimaryFilter("user", ugi.getShortUserName());
//        TimelineEvent event = new TimelineEvent();
//        event.setTimestamp(System.currentTimeMillis());
//        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
//        event.addEventInfo("State", container.getState().name());
//        event.addEventInfo("Exit Status", container.getExitStatus());
//        entity.addEvent(event);
//        try {
//            ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
//                @Override
//                public TimelinePutResponse run() throws Exception {
//                    return timelineClient.putEntities(entity);
//                }
//            });
//        } catch (Exception e) {
//            LOG.error("Container end event could not be published for "
//                            + container.getContainerId().toString(),
//                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
//        }
//    }
//    public static enum DSEntity {
//        DS_APP_ATTEMPT, DS_CONTAINER
//    }
//    private static void publishApplicationAttemptEvent(final TimelineClient timelineClient,
//                                                       String appAttemptId,
//                                                       org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEvent appEvent ,
//                                                       String domainId, UserGroupInformation ugi){
//        final TimelineEntity entity=new TimelineEntity();
//        entity.setEntityId(appAttemptId);
//        entity.setEntityType(org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEntity.DS_APP_ATTEMPT.toString());
//        entity.setDomainId(domainId);
//        entity.addPrimaryFilter("user",ugi.getShortUserName());
//        TimelineEvent event=new TimelineEvent();
//        event.setEventType(appEvent.toString());
//        event.setTimestamp(System.currentTimeMillis());
//        entity.addEvent(event);
//        try{
//            ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
//                @Override
//                public TimelinePutResponse run() throws Exception {
//                    return timelineClient.putEntities(entity);
//                }
//            });
//        }catch (Exception e){
//            LOG.error("App Attempt "
//                            + (appEvent.equals(org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster.DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
//                            + " event could not be published for "
//                            + appAttemptId.toString(),
//                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
//        }
//    }
//    //判断各个参数怎么样，并初始化
//    public boolean init(String[] args)throws ParseException,IOException{
//        //这是执行命令参数对比和帮助
//        Options opts=new Options();
//        opts.addOption("app_attempt_id", true,
//                "App Attempt ID. Not to be used unless for testing purposes");
//        opts.addOption("shell_env", true,
//                "Environment for shell script. Specified as env_key=env_val pairs");
//        opts.addOption("container_memory", true,
//                "Amount of memory in MB to be requested to run the shell command");
//        opts.addOption("container_vcores", true,
//                "Amount of virtual cores to be requested to run the shell command");
//        opts.addOption("num_containers", true,
//                "No. of containers on which the shell command needs to be executed");
//        opts.addOption("priority", true, "Application Priority. Default 0");
//        opts.addOption("debug", false, "Dump out debug information");
//
//        opts.addOption("help", false, "Print usage");
//        CommandLine cliParser=new GnuParser().parse(opts,args);
//        if(args.length==0){
//            //用来打印信息的
//            printUsage(opts);
//            throw new IllegalArgumentException("args.length is 0,master not initialize");
//        }
//        if (cliParser.hasOption("help")) {
//            printUsage(opts);
//            return false;
//        }
//        if (cliParser.hasOption("debug")) {
//            //debug模式开启
//            dumpOutDebugInfo();
//        }
//        Map<String, String> envs = System.getenv();
//        //如果配置文件里面没有container的id
//        if(!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())){
//            if(cliParser.hasOption("app_attempt_id")){
//                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
//                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
//            }else{
//                //没指定应用程序的尝试次数
//                throw new IllegalArgumentException(
//                        "Application Attempt Id not set in the environment");
//            }
//        }else{
//            //有containerID就取出来
//            ContainerId containerId = ConverterUtils.toContainerId(envs
//                    .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
//            appAttemptID = containerId.getApplicationAttemptId();
//        }
//        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
//            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
//                    + " not set in the environment");
//        }
//        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name())) {
//            throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name()
//                    + " not set in the environment");
//        }
//        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name())) {
//            throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT
//                    + " not set in the environment");
//        }
//        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name())) {
//            throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name()
//                    + " not set in the environment");
//        }
//
//        LOG.info("Application master for app" + ", appId="
//                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
//                + appAttemptID.getApplicationId().getClusterTimestamp()
//                + ", attemptId=" + appAttemptID.getAttemptId());
//        //这几行就是读取存放命令和存放命令参数的文件到内存准备着
//        if (!fileExist(shellCommandPath)
//                && envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION).isEmpty()) {
//            throw new IllegalArgumentException(
//                    "No shell command or shell script specified to be executed by application master");
//        }
//
//        if (fileExist(shellCommandPath)) {
//            shellCommand = readContent(shellCommandPath);
//        }
//
//        if (fileExist(shellArgsPath)) {
//            shellArgs = readContent(shellArgsPath);
//        }
//        //----------------------------------------------------------
//        //如果执行jar的时候-shell_env有参数，全加入到这个map集合中
//        if (cliParser.hasOption("shell_env")) {
//            String shellEnvs[] = cliParser.getOptionValues("shell_env");
//            for (String env : shellEnvs) {
//                env = env.trim();
//                int index = env.indexOf('=');
//                if (index == -1) {
//                    shellEnv.put(env, "");
//                    continue;
//                }
//                String key = env.substring(0, index);
//                String val = "";
//                if (index < (env.length() - 1)) {
//                    val = env.substring(index + 1);
//                }
//                shellEnv.put(key, val);
//            }
//        }
//        if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
//            scriptPath = envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);
//
//            if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
//                shellScriptPathTimestamp = Long.valueOf(envs
//                        .get(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
//            }
//            if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
//                shellScriptPathLen = Long.valueOf(envs
//                        .get(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN));
//            }
//            if (!scriptPath.isEmpty()
//                    && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
//                LOG.error("Illegal values in env for shell script path" + ", path="
//                        + scriptPath + ", len=" + shellScriptPathLen + ", timestamp="
//                        + shellScriptPathTimestamp);
//                throw new IllegalArgumentException(
//                        "Illegal values in env for shell script path");
//            }
//        }
//        if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN)) {
//            domainId = envs.get(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN);
//        }
//        //从配置文件里读出来的container内存，内核，数量，优先级
//        containerMemory = Integer.parseInt(cliParser.getOptionValue(
//                "container_memory", "10"));
//        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
//                "container_vcores", "1"));
//        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
//                "num_containers", "1"));
//        if (numTotalContainers == 0) {
//            throw new IllegalArgumentException(
//                    "Cannot run distributed shell with no containers");
//        }
//        requestPriority = Integer.parseInt(cliParser
//                .getOptionValue("priority", "0"));
//        //根据时间创建client
//        timelineClient = TimelineClient.createTimelineClient();
//        timelineClient.init(conf);
//        timelineClient.start();
//        return true;
//    }
//    private void printUsage(Options opts) {
//        new HelpFormatter().printHelp("ApplicationMaster", opts);
//    }
//    /**
//     * Dump out contents of $CWD and the environment to stdout for debugging
//     */
//    private void dumpOutDebugInfo() {
//
//        LOG.info("Dump debug output");
//        Map<String, String> envs = System.getenv();
//        for (Map.Entry<String, String> env : envs.entrySet()) {
//            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
//            System.out.println("System env: key=" + env.getKey() + ", val="
//                    + env.getValue());
//        }
//
//        BufferedReader buf = null;
//        try {
//            String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
//                    Shell.execCommand("ls", "-al");
//            buf = new BufferedReader(new StringReader(lines));
//            String line = "";
//            while ((line = buf.readLine()) != null) {
//                LOG.info("System CWD content: " + line);
//                System.out.println("System CWD content: " + line);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            IOUtils.cleanup(LOG, buf);
//        }
//    }
//    //判断本地文件存不存在
//    private boolean fileExist(String filePath) {return new File(filePath).exists();}
//    //读取指定文件的内容，返回内容
//    private String readContent(String filePath) throws IOException {
//        DataInputStream ds = null;
//        try {
//            ds = new DataInputStream(new FileInputStream(filePath));
//            return ds.readUTF();
//        } finally {
//            org.apache.commons.io.IOUtils.closeQuietly(ds);
//        }
//    }
//
//
//}
