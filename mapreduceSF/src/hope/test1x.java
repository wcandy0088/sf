package hope;


import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.distributedshell.DSConstants;
import org.apache.hadoop.yarn.applications.distributedshell.Log4jPropertyHelper;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.security.token.Token;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by 我自己 on 2016/7/22.
 */
public class test1x {
    private static final Log LOG= LogFactory.getLog(test1x.class);
    private  String shellScriptPath="";
    private String appName = "";
    private boolean keepContainers=false;;
    private String appMasterJar="";
    private static final String appMasterJarPath = "AppMaster.jar";
    private final String SCRIPT_PATH = "ExecScript";
    //要执行的shell命令
    private  String shellCommand = "";
    private  final String shellCommandPath = "shellCommands";
    private  final String shellArgsPath = "shellArgs";
    //shell脚本的参数
    private  String[] shellArgs = new String[] {};
    // container内存
    private  int containerMemory = 10;
    //container的core数量
    private  int containerVirtualCores = 1;
    //在shell脚本中需要执行的container号
    private  int numContainers = 1;
    // shell启动用的container
    private  int shellCmdPriority = 0;
    //shell运行的参数
    private  Map<String, String> shellEnv = new HashMap<String, String>();
    //是否启动调试
    private  boolean debugFlag=false;
    // 运行ApplicationMaster的内存大小
    private  int amMemory = 10;
    // 运行ApplicationMaster的内核数量
    private  int amVCores = 1;
    // 程序优先级
    private  int amPriority = 0;
    //程序队列
    private  String amQueue = "";
    //命令行的选项
    private Options opts;
    private Configuration conf;
    private YarnClient yarnClient;
    //主方法
    private final String appMasterMainClass;
    private String nodeLabelExpression = null;
    //客户端超市时间，超时就kill app
    private long clientTimeout = 600000;
    // Timeline domain writer access control
    private String modifyACLs = null;
    // Timeline domain ID
    private String domainId = null;
    // Flag to indicate whether to create the domain of the given ID
    private boolean toCreateDomain = false;
    // Timeline domain reader access control
    private String viewACLs = null;
    private long attemptFailuresValidityInterval = -1;
    // log4j.properties配置文件地址
    // 如果有就加入classpath
    private String log4jPropFile = "";
    // Start time for client
    private final long clientStartTime = System.currentTimeMillis();
    test1x(String appMasterMainClass,Configuration conf){
        this.conf=conf;
        this.appMasterMainClass=appMasterMainClass;
        //获取yarnclient对象
        yarnClient=YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts=new Options();
        opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("shell_command", true, "Shell command to be executed by " +
                "the Application Master. Can only specify either --shell_command " +
                "or --shell_script");
        opts.addOption("shell_script", true, "Location of the shell script to be " +
                "executed. Can only specify either --shell_command or --shell_script");
        opts.addOption("shell_args", true, "Command line args for the shell script." +
                "Multiple args can be separated by empty space.");
        opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES);
        opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("shell_cmd_priority", true, "Priority for the shell command containers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("keep_containers_across_application_attempts", false,
                "Flag to indicate whether to keep containers across application attempts." +
                        " If the flag is true, running containers will not be killed when" +
                        " application attempt fails and these containers will be retrieved by" +
                        " the new application attempt ");
        opts.addOption("attempt_failures_validity_interval", true,
                "when attempt_failures_validity_interval in milliseconds is set to > 0," +
                        "the failure number will not take failures which happen out of " +
                        "the validityInterval into failure count. " +
                        "If failure count reaches to maxAppAttempts, " +
                        "the application will be failed.");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("domain", true, "ID of the timeline domain where the "
                + "timeline entities will be put");
        opts.addOption("view_acls", true, "Users and groups that allowed to "
                + "view the timeline entities in the given domain");
        opts.addOption("modify_acls", true, "Users and groups that allowed to "
                + "modify the timeline entities in the given domain");
        opts.addOption("create", false, "Flag to indicate whether to create the "
                + "domain specified with -domain.");
        opts.addOption("help", false, "Print usage");
        opts.addOption("node_label_expression", true,
                "Node label expression to determine the nodes"
                        + " where all the containers of this application"
                        + " will be allocated, \"\" means containers"
                        + " can be allocated anywhere, if you don't specify the option,"
                        + " default node_label_expression of queue will be used.");
    }


    //主要工作步骤
    public  boolean run()throws IOException,YarnException{
        //初始化和启动yarnClient
        LOG.info("Running Client");
        yarnClient.start();
        //设置客户端，客户端需要创建一个应用程序，并得到应用的ID
        YarnClientApplication app=yarnClient.createApplication();
        GetNewApplicationResponse appResponse=app.getNewApplicationResponse();
        //制定RM需要的信息
        // 程序编号，程序名称
        //队列优先级
        //提交请求的用户

        ApplicationSubmissionContext appContext=app.getApplicationSubmissionContext();
        ApplicationId appId=appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);
//        appContext.setApplicationId(appId);

        //指定应用程序需要的本地资源(jar包也算)
        Map<String,LocalResource> localResources=new HashMap<String,LocalResource>();
        LOG.info("copy localResource from local");
        //拷贝jar包
        //创建本地的path指向jar包
        FileSystem fs=FileSystem.get(conf);
        addToLocalResources(fs,appMasterJar,appMasterJarPath,appId.toString(),localResources,null);

        //shell脚本已经在container上
        //在container执行
        //先复制到文件系统
        String hdfsShellScription="";
        long hdfsShellScriptLen=0;
        long hdfsShellScriptTimesetamp=0;
        if(!shellScriptPath.isEmpty()){
            Path shellSrc=new Path(shellScriptPath);
            String shellPathSuffix=appName+"/"+appId.toString()+"/"+SCRIPT_PATH;
            Path shellDst=new Path(fs.getHomeDirectory(),shellPathSuffix);
            fs.copyFromLocalFile(false,true,shellSrc,shellDst);
            hdfsShellScription=shellDst.toUri().toString();
            FileStatus shellFileStatus=fs.getFileStatus(shellDst);
            hdfsShellScriptLen=shellFileStatus.getLen();
            hdfsShellScriptTimesetamp=shellFileStatus.getModificationTime();
        }
        if(shellCommand.isEmpty()){
            addToLocalResources(fs,null,shellCommandPath,appId.toString(),localResources,shellCommand);
        }
        if(shellArgs.length>0){
            addToLocalResources(fs,null,shellArgsPath,appId.toString(),localResources,StringUtils.join(shellArgs," "));
        }
        //为主应用程序设定环境
        LOG.info("set main environment");
        addToLocalResources(fs,null,shellArgsPath,appId.toString(),localResources,shellCommand);
        Map<String,String> env=new HashMap<String,String>();
        //把本地的shell资源放到主程序的环境里
        //通过env的信息，程序将在容器中执行shell脚本，并使用本地资源
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION,hdfsShellScription);
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP,Long.toString(hdfsShellScriptTimesetamp));
        env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN,Long.toString(hdfsShellScriptLen));

        //把jar包加入classpath（这样在哪个目录下执行都可以直接读到，jar包运行无关运行jar的目录）
        //hadoop需要的的环境资源都在env
        //设置对于这个jar包，所有必须类的路径
        StringBuffer classPathEnv=new StringBuffer(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for(String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration
                .DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)){
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

        //设定必要的命令来启动AM
        Vector<CharSequence> vargs=new Vector<CharSequence>(30);

        //一套java可执行命令
        LOG.info("set up AM start command");
        vargs.add("--container_memory "+String.valueOf(containerMemory));
        vargs.add("--container_vcores "+String.valueOf(containerVirtualCores));
        vargs.add("--num_containers "+String.valueOf(numContainers));
        vargs.add("--priority "+String.valueOf(shellCmdPriority));

        for(Map.Entry<String,String> entry : shellEnv.entrySet()){
            vargs.add("--shell_env "+entry.getKey()+"="+entry.getValue());
        }
        if(debugFlag){
            vargs.add("--debug");
        }
        vargs.add("1>"+ApplicationConstants.LOG_DIR_EXPANSION_VAR+"/AppMaster.stdout");
        vargs.add("2>"+ApplicationConstants.LOG_DIR_EXPANSION_VAR+"/AppMaster.stderr");

        //获得最后的命令
        StringBuilder command=new StringBuilder();
        for(CharSequence str : vargs){
            command.append(str).append(" ");
        }
        LOG.info("completed AM command"+command.toString());
        List<String> commands=new ArrayList<String>();
        commands.add(command.toString());

        //设置container，用于启动AM
        ContainerLaunchContext amContainer=ContainerLaunchContext.newInstance(localResources,env,commands,null,null,null);

        //设定container的属性，目前可以制定的是内存和内核
        Resource capability=Resource.newInstance(amMemory,amVCores);
        appContext.setResource(capability);

        if(UserGroupInformation.isSecurityEnabled()) {
            //设定安全令牌为HDFS和MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException("not get master kerberos in RM ");
            }
            //获取文件系统对象
            final Token<?> tokens[]=fs.addDelegationTokens(tokenRenewer,credentials);
            if(tokens != null){
                for(Token<?> token :tokens){
                    LOG.info("Got dr for"+fs.getUri()+";"+token);
                }
            }
            DataOutputBuffer dob=new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens=ByteBuffer.wrap(dob.getData(),0,dob.getLength());
            amContainer.setTokens(fsTokens);
        }
        appContext.setAMContainerSpec(amContainer);

        //客户端准备提交申请制定的优先级和队列
        //指定优先级
        Priority pri=Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        //设置要提交的队列
        appContext.setQueue(amQueue);

        //提交程序到RM
        yarnClient.submitApplication(appContext);
        return monitorApplication(appId);
        //RM接受提交，通过规定分配container，最终建立和启动分配的container
        //获取我们想看的程序id的报告
        //ApplicationReport report=yarnClient.getApplicationReport(appId);
        //收到的内容有，应用id，应用队列，提交申请，提交用户，提交程序的启动时间

    }
    private  void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix =
                appName + "/" + appId + "/" + fileDstPath;
        //hdfs上的路径是以appName + "/" + appId + "/" + fileDstPath，把jar包放在这里了
        Path dst =
                new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                //文件权限
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);

    }
    public static void main(String[] args){
        boolean result=false;
        try{
            test1x client=new test1x();
            LOG.info("client initializing");
            try{
                boolean doRun=client.init(args);
                if(!doRun){
                    System.exit(0);
                }
            }catch (IllegalArgumentException e){
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result=client.run();

        }  catch(Throwable t){
            LOG.fatal("Error running client",t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }
    public test1x(Configuration conf) throws Exception  {
        this(
                "org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster",
                conf);
    }
    public test1x() throws Exception  {
        this(new YarnConfiguration());
    }


    public boolean init(String[] args) throws ParseException{
        CommandLine cliParser = new GnuParser().parse(opts, args);
        if(args.length==0){
            throw new IllegalArgumentException("args.length is 0");
        }
        if(cliParser.hasOption("log_properties")){
            String log4jPath=cliParser.getOptionValue("log_properties");
            try{
                Log4jPropertyHelper.updateLog4jConfiguration(test1x.class,log4jPath);
            }catch (Exception e){
                LOG.warn("not set log4j properties");
            }
        }
        if(cliParser.hasOption("help")){
            printUsage();
            return false;
        }
        if(cliParser.hasOption("debug")){
            debugFlag=true;
        }
        if(cliParser.hasOption("keep_containers_across_application_attempts")){
            LOG.info("keep_containers_across_application_attempts");
            keepContainers = true;
        }
        appName = cliParser.getOptionValue("appname", "DistributedShell");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));
        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        appMasterJar = cliParser.getOptionValue("jar");

        if (!cliParser.hasOption("shell_command") && !cliParser.hasOption("shell_script")) {
            throw new IllegalArgumentException(
                    "No shell command or shell script specified to be executed by application master");
        } else if (cliParser.hasOption("shell_command") && cliParser.hasOption("shell_script")) {
            throw new IllegalArgumentException("Can not specify shell_command option " +
                    "and shell_script option at the same time");
        } else if (cliParser.hasOption("shell_command")) {
            shellCommand = cliParser.getOptionValue("shell_command");
        } else {
            shellScriptPath = cliParser.getOptionValue("shell_script");
        }
        if (cliParser.hasOption("shell_args")) {
            shellArgs = cliParser.getOptionValues("shell_args");
        }
        if (cliParser.hasOption("shell_env")) {
            String envs[] = cliParser.getOptionValues("shell_env");
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length()-1)) {
                    val = env.substring(index+1);
                }
                shellEnv.put(key, val);
            }
        }
        shellCmdPriority = Integer.parseInt(cliParser.getOptionValue("shell_cmd_priority", "0"));

        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));


        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

        nodeLabelExpression = cliParser.getOptionValue("node_label_expression", null);

        clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

        attemptFailuresValidityInterval =
                Long.parseLong(cliParser.getOptionValue(
                        "attempt_failures_validity_interval", "-1"));

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        // Get timeline domain options
        if (cliParser.hasOption("domain")) {
            domainId = cliParser.getOptionValue("domain");
            toCreateDomain = cliParser.hasOption("create");
            if (cliParser.hasOption("view_acls")) {
                viewACLs = cliParser.getOptionValue("view_acls");
            }
            if (cliParser.hasOption("modify_acls")) {
                modifyACLs = cliParser.getOptionValue("modify_acls");
            }
        }

        return true;
    }
    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
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
                }
                else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;
            }
        }

    }
    /**
     * Kill a submitted application by sending a call to the ASM
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
        // the same time.
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
    }
}
