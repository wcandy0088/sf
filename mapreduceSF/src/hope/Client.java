//package hope;
//
//import org.apache.commons.cli.HelpFormatter;
//import org.apache.commons.cli.Option;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.permission.FsPermission;
//import org.apache.commons.io.IOUtils;
//import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
//import org.apache.hadoop.yarn.api.impl.pb.client.ApplicationClientProtocolPBClientImpl;
//import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
//import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
//import org.apache.hadoop.yarn.api.records.*;
//import org.apache.hadoop.yarn.client.api.YarnClient;
//import org.apache.hadoop.yarn.client.api.YarnClientApplication;
//import org.apache.hadoop.yarn.conf.YarnConfiguration;
//import org.apache.hadoop.yarn.exceptions.YarnException;
//import org.apache.commons.cli.Options;
//import org.apache.hadoop.yarn.util.ConverterUtils;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by 我自己 on 2016/7/21.
// * 初次尝试写yarn程序
// */
//public class Client {
//    //日志
//    private static final Log LOG= LogFactory.getLog(Client.class);
//    //配置文件
//    private Configuration conf;
//    //用来申请一个新的应用，与RM/ASM交互
//    private YarnClient yarnClient;
//    private String appName="xc_test1";
//    //程序的优先级
//    private int amPriority=0;
//    //程序队列
//    private  String amQueue="xc";
//    //程序的内存
//    private int amMemory=10;
//    //程序虚拟内核
//    private int amVCores=2;
//    //jar包文件
//    private String appMasterJar="";
//    //jar包里面的主方法
//    private String appMasterMainClass;
//    //要执行的shell命令
//    private String shellCommand="";
//    //shell脚本的位置
//    private  String shellScriptPath="";
//    //shell命令的参数
//    private String[] shellArgs=new String[]{};
//    //这个我不知道是干嘛的
//    private Map<String, String> shellEnv = new HashMap<String, String>();
//    //shell命令的优先级
//    private int shellCmdPriority = 0;
//    //container的内存
//    private int containerMemory = 10;
//    //container的核数
//    private int containerVirtualCores = 1;
//    //container的数量
//    private int numContainers = 1;
//    //这个我也不知道是干啥的
//    private String nodeLabelExpression = null;
//    private String log4jPropFile = "";
//    //运行的开始时间
//    private final long clientStartTime = System.currentTimeMillis();
//    //运行的超时时间
//    private long clientTimeout = 600000;
//    //是否对于containers保持
//    private boolean keepContainers = false;
//    //尝试失败的有效间隔
//    private long attemptFailuresValidityInterval = -1;
//    // Debug flag
//    boolean debugFlag = false;
//
//    // Timeline domain ID
//    private String domainId = null;
//
//    // Flag to indicate whether to create the domain of the given ID
//    private boolean toCreateDomain = false;
//
//    // Timeline domain reader access control
//    private String viewACLs = null;
//
//    // Timeline domain writer access control
//    private String modifyACLs = null;
//
//    // Command line options
//    private Options opts;
//
//    private static final String shellCommandPath = "shellCommands";
//    private static final String shellArgsPath = "shellArgs";
//    private static final String appMasterJarPath = "AppMaster.jar";
//    // Hardcoded path to custom log_properties
//    private static final String log4jPath = "log4j.properties";
//
//    public static final String SCRIPT_PATH = "ExecScript";
//    //构造函数
//    Client(Configuration conf)throws Exception{
//        this("hope.ApplicationMaster", conf);
//    }
//     Client(String appMsterMainClass,Configuration conf){
//        this.conf=conf;
//         this.appMasterMainClass=appMsterMainClass;
//         yarnClient=YarnClient.createYarnClient();
//         yarnClient.init(conf);
//         opts=new Options();
//         opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
//         opts.addOption("priority", true, "Application Priority. Default 0");
//         opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
//         opts.addOption("timeout", true, "Application timeout in milliseconds");
//         opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
//         opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
//         opts.addOption("jar", true, "Jar file containing the application master");
//         opts.addOption("shell_command", true, "Shell command to be executed by " +
//                 "the Application Master. Can only specify either --shell_command " +
//                 "or --shell_script");
//         opts.addOption("shell_script", true, "Location of the shell script to be " +
//                 "executed. Can only specify either --shell_command or --shell_script");
//         opts.addOption("shell_args", true, "Command line args for the shell script." +
//                 "Multiple args can be separated by empty space.");
//         opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES);
//         opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
//         opts.addOption("shell_cmd_priority", true, "Priority for the shell command containers");
//         opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
//         opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
//         opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
//         opts.addOption("log_properties", true, "log4j.properties file");
//         opts.addOption("keep_containers_across_application_attempts", false,
//                 "Flag to indicate whether to keep containers across application attempts." +
//                         " If the flag is true, running containers will not be killed when" +
//                         " application attempt fails and these containers will be retrieved by" +
//                         " the new application attempt ");
//         opts.addOption("attempt_failures_validity_interval", true,
//                 "when attempt_failures_validity_interval in milliseconds is set to > 0," +
//                         "the failure number will not take failures which happen out of " +
//                         "the validityInterval into failure count. " +
//                         "If failure count reaches to maxAppAttempts, " +
//                         "the application will be failed.");
//         opts.addOption("debug", false, "Dump out debug information");
//         opts.addOption("domain", true, "ID of the timeline domain where the "
//                 + "timeline entities will be put");
//         opts.addOption("view_acls", true, "Users and groups that allowed to "
//                 + "view the timeline entities in the given domain");
//         opts.addOption("modify_acls", true, "Users and groups that allowed to "
//                 + "modify the timeline entities in the given domain");
//         opts.addOption("create", false, "Flag to indicate whether to create the "
//                 + "domain specified with -domain.");
//         opts.addOption("help", false, "Print usage");
//         opts.addOption("node_label_expression", true,
//                 "Node label expression to determine the nodes"
//                         + " where all the containers of this application"
//                         + " will be allocated, \"\" means containers"
//                         + " can be allocated anywhere, if you don't specify the option,"
//                         + " default node_label_expression of queue will be used.");
//    }
//    Client()throws Exception{
//        this(new YarnConfiguration());
//    }
//    //这就是个打印控制台的东西
//    private void printUsage(){new HelpFormatter().printHelp("Client",opts);}
//
//    public boolean run()throws IOException,YarnException{
//        LOG.info("Client start");
//        yarnClient.start();
//        //获得yarn集群的信息
//        YarnClusterMetrics clusterMetrics=yarnClient.getYarnClusterMetrics();
//        //查看集群运行的节点信息
//        List<NodeReport> clusterNodeReports=yarnClient.getNodeReports(NodeState.RUNNING);
//        LOG.info("runing node content");
//        for(NodeReport node:clusterNodeReports){
//            LOG.info("Node_ID："+node.getNodeId()+
//                    ",Node_Adress:"+node.getHttpAddress()+
//                    ",Node_RackName:"+node.getRackName()+
//                    ",nodeNumContainers"+node.getNumContainers());
//        }
//        QueueInfo queueInfo=yarnClient.getQueueInfo(this.amQueue);
//        LOG.info("queueName:"+queueInfo.getQueueName()+
//                  ",queueMaxCapacity:"+queueInfo.getMaximumCapacity()+
//                  ",thisQueueCapacity:"+queueInfo.getCurrentCapacity()+
//                  ",queueApplicationCount:"+queueInfo.getApplications().size());
//        //初始化
//        if(domainId !=null && domainId.length()>0 && toCreateDomain){
//
//        }
//        //client:第一步骤----获取一个新的应用程序的ID
//        YarnClientApplication app=yarnClient.createApplication();
//        //获取与AM通讯返回的对象
//        GetNewApplicationResponse appResponse=app.getNewApplicationResponse();
//        //获取最大内存
//        int maxMem=appResponse.getMaximumResourceCapability().getMemory();
//        LOG.info("Max Memory:"+maxMem);
//        //如果你设定的内存大于最大内存,那么用最大内存跑
//        if(amMemory>maxMem){
//            LOG.info("Memory exceed MaxMemory");
//            amMemory=maxMem;
//        }
//        //获取虚拟内核的数量
//        int maxVCoores=appResponse.getMaximumResourceCapability().getVirtualCores();
//        LOG.info("Max Cores is "+maxVCoores);
//        //如果你设定的内核数量大于最大内核，那么用最大的内核数量跑
//        if(amVCores>maxVCoores){
//            amVCores=maxVCoores;
//        }
//        //定义程序名称
//        ApplicationSubmissionContext appContext=app.getApplicationSubmissionContext();
//        ApplicationId appID=appContext.getApplicationId();
//        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
//        appContext.setApplicationName(appName);
//        //尝试失败有效间隔,上面写的-1，采用默认值
//        if(attemptFailuresValidityInterval>=0){
//            appContext.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
//        }
//        //jar包是在本地资源上，这里指定本地资源
//        Map<String,LocalResource> locaResources=new HashMap<>();
//        //从发布的这台机器把jar包放到hdfs上
//        FileSystem fs= FileSystem.get(conf);
//        //文件系统，jar包文件名，jar的住方法，程序id，需要的资源
//        addToLocalResources(fs,appMasterJar,appMasterJarPath,appID.toString(),locaResources,null);
//        LOG.info("biu biu biu jar file to hdfs");
//
//        if(!log4jPropFile.isEmpty()){
//            addToLocalResources(fs,log4jPropFile,log4jPath,appID.toString(),locaResources,null);
//        }
//
//    }
//    //把程序用到的程序或者文件加载hdfs里
//    private void addToLocalResources(FileSystem fs, String fileSrcPath,
//                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
//                                     String resources) throws IOException {
//        String suffix =
//                appName + "/" + appId + "/" + fileDstPath;
//        //hdfs上的路径是以appName + "/" + appId + "/" + fileDstPath，把jar包放在这里了
//        Path dst =
//                new Path(fs.getHomeDirectory(), suffix);
//        if (fileSrcPath == null) {
//            FSDataOutputStream ostream = null;
//            try {
//                //文件权限
//                ostream = FileSystem
//                        .create(fs, dst, new FsPermission((short) 0710));
//                ostream.writeUTF(resources);
//            } finally {
//                IOUtils.closeQuietly(ostream);
//            }
//        } else {
//            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
//        }
//        FileStatus scFileStatus = fs.getFileStatus(dst);
//        LocalResource scRsrc =
//                LocalResource.newInstance(
//                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
//                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
//                        scFileStatus.getLen(), scFileStatus.getModificationTime());
//        localResources.put(fileDstPath, scRsrc);
//
//    }
//}
