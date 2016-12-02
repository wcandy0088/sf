import Jama.Matrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;


/**
 * Created by 我自己 on 2016/7/18.
 */
public class test1 extends Configured implements Tool{
    //第一轮迭代的map阶段，计算样本特征向量的sigmoid
    public static class Map extends Mapper<LongWritable, Text  ,IntWritable , Text> {
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
            StringBuffer strb=new StringBuffer();
            strb.append("1.0\t");
            strb.append(value.toString());
            String[] temp=strb.toString().split("\t");
            context.write(new IntWritable(1),new Text(sigmoid(temp).toString()));
        }

        //传入一条样本，把样本变成变成sigmoid计算后的向量，其中最后一个值是样本的真实分类
        public  StringBuffer sigmoid(String[] temp){
            StringBuffer strb=new StringBuffer();
            for(int x=0;x<temp.length-1;x++){
                strb.append(1.0/(1+Math.exp(-Double.parseDouble(temp[x])))+"\t");
            }
            strb.append(temp[temp.length-1]);
            return strb;
        }
    }

    //根据每个节点上的少量数据进行预处理，得到每个节点的全部样本的逻辑回归参数（针对小样本做逻辑回归）
    public static class Combiner extends Reducer<IntWritable,Text,IntWritable,Text>{
        public void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
            //还是只算一次吧，迭代用MRMR来做
            //(3*1)
            Matrix weight=new Matrix(3,1,1);
            int sign=0;
            double alpha=0.01;
            for(Text line: values){
                //对每行数据进行拆分，构建矩阵
                String[] lineSplit=line.toString().split("\t");
                double[] lineDouble=new double[lineSplit.length-1];
                for(int x=0;x<lineSplit.length-1;x++){
                       lineDouble[x]=Double.parseDouble(lineSplit[x]);
                }
                //(1 *3)
                Matrix sample= new Matrix(lineDouble,1);
                //(1*1)=(1*3) *(3*1)
                Matrix h=sample.times(weight);
                //(1,1)
                Matrix sampleresult=new Matrix(1,1,Double.parseDouble(lineSplit[lineSplit.length-1]));
                //(1*1)
                Matrix error=sampleresult.minus(h);
                //(3*1)=(3*1)-{(3*1) * (1*1) }
                weight=weight.minus(sample.transpose().times(error).times(alpha).times(-1.0/100));
            }
           double[][] temp= weight.transpose().getArray();
            StringBuffer stringBuffer=new StringBuffer();
            for(int x=0;x<Integer.parseInt(context.getConfiguration().get("feture"));x++){
                stringBuffer.append(temp[0][x]+"\t");
            }
            context.write(new IntWritable(1),new Text(stringBuffer.toString()));

        }
    }
    //迭代用的combiner，加载中间结果,需要随机选取样本
    public static class Combiner1 extends Reducer<IntWritable,Text,IntWritable,Text>{
        public void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
            Path[] cacheFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
            FileSystem fsopen = FileSystem.getLocal(context.getConfiguration());
            int singing=0;
            FSDataInputStream in =fsopen.open(cacheFiles[singing]);
            String[] temps=in.readLine().split("\t");
            //获得参数
            String[] b= Arrays.copyOfRange(temps,1,temps.length);
            double[] c=new double[b.length];
            for(int x=0;x<c.length;x++){
                c[x]=Double.parseDouble(b[x]);
            }
           // (3*1)
            Matrix weight=new Matrix(3,1,1);
            for(int x=0;x<b.length;x++){
                weight.set(x,0,c[x]);
            }
            //存放样本的List
            ArrayList<Text> arrayList=new ArrayList<Text>();

            int sign=0;
            double alpha=0.01;
            for(Text line: values) {
                arrayList.add(line);
            }
            ArrayList<Integer> arrList1=new ArrayList<Integer>(arrayList.size());
            for(int x=0;x<arrayList.size();x++){
                arrList1.add(x);
            }
            Random r1=new Random();
            //循环所有的样本
            for(int xx=0;xx<arrayList.size();xx++){
                //随机抽取一个样本
                int signxc=r1.nextInt(arrList1.size());
                Text line=arrayList.get(arrList1.get(signxc));
                //对每行数据进行拆分，构建矩阵
                String[] lineSplit=line.toString().split("\t");
                double[] lineDouble=new double[lineSplit.length-1];
                for(int x=0;x<lineSplit.length-1;x++){
                    lineDouble[x]=Double.parseDouble(lineSplit[x]);
                }
                //(1 *3)
                Matrix sample= new Matrix(lineDouble,1);
                //(1*1)=(1*3) *(3*1)
                Matrix h=sample.times(weight);
                //(1,1)
                Matrix sampleresult=new Matrix(1,1,Double.parseDouble(lineSplit[lineSplit.length-1]));
                //(1*1)
                Matrix error=sampleresult.minus(h);
                //(3*1)=(3*1)-{(3*1) * (1*1) }
                weight=weight.minus(sample.transpose().times(error).times(alpha).times(-1.0/100));
                //把抽到的样本删除
                arrList1.remove(signxc);
            }
            double[][] temp= weight.transpose().getArray();
            StringBuffer stringBuffer=new StringBuffer();
            for(int x=0;x<Integer.parseInt(context.getConfiguration().get("feture"));x++){
                stringBuffer.append(temp[0][x]+"\t");
            }
            context.write(new IntWritable(1),new Text(stringBuffer.toString()));
        }
    }

//    public static class Combiner2 extends Reducer<IntWritable,Text,IntWritable,Text>{
//        public void reduce(IntWritable key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
//
//            Path[] cacheFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            FileSystem fsopen = FileSystem.getLocal(context.getConfiguration());
//            int singing=0;
//            FSDataInputStream in =fsopen.open(cacheFiles[singing]);
//            String[] temp=in.readLine().split("\t");
//            String[] temps=Arrays.copyOfRange(temp,1,temp.length);
//            StringBuffer stringBuffer=new StringBuffer();
//            for(int x=0;x<temps.length;x++){
//                stringBuffer.append((Double.parseDouble(temps[x])-0.1)+"\t");
//            }
//            context.write(new IntWritable(cacheFiles.length),new Text(stringBuffer.toString()));
//        }
//    }

    //根据每个小样本的计算结果参数，重组参数，当前策略是求平均数
    public static class Reduce1 extends Reducer<IntWritable,Text,IntWritable,Text>{
        public void reduce(IntWritable key, Iterable<Text> values,Context context)throws IOException,InterruptedException {
            double[] result=new double[Integer.parseInt(context.getConfiguration().get("feture"))];
            int sign=0;
            for(Text line: values){
                    String[] lineSplit=line.toString().split("\t");
                    for(int x=0;x<result.length;x++){
                        result[x]+=Double.parseDouble(lineSplit[x]);
                    }
                sign++;
            }
            //所有数据全部求完和，求平均值
            StringBuffer stringBuffer=new StringBuffer();
            for(int x=0;x<result.length;x++){
                stringBuffer.append(result[x]/sign+"\t");
            }
            context.write(new IntWritable(1),new Text(stringBuffer.toString()));
        }
    }
//    public static class Reduce2 extends Reducer<IntWritable,Text,IntWritable,Text>{
//        public void reduce(IntWritable key, Iterable<Text> values,Context context)throws IOException,InterruptedException {
//
//            context.write(key,new Text(values.iterator().next()));
//        }
//    }
    //这里的args是经过解析处理过的，可用于hadoop使用的参数
    public int run(String[] args)throws Exception {
        //job队列
        Job[] jobList=new Job[Integer.parseInt(args[2])-1];
        //job控制器队列
        ControlledJob[] controlledList=new ControlledJob[Integer.parseInt(args[2])];
        //总控队列
        JobControl jobControl=new JobControl("master");
        int sign=1;
        Configuration conf=new Configuration();
        conf.set("feture",args[3]);
        Job job1=new Job(conf,"xc1");
        job1.setJarByClass(test1.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce1.class);
        job1.setCombinerClass(Combiner.class);
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/number"+sign));
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);
        //加入控制器
        controlledList[0]=new ControlledJob(conf);
        controlledList[0].setJob(job1);
        jobControl.addJob(controlledList[0]);
        //迭代次数
        for(int x=0;x<Integer.parseInt(args[2])-1;x++) {
            conf=new Configuration();
            DistributedCache.addCacheFile(new Path("hdfs://"+args[1]+"/number"+(x+1)+"/part-r-00000").toUri(), conf);
            conf.set("feture",args[3]);
            jobList[x]=new Job(conf,"xc"+(x+2));
            jobList[x].setJarByClass(test1.class);
            jobList[x].setMapperClass(Map.class);
            jobList[x].setReducerClass(Reduce1.class);
            //从第二次以后的迭代用新的combiner
            jobList[x].setCombinerClass(Combiner1.class);
            //每一次迭代的输入都是样本数据
            FileInputFormat.addInputPath(jobList[x],new Path(args[0]));
            //每一次的输出结果要变化，要不无法创建并写入一个hdfs已经存在的文件
            FileOutputFormat.setOutputPath(jobList[x],new Path(args[1]+"/number"+(x+2)));
            jobList[x].setOutputFormatClass(TextOutputFormat.class);
            jobList[x].setOutputKeyClass(IntWritable.class);
            jobList[x].setOutputValueClass(Text.class);
            jobList[x].waitForCompletion(true);
            //加入控制器
            controlledList[x+1]=new ControlledJob(conf);
            controlledList[x+1].setJob(jobList[x]);
        }
        //设置多个作业的直接依赖关系,设置总控制器的各作业控制器队列
        for(int y=0;y<controlledList.length-1;y++){
            controlledList[y].addDependingJob(controlledList[y+1]);
            jobControl.addJob(controlledList[y+1]);
        }
        Thread t=new Thread(jobControl);
        t.start();
//        while (true){
//            if(jobControl.allFinished()){
//                System.out.println(jobControl.getSuccessfulJobList());
//                jobControl.stop();
//                break;
//            }
//        }
        t.stop();

        return 1;
    }
    public static void main(String[] args)throws Exception{
        ToolRunner.run(new Configuration(),new test1(),args);
    }
}
