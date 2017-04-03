package com.myproj.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileOperations {

    Configuration myConf;
    FileSystem dfs;

    {
        myConf = new Configuration();

        myConf.set("dfs.permissions.enabled", "false");
        myConf.set("dfs.web.ugi", "webuser,webgroup");
        myConf.set("fs.defaultFS", "hdfs://localhost:8020");
        myConf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        myConf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
    }

    public boolean mkdir(String dirName) throws IOException {
        dfs = FileSystem.get(myConf);
        System.out.println(dfs.getWorkingDirectory() + " this is from /n/n");
        Path src = new Path(dfs.getWorkingDirectory() + "/" + dirName);
        return dfs.mkdirs(src);
    }

    public boolean delete(String dirName, boolean recurcive) throws IOException {
        dfs = FileSystem.get(myConf);
        Path src = new Path(dfs.getWorkingDirectory() + "/" + dirName);
        return dfs.delete(src, recurcive);
    }

    public void copyFromLocalFile(String localSrc, String dst) throws IOException, OperationNotSupportedException {
        throw new OperationNotSupportedException("File /user/dumin/ac.log could only be replicated to 0 nodes instead of minReplication (=1).  There are 1 datanode(s) running and 1 node(s) are excluded in this operation");
//        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
//        dfs = FileSystem.get(myConf);
//        OutputStream out = dfs.create(new Path(dst));
//        IOUtils.copyBytes(in, out, myConf, true);
//        System.out.println(dst + " copied to HDFS");
//                InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
//        Configuration conf = new Configuration();
//        System.out.println("Connecting to -- " + myConf.get("fs.defaultFS"));
//        FileSystem fs = FileSystem.get(URI.create(dst), myConf);
//        OutputStream out = dfs.create(new Path(dst));
//        IOUtils.copyBytes(in, out, 4096, true);
//        System.out.println(dst + " copied to HDFS");
    }

    static String from = "/tmp/aa.log";
    static String to = "/user/dumin/ae.log";
    static String dir = "Test1";

    public static void main(String[] args) throws Exception {
        FileOperations fo = new FileOperations();
        fo.mkdir(dir);
        fo.delete(dir, true);
//        fo.delete(to,true);
        fo.copyFromLocalFile(from, to);
        Map a;
        List b;
//        b.stream().map().reduce()
    }

//    public static void m1ain(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }


}