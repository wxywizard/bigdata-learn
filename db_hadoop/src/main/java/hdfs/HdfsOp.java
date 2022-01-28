package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Java代码操作HDFS
 * 文件操作：上传文件、下载文件、删除文件
 * @author wangxy
 * @Classname HdfsOp
 * @Date 2022/1/28 1:57 PM
 */
public class HdfsOp {
    public static void main(String[] args) {
        //创建配置对象
        Configuration conf = new Configuration();
        //指定HDFS的地址
        conf.set("fs.defaultFS","hdfs://bigdata01:9000");
        //设置通过域名访问datanode
       // conf.set("dfs.client.use.datanode.hostname", "true");
        try {
            //获取操作HDFS的对象
            FileSystem fileSystem = FileSystem.get(conf);

            //获取HDFS文件系统的输出流
            FSDataOutputStream fos = fileSystem.create(new Path("/test/hadoop_install.txt"));
            //获取本地文件的输入流
            FileInputStream fis = new FileInputStream("/Users/will/Downloads/hadoop集群安装");
            //上传文件：通过工具类把输入流拷贝到输出流里面，实现本地文件上传到HDFS
            IOUtils.copyBytes(fis,fos,1024,true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
