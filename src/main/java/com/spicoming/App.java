package com.spicoming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {

       /* URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        try {
            URL url=new URL("hdfs://192.168.2.128:9000/input.txt");
            InputStream in=url.openStream();
            IOUtils.copyBytes(in,System.out,1024,true);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Hello World!");
        System.out.println(getFileSystem());*/
        //mkdir();
        //上传
        //putData();
        //下载
        //downLoad();
        //删除文件或目录
        //delete();
        //遍历目录
        list();

    }
    //获取FileSystem
    public static FileSystem getFileSystem(){
        try {
            FileSystem fileSystem=FileSystem.get(new URI(
                    "hdfs://192.168.2.128:9000"
            ),new Configuration());
            System.out.println("获取FileSystem成功-->"+fileSystem);
            return fileSystem;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;

    }
    //创建文件夹
    public static void mkdir(){
        FileSystem fileSystem=getFileSystem();
        try {
            boolean isSuccess=fileSystem.mkdirs(
                    new Path("/dir1")
            );
            if (isSuccess){
                System.out.println("mkdir success!");
            }else{
                System.out.println("mkdir failed!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    //上传
    public static void putData(){
        FileSystem fileSystem=getFileSystem();
        //创建一个上传路径，返回输出流
        try {
            FSDataOutputStream os=fileSystem.create(
                    new Path("/dir/readmd")
            );
            FileInputStream in=new FileInputStream(
                    "D:\\role-menu.sql");
            IOUtils.copyBytes(in,os,1024,true);
            System.out.println("上传成功...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //下载
    public static void downLoad() throws IOException {
        FileSystem fileSystem=getFileSystem();
        FSDataInputStream in=fileSystem.open(
                new Path("hdfs://192.168.2.128:9000/hello")
        );
        //关闭流需要手动关闭，System.out也是一个输出流
        //如果是true,下面就不会输出了
        IOUtils.copyBytes(in,System.out,1024,false);
        in.close();
    }
    //删除文件或文件夹
    public static void delete() throws IOException {
        FileSystem fileSystem=getFileSystem();
        //true表示是否递归删除，如果是文件，true or false均可
        //如果是文件夹，则必须是true
        boolean isDeleted=fileSystem.delete(
                new Path("/dir"),true
        );
        if(isDeleted){
            System.out.println("delete success");
        }else{
            System.out.println("delete failed");
        }
    }
    //遍历目录
    public static void list() throws IOException {
        FileSystem fileSystem=getFileSystem();
        FileStatus[] listFileStatus=fileSystem.listStatus(new Path("/"));
        for(FileStatus fileStatus:listFileStatus){
            String isDir=fileStatus.isDirectory()?"目录":"文件";
            String name=fileStatus.getPath().getName();
            System.out.println(isDir+"-->"+name);
        }
    }



}
