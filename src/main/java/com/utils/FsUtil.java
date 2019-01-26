package com.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FsUtil {
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
}