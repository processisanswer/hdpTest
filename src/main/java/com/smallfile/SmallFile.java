package com.smallfile;

import com.utils.FsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

//小文件解决方案
public class SmallFile {
    public static void main(String[] args) throws IOException {
        //1 应用程序控制
        final Path path=new Path("/combinedfile");
        final FSDataOutputStream create= FsUtil.getFileSystem().create(path);
        final File dir= new File("C:\\tmp\\mes\\logs");
        for(File fileName:dir.listFiles()){
            System.out.println(fileName.getAbsolutePath());
            final FileInputStream fileInputStream=new
                    FileInputStream(fileName.getAbsolutePath());
            final List<String> readLines= org.apache.commons.io.IOUtils.readLines(fileInputStream);
            for(String line:readLines){
                create.write(line.getBytes());
            }
            fileInputStream.close();
        }
        create.close();
    }

}