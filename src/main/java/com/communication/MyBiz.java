package com.communication;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyBiz implements MyBizable {
    @Override
    public String hello(String name) {
        System.out.println("我被调用了...");
        return "hello,"+name;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
     return MyBiz.PORT;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }

}