package com.communication;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;
//接口
public interface MyBizable extends VersionedProtocol {
    static final int PORT=123456;
    abstract String hello(String name);
    abstract long getProtocolVersion(
            String protocol,
            long clientVersion
    )throws IOException;
}