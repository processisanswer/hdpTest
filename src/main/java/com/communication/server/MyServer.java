package com.communication.server;


import com.communication.MyBiz;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;

public class MyServer {
    public static final String ADDRESS="localhost";
    public static final int PORT=2454;

    public static void main(String[] args) throws Exception{
        //ProtobufRpcEngine.Server server=RPC.getServer(new MyBiz());
    }
}