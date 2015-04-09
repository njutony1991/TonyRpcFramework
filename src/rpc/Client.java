package rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by tony on 15-3-17.
 */
public class Client {

    private ConcurrentHashMap<ConnectionId,Connection> connections =
                new ConcurrentHashMap<ConnectionId, Connection>();

    private Class<? extends Serializable> valueClass;
    private int counter;
    private AtomicBoolean running = new AtomicBoolean(true);

    /***
     * a call waiting for value
     */
    private class Call{
        int id;
        Serializable param;
        Serializable value;
        IOException error;
        boolean done;

        protected Call(Serializable param){
            this.param = param;
            synchronized (Client.this){
                this.id = counter++;
            }
        }

        protected synchronized void callComplete(){
            this.done = true;
            notify();
        }

        public synchronized void setException(IOException error){
            this.error = error;
            callComplete();
        }

        public synchronized void setValue(Serializable value){
            this.value = value;
            callComplete();
        }

        public synchronized Serializable getValue(){
            return this.value;
        }
    }

    private class Connection extends Thread{
        private InetSocketAddress server;
        private Socket socket = null;
        private DataInputStream in = null;
        private DataOutputStream out = null;

        private ConcurrentHashMap<Integer,Call> calls = new ConcurrentHashMap<Integer, Call>();
    }

    /**
     * this class holds the address and the protocol,the client connections
     * to servers are uniquely identified by <address,protocol>
     */
    static class ConnectionId{
        private static final int PRIME = 16777619;
        InetSocketAddress address;
        Class<?> protocol;
        private int rpcTimeout;
        private int maxIdleTime;
        private int maxRetries;
        private boolean tcpNoDelay;
        private boolean doPing;

        private int pingInterval;

        ConnectionId(InetSocketAddress address,Class<?> protocol,int rpcTimeout,
                     int maxIdleTime,int maxRetries,boolean tcpNoDelay,boolean doPing,int pingInterval){
            this.protocol = protocol;
            this.address = address;
            this.rpcTimeout = rpcTimeout;
            this.maxIdleTime = maxIdleTime;
            this.maxRetries = maxRetries;
            this.tcpNoDelay = tcpNoDelay;
            this.doPing  = doPing;
            this.pingInterval = pingInterval;
        }

        InetSocketAddress getAddress(){
            return this.address;
        }

        Class<?> getProtocol(){
            return this.protocol;
        }

        int getRpcTimeout(){
            return this.rpcTimeout;
        }

        int getMaxIdleTime() {
            return maxIdleTime;
        }

        int getMaxRetries() {
            return maxRetries;
        }

        boolean getTcpNoDelay() {
            return tcpNoDelay;
        }

        boolean getDoPing() {
            return doPing;
        }

        int getPingInterval() {
            return pingInterval;
        }

        static ConnectionId getConnectionId(InetSocketAddress addr,Class<?> protocol,int rpcTimeout){
            return new ConnectionId(addr,protocol,rpcTimeout,
                                    10000, //10s
                                    10,    //10 retries
                                    false,
                                    true,  //ping
                                    1000);
        }

        static boolean isEqual(Object a,Object b){
            return a==null ? b==null : a.equals(b);
        }

        @Override
        public boolean equals(Object obj){
            if(obj==this)
                return true;

            if(obj instanceof ConnectionId){
                ConnectionId that = (ConnectionId) obj;
                return isEqual(this.address,that.address)
                        && this.doPing==that.doPing
                        && this.maxIdleTime==that.maxIdleTime
                        && this.maxRetries==that.maxRetries
                        && this.pingInterval==that.pingInterval
                        && isEqual(this.protocol,that.protocol)
                        && this.rpcTimeout==that.rpcTimeout
                        && this.tcpNoDelay==that.tcpNoDelay;
            }
            return false;
        }

        public int hashCode(){
            int result = 1;
            result = PRIME*result + ((address==null) ? 0: address.hashCode());
            result = PRIME*result + (doPing ? 1231:1237);
            result = PRIME*result + maxIdleTime;
            result = PRIME*result + maxRetries;
            result = PRIME*result + pingInterval;
            result = PRIME*result + ((protocol==null)? 0: protocol.hashCode());
            result = PRIME*result + rpcTimeout;
            result = PRIME*result + (tcpNoDelay? 1231:1237);
            return result;
        }
    }
}
