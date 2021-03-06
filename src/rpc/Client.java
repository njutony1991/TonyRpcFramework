package rpc;

import net.NetUtils;

import javax.net.SocketFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tony on 15-3-17.
 */
public class Client {

    private ConcurrentHashMap<ConnectionId,Connection> connections =
                new ConcurrentHashMap<ConnectionId, Connection>();

    private Class<? extends Serializable> valueClass;
    private int counter;
    private AtomicBoolean running = new AtomicBoolean(true);

    private SocketFactory socketFactory;

    final static int DEFAULT_PING_INTERVAL = 60000;
    final static int PING_CALL_ID = -1;
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
        private ConnectionHeader header;
        final private  ConnectionId remoteId;

        private Socket socket = null;
        private DataInputStream in = null;
        private DataOutputStream out = null;
        private int rpcTimeout;
        private int maxIdleTime;

        private int maxRetries;
        private boolean tcpNoDelay;
        private boolean doPing;
        private int pingInterval;

        private ConcurrentHashMap<Integer,Call> calls = new ConcurrentHashMap<Integer, Call>();
        private AtomicLong lastActivity = new AtomicLong();
        private AtomicBoolean shouldCloseConnection = new AtomicBoolean();
        private IOException closeException;

        public Connection(ConnectionId remoteId) throws IOException {
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            if (server.isUnresolved()) {
                throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
            }
            this.rpcTimeout = remoteId.getRpcTimeout();
            this.maxIdleTime = remoteId.getMaxIdleTime();
            this.maxRetries = remoteId.getMaxRetries();
            this.tcpNoDelay = remoteId.getTcpNoDelay();
            this.doPing = remoteId.getDoPing();
            this.pingInterval = remoteId.getPingInterval();

            this.header = new ConnectionHeader();  // to do

            this.setName("IPC Client (" + socketFactory.hashCode() + ") connection to" +
                    remoteId.getAddress().toString()); //to do
            this.setDaemon(true);
        }

        // Update lastActivity with the current time
        private void touch(){
            lastActivity.set(System.currentTimeMillis());
        }

        private synchronized boolean addCall(Call call){
            if(shouldCloseConnection.get())
                return false;
            calls.put(call.id,call);
            notify();
            return true;
        }

        /**
         *  This class sends a ping to the remote side when timeout on
         *  reading. If no failure is detected, it retries until at least
         *  a byte is read.
         */
        private class PingInputStream extends FilterInputStream{
            /**
             * Creates a <code>FilterInputStream</code>
             * by assigning the  argument <code>in</code>
             * to the field <code>this.in</code> so as
             * to remember it for later use.
             *
             * @param in the underlying input stream, or <code>null</code> if
             *           this instance is to be created without an underlying stream.
             */
            protected PingInputStream(InputStream in) {
                super(in);
            }

            private void handleTimeout(SocketTimeoutException e) throws IOException{
                if(shouldCloseConnection.get() || !running.get() || rpcTimeout >0)
                    throw e;
                else
                    sendPing();
            }

            /***
             * Read a byte from the stream
             * send a ping if timeout on read.Retries if no failure is detected until a byte is read
             * @throws IOException for any IO problem other than socket timeout
             */
            public int read() throws IOException{
                do{
                    try {
                        return super.read();
                    }catch (SocketTimeoutException e){
                        handleTimeout(e);
                    }
                }while(true);
            }

            /***
             * Read byte into a buffer starting from offset <code>off</code>
             * Send a ping if timeout on read.Retries if no failure is detected
             * @param buf
             * @param off
             * @param len
             * @return
             * @throws IOException
             */
            public int read(byte[] buf,int off,int len) throws IOException{
                do{
                    try {
                        return super.read(buf, off, len);
                    }catch(SocketTimeoutException e){
                        handleTimeout(e);
                    }
                }while(true);
            }
        }

        private synchronized void setupIOStreams() {
            if(socket!=null || shouldCloseConnection.get())
                return;
            try{
                final short MAX_RETRIES = 5;
                while(true){
                    setupConnection();
                    InputStream inStream = NetUtils.getInputStream(socket);
                    OutputStream outStream = NetUtils.getOutputStream(socket);
                    writeRpcHeader(outStream);

                    if(doPing)
                        this.in = new DataInputStream(new BufferedInputStream(
                                                      new PingInputStream(inStream)));
                    else
                        this.in = new DataInputStream(new BufferedInputStream(inStream));
                    this.out = new DataOutputStream(new BufferedOutputStream(outStream));

                    writeHeader();
                    /**update the last activity time**/
                    touch();

                    /**start the receiver thread after the connection **/
                    start();

                    return;
                }
            }catch(IOException e){
                markClosed(e);
                close();
            }
        }

        private void setupConnection() throws IOException{
            short ioFailures = 0;
            short timeoutFailures = 0;
            while(true){
                try{
                    this.socket = socketFactory.createSocket();
                    this.socket.setTcpNoDelay(tcpNoDelay);
                    NetUtils.connect(this.socket,remoteId.getAddress(),20000);  // connection time out is 20s
                    if(rpcTimeout > 0)
                        pingInterval = rpcTimeout;  //rpcTimeout overwrites pingInterval
                    this.socket.setSoTimeout(this.pingInterval);
                    return;
                }catch(SocketTimeoutException toe){
                    handleConnectionFailure(timeoutFailures++,45,toe);
                }catch(IOException e){
                    handleConnectionFailure(ioFailures++,this.maxRetries,e);
                }
            }
        }

        /**
         * Handle connection failures
         *
         * if the current number of retries is equal to the max number of retries,
         * stop retrying and throw the exception;Otherwise backoff 1 second and
         * try connecting again.
         *
         * @param failures
         * @param maxRetries
         * @param ie
         */
        private void handleConnectionFailure(int failures,int maxRetries,IOException ie) throws IOException {
            closeConnection();
            if (failures >= maxRetries)
                throw ie;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }

            System.err.println("Retrying connect to server: " +server+". Already tried "+ failures+ " time(s).");
        }

        private void closeConnection(){
            if(socket==null)
                return;
            try{
                socket.close();
            } catch (IOException e) {
               System.err.println("Not able to close a socket "+e);
            }
            socket = null;
        }

        private void writeRpcHeader(OutputStream out) throws IOException{
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(out));
            outputStream.write(Server.HEADER.array());
            out.flush();
        }

        private void writeHeader() throws IOException{
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(buf);
            out.writeObject(header);
            out.writeInt(buf.size());
            out.write(buf.toByteArray(),0,buf.size());
        }


        public void run(){
            while(waitForWork())
                receiveResponse();

            close();
        }

        private synchronized boolean waitForWork(){
            // calls is empty, wait for the timeout
            if(calls.isEmpty() && !shouldCloseConnection.get() && running.get()){
                long timeout = maxIdleTime - (System.currentTimeMillis()-lastActivity.get());
                if(timeout>0){
                    try{
                        wait(timeout);
                    }catch(InterruptedException e) {}
                }
            }

            if(!calls.isEmpty() && !shouldCloseConnection.get() && running.get()){
                return true;
            }else if(shouldCloseConnection.get()){  // the connection get shut down
                return false;
            }else if(calls.isEmpty()){ // idle connection closed or stopped
                markClosed(null);
                return false;
            }else{  // get stopped but there are still pending requests
                markClosed((IOException)new IOException().initCause(new InterruptedIOException()));
                return false;
            }
        }

        /**
         * receive a response.
         * only one receiver, so no synchronization
         */
        private void receiveResponse(){
            if(shouldCloseConnection.get())
                return;
            touch();

            try {
                int id = in.readInt();   // try to read an id

                Call call = calls.get(id);
                int state = in.readInt();
                if(state == Status.SUCCESS.state){

                }else if(state == Status.ERROR.state){

                }else if(state == Status.FATAL.state){

                }

            }catch(IOException e){
                markClosed(e);
            }
        }

        private synchronized void markClosed(IOException e){

        }

        private synchronized void close(){

        }

        private synchronized void sendPing() throws IOException{
            long curTime = System.currentTimeMillis();
            if(curTime-lastActivity.get() >= pingInterval){
                lastActivity.set(curTime);
                synchronized (out){
                    out.writeInt(PING_CALL_ID);
                    out.flush();
                }
            }
        }
    }

    public Client(Class<? extends Serializable> valueClass,SocketFactory factory){
        this.valueClass = valueClass;
        this.socketFactory = factory;
    }

    /**
     *  get a connection from the pool,or create a new one and add it to the pool.
     *  Connections to a given ConnectionId are reused
     * @param remoteId
     * @param call
     * @return
     */
    private Connection getConnection(ConnectionId remoteId,Call call) throws IOException{
        if(!running.get())
            throw new IOException("The client is stopped");

        Connection connection;
        do{
            synchronized (connections){
                connection = connections.get(remoteId);
                if(connection==null){
                    connection = new Connection(remoteId);
                    connections.put(remoteId,connection);
                }
            }
        }while(!connection.addCall(call));

        connection.setupIOStreams();
        return connection;
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
