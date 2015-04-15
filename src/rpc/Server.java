package rpc;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.BlockingQueue;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by tony on 15-3-17.
 */

public abstract class Server {

    public static final ByteBuffer HEADER = ByteBuffer.wrap("trpc".getBytes());

    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThreads;
    private Class<? extends Serializable> paramClass;
    private int maxIdleTime;  // the maximum idle time after which a client may be disconnected

    private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

    private int maxQueueSize;
    private int maxRespSize;
    private final boolean tcpNoDelay;

    volatile private boolean running = true;
    private BlockingQueue<Call> callQueue;

    private  List<Connection> connectionList;  // a list of client connections

    private Listener listener = null;
    private Responder responder = null;
    private int numConnections = 0;
    private Handler[] handlers = null;


    public static void bind(ServerSocket socket,InetSocketAddress address,int backlog) throws IOException{
        try {
            socket.bind(address,backlog);
        } catch (BindException e) {
            BindException bindException = new BindException("Problem binding to "+address+" : "
                                            + e.getMessage());
            bindException.initCause(e);
            throw bindException;
        } catch(SocketException e){
            if("Unresolved address".equals(e.getMessage())){
                throw new UnknownHostException("Invalid hostname for server: "+ address.getHostName());
            }else
                throw e;
        }
    }


    private static class Call {
        private int id;
        private Serializable param;
        private Connection connection;
        private long timestamp;

        private ByteBuffer response;

        public Call(int id,Serializable param,Connection connection){
            this.id = id;
            this.param = param;
            this.connection = connection;
            this.timestamp = System.currentTimeMillis();
            this.response = null;
        }

        @Override
        public String toString(){
            return param.toString() + " from " + connection.toString();
        }

        public void setResponse(ByteBuffer response){
            this.response = response;
        }
    }

    private class Listener extends Thread{
        private ServerSocketChannel acceptChannel = null;
        private Selector selector = null;
        private Reader[] readers = null;
        private int currentReader = 0;
        private InetSocketAddress address;
        private int backlongLength = 100;

        public Listener() throws IOException{
            address = new InetSocketAddress(bindAddress,port);
            //create a new Server socket and set to non blocking mode
            acceptChannel = ServerSocketChannel.open();
            acceptChannel.configureBlocking(false);

            bind(acceptChannel.socket(),address, backlongLength);

            selector = Selector.open();
            readers = new Reader[readThreads];
            for(int i=0;i<readThreads;i++){
                Reader reader = new Reader("Socket Reader #"+(i+1)+" for port "+port);
                readers[i] = reader;
                readers[i].start();
            }

            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
            this.setName("IPC Server listener on "+port);
            this.setDaemon(true);
        }

        private class Reader extends Thread{
            private volatile boolean adding = false;
            private final Selector readSelector;

            Reader(String name) throws IOException{
                super(name);
                readSelector = Selector.open();
            }

            public void run(){
                try{
                    doRunLoop();
                }finally {
                    try{
                        readSelector.close();
                    }catch(IOException e){
                        System.err.println("Error closing read Selector in "+ this.getName()+ e);
                    }
                }
            }

            private synchronized void doRunLoop(){
                while(running){
                    SelectionKey key = null;
                    try{
                        readSelector.select();
                        while(adding)
                            this.wait(1000);

                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while(iter.hasNext()){
                            key = iter.next();
                            iter.remove();
                            if(key.isValid()){
                                if(key.isReadable()){
                                    doRead(key);
                                }
                            }
                            key = null;
                        }
                    } catch (InterruptedException e) {
                        if(running){
                            System.out.println(this.getName() + " unexpectedly interrupted "+ e);
                        }
                    }catch (IOException e){
                        System.err.println("Error in Reader " + e);
                    }
                }
            }

            public synchronized SelectionKey registerChannel(SocketChannel channel) throws ClosedChannelException {
                return channel.register(readSelector,SelectionKey.OP_READ);
            }

            public void startAdd(){
                adding = true;
                readSelector.wakeup();
            }

            public synchronized void finishAdd(){
                adding = false;
                this.notify();
            }

            void shutdown(){
                assert !running;
                readSelector.wakeup();
                try{
                    join();
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void run(){
            while(running){
                SelectionKey key = null;
                try{
                    getSelector().select();
                    Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
                    while(iter.hasNext()){
                        key = iter.next();
                        iter.remove();
                        if(key.isValid()){
                            if(key.isAcceptable())
                                doAccept(key);
                        }
                        key = null;
                    }
                }catch(OutOfMemoryError e){

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            synchronized (this){
                try {
                    acceptChannel.close();
                    selector.close();
                } catch (IOException e) {
                }

                selector = null;
                acceptChannel = null;
            }
        }

        void doAccept(SelectionKey key) throws IOException {
            Connection conn = null;
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            while((channel = server.accept())!=null){
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(tcpNoDelay);

                Reader reader = getReader();
                try{
                    reader.startAdd();
                    SelectionKey readkey = reader.registerChannel(channel);
                    conn = new Connection(readkey,channel,System.currentTimeMillis());
                    readkey.attach(conn);
                    synchronized (connectionList){
                        connectionList.add(numConnections,conn);
                        numConnections++;
                    }
                }finally {
                    reader.finishAdd();
                }
            }
        }

        void doRead(SelectionKey key){
            int count = 0;
            Connection c = (Connection)key.attachment();
            if(c==null)
                return;
            /**
             * to do
             */
        }

        synchronized void doStop(){
            if(selector!=null){
                selector.wakeup();
            }
            if(acceptChannel!=null){
                try {
                    acceptChannel.socket().close();
                } catch (IOException e) {
                    System.err.println(getName()+ " Exception in closing listener socket. "+e);
                }
            }
            for(Reader r : readers)
                r.shutdown();
        }

        synchronized Selector getSelector(){
            return this.selector;
        }

        Reader getReader(){
            currentReader = (currentReader+1) % readers.length;
            return readers[currentReader];
        }

    }

    class Responder extends Thread{

    }

    class Handler extends Thread{

    }


    class Connection {
        private boolean headerRead = false;

        private SocketChannel channel;
        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;
        private volatile  int rpcCount = 0;
        private LinkedList<Call> responseQueue;

        private long lastContact;
        private Socket socket;

        private String hostAddress;
        private int remotePort;
        private InetAddress addr;

        private ByteBuffer unwrappedData;
        private ByteBuffer unwrappedDataLengthBuffer;
        Class<?> protocol;

        public Connection(SelectionKey key,SocketChannel channel,long lastContact){
            this.channel = channel;
            this.lastContact = lastContact;
            this.data = null;
            this.dataLengthBuffer = ByteBuffer.allocate(4);
            this.socket = channel.socket();
            this.addr = this.socket.getInetAddress();
            if(addr == null)
                this.hostAddress = "Unknown";
            else
                this.hostAddress = addr.getHostAddress();

            this.remotePort = socket.getPort();
            this.responseQueue = new LinkedList<Call>();

        }

        @Override
        public String toString(){
            return this.hostAddress + ":" + remotePort;
        }

        public String getHostAddress(){
            return this.hostAddress;
        }

        public InetAddress getHostInetAddress(){
            return this.addr;
        }

        public void setLastContact(long lastContact){
            this.lastContact = lastContact;
        }

        public long getLastContact(){
            return this.lastContact;
        }

        private boolean isIdle(){
            return rpcCount==0;
        }

        synchronized private void decRpcCount(){
            rpcCount--;
        }

        synchronized private void incrRpcCount(){
            rpcCount++;
        }

        private boolean timedOut(long currentTime){
            if(isIdle() && currentTime - lastContact > maxIdleTime)
                return true;
            return false;
        }


        public int readAndProcess(){

            return 0;
        }

    }

    protected Server(String bindAddress, int port, Class<? extends Serializable> paramClass,int handlerCount){
        this.bindAddress = bindAddress;
        this.port = port;
        this.paramClass = paramClass;
        this.handlerCount = handlerCount;
        this.maxQueueSize = 30; // to do
        this.maxRespSize = 15;  // to do
        this.readThreads = 20;  // to do
        this.callQueue = new LinkedBlockingQueue<Call>(maxQueueSize);
        this.tcpNoDelay = true; // to do
    }


}
