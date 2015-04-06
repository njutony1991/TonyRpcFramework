package rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tony on 15-3-17.
 */
public class Client {
    private class Call{

    }

    private class Connection extends Thread{
        private InetSocketAddress server;
        private Socket socket = null;
        private DataInputStream in = null;
        private DataOutputStream out = null;

        private ConcurrentHashMap<Integer,Call> calls = new ConcurrentHashMap<Integer, Call>();
    }
}
