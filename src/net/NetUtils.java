package net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * Created by tony on 15-4-10.
 */
public class NetUtils {
    public static void connect(Socket socket,SocketAddress endpoint,int timeout) throws IOException {
        if(socket == null || endpoint == null || timeout <0)
            throw  new IllegalArgumentException("Illegal argument for connect()");

        socket.connect(endpoint,timeout);

        if(socket.getPort()==socket.getLocalPort() && socket.getInetAddress().
                    equals(socket.getLocalAddress())){
            System.err.println("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException("Localhost target connection resulted in a loopback. "+
                                       " No daemon is listening on the target port. ");
        }
    }

    public static InputStream getInputStream(Socket socket) throws IOException {
        return socket.getInputStream();
    }

    public static OutputStream getOutputStream(Socket socket) throws IOException{
        return socket.getOutputStream();
    }
}
