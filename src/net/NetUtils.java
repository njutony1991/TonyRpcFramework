package net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by tony on 15-4-10.
 */
public class NetUtils {
    public static void connect(){

    }

    public static InputStream getInputStream(Socket socket) throws IOException {
        return socket.getInputStream();
    }

    public static OutputStream getOutputStream(Socket socket) throws IOException{
        return socket.getOutputStream();
    }
}
