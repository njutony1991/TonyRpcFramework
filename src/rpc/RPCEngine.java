package rpc;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

/**
 * Created by tony on 15-3-19.
 */
public interface RPCEngine {
    Object getProxy(Class<?> protocol,SocketFactory factory,int rpcTimeOut) throws IOException;

    void stopProxy(Object proxy);

    Object[] call(Method method,Object[][] params,InetSocketAddress[] addrs) throws IOException,InterruptedException;

    RPC.Server getRerver(Class<?> protocol, Object instance, String bindAddress, int port, int numHandlers) throws IOException;
}
