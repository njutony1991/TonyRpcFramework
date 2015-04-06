package rpc;

import java.io.Serializable;

/**
 * Created by tony on 15-3-17.
 */
public class RPC {

    class Server extends rpc.Server{

        protected Server(String bindAddress, int port, Class<? extends Serializable> paramClass, int handlerCount) {
            super(bindAddress, port, paramClass, handlerCount);
        }
    }

}
