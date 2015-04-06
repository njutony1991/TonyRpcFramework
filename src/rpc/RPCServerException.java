package rpc; /**
 * Created by tony on 15-3-17.
 */

/**
 * An Exception on the RPC Server
 */
public class RPCServerException extends RPCException{
    RPCServerException(String message){
        super(message);
    }

    RPCServerException(String message,Throwable cause){
        super(message,cause);
    }
}
