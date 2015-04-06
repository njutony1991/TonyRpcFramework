package rpc; /**
 * Created by tony on 15-3-17.
 */

import java.io.IOException;

/**
 * An Exception during the execution of rpc
 */
public class RPCException extends IOException{

    RPCException(String message){
        super(message);
    }

    RPCException(String message,Throwable cause){
        super(message,cause);
    }
}
