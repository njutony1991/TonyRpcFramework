package rpc;

import java.io.*;
import java.io.Serializable;

/**
 * Created by tony on 15-4-10.
 */
public class ConnectionHeader implements Serializable{

    private String protocol;

    public ConnectionHeader() { }

    public ConnectionHeader(String protocol){
        this.protocol = protocol;
    }

    public String getProtocol(){
        return this.protocol;
    }

    private void readObject(ObjectInputStream in) throws IOException,ClassNotFoundException{
        in.defaultReadObject();

        if(protocol==null)
            throw new InvalidObjectException("protocol should not be null");
    }

    private void writeObject(ObjectOutputStream out) throws IOException,ClassCastException{
        out.defaultWriteObject();
    }
}
