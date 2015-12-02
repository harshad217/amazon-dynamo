package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by harshad on 4/23/15.
 */

import android.content.ContentValues;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import android.content.ContentValues;

public class Message implements Serializable {

    public String key;
    public String value;

    public String from_port;
    public String to_port;

    public int version;
    public String coord;

    public HashMap < String, ArrayList < Object >> tuple_map;

    public enum TYPE {
        FORWARD_INSERT, INSERT_REPLICA, ASK_VERSION, VERSION_RESPONSE, WOKEUP,
        MORNING_FROM_COORD, MORNING_FROM_SUCCESSOR, INSERTION_REPLY, FAIL_PUT_REPLICA, FAIL_INSERT,
        DELETE_ALL, DELETE_REPLICA, DELETE_FORWARD, FORWARD_QUERY,
        QUERY_RESPONSE, COLLECT_ALL, TAKE_ALL
    }
    public TYPE t;

    public int pid;

    public void setToPort(String given_port) {
        this.to_port = given_port;
    }

    public String getToPort() {
        return this.to_port;
    }

    public void setFromPort(String given_port) {
        this.from_port = given_port;
    }

    public String getFromPort() {
        return this.from_port;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getPid() {
        return pid;
    }

    public void setType(TYPE given_type) {
        this.t = given_type;
    }

    public TYPE getType() {
        return this.t;
    }

    public void setKey(String given_key) {
        this.key = given_key;
    }

    public String getKey() {
        return this.key;
    }

    public void setValue(String given_value) {
        this.value = given_value;
    }

    public String getValue() {
        return this.value;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public String getCoord() {
        return coord;
    }

    public void setCoord(String coord) {
        this.coord = coord;
    }

    public HashMap < String, ArrayList < Object >> getTuple_map() {
        return tuple_map;
    }

    public void setTuple_map(HashMap < String, ArrayList < Object >> tuple_map) {
        this.tuple_map = tuple_map;
    }

}