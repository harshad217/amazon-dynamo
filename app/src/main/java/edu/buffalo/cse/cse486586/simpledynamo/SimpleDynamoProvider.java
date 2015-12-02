package edu.buffalo.cse.cse486586.simpledynamo;

/*
 Name: Harshad R. Deshpande
 UBIT: harshadr
 Person Number: 50133685
  */

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {
    SQLiteDatabase db;
    String my_hash;
    String myPort1;
    String succ1, succ2, mycod1, mycod2, pred, pred_hash, mycod2pred;
    int factorId = 2;
    HashMap < String, ArrayList < Object >> tuple_result = new HashMap < > ();
    HashMap < Integer, Long > factorTime = new HashMap < > ();
    HashMap < Integer, Boolean > factor = new HashMap < > ();
    HashMap < Integer, Integer > factorVersion = new HashMap < > ();
    boolean versionFlag;
    int versionGot;
    HashMap < Integer, String > factorValue = new HashMap < > ();
    public Uri theUri;
    String myPort;
    int count = 0;
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.toString((Integer.parseInt(portStr) * 2));
        myPort1 = Integer.toString((Integer.parseInt(portStr)));
        //theUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        Log.e(TAG, "AVD Started of: " + myPort + " at time: " + Calendar.getInstance().getTime());
        try {
            my_hash = genHash(myPort1);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (myPort.equals("11124")) {
            pred = "11120";
            succ1 = "11112";
            succ2 = "11108";
            mycod1 = "11120";
            mycod2 = "11116";
            mycod2pred = "11108";
        } else if (myPort.equals("11112")) {
            pred = "11124";
            succ1 = "11108";
            succ2 = "11116";
            mycod1 = "11124";
            mycod2 = "11120";
            mycod2pred = "11116";
        } else if (myPort.equals("11108")) {
            pred = "11112";
            succ1 = "11116";
            succ2 = "11120";
            mycod1 = "11112";
            mycod2 = "11124";
            mycod2pred = "11120";
        } else if (myPort.equals("11116")) {
            pred = "11108";
            succ1 = "11120";
            succ2 = "11124";
            mycod1 = "11108";
            mycod2 = "11112";
            mycod2pred = "11124";
        } else if (myPort.equals("11120")) {
            pred = "11116";
            succ1 = "11124";
            succ2 = "11112";
            mycod1 = "11116";
            mycod2 = "11108";
            mycod2pred = "11112";
        }

        Log.e(TAG, "I'm AVD --> " + myPort + " $$ " + succ1 + " $$ " + succ2 + " $$ " + pred);
        try {
            pred_hash = genHash(Integer.toString((Integer.parseInt(pred)) / 2));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //START LISTENING TO INCOMING MESSAGES
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i(TAG, "Started listening socket on AVD-->" + myPort);
            Log.i(TAG, "this should have run till now");
            Log.e(TAG, "This should be exited in some ");
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            //return;
        }
        Context context = getContext();
        dbhelp dbhelper = new dbhelp(context);
        db = dbhelper.getWritableDatabase();

        Log.e(TAG, "I just woke up, sending messages to my Coord1, Coord2 and Succ1, Succ2");
        Message msg1 = new Message();
        msg1.setType(Message.TYPE.WOKEUP);
        msg1.setToPort(mycod1);
        msg1.setFromPort(myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null);
        Log.i(TAG, "Sent messge to my Coord1" + mycod1);
        Message msg2 = new Message();
        msg2.setType(Message.TYPE.WOKEUP);
        msg2.setToPort(mycod2);
        msg2.setFromPort(myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, null);
        Log.i(TAG, "Sent messge to my Coord2" + mycod2);
        Message msg3 = new Message();
        msg3.setType(Message.TYPE.WOKEUP);
        msg3.setToPort(succ1);
        msg3.setFromPort(myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3, null);
        Log.i(TAG, "Sent messge to my Succ1" + succ1);
        Message msg4 = new Message();
        msg4.setType(Message.TYPE.WOKEUP);
        msg4.setToPort(succ1);
        msg4.setFromPort(myPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4, null);
        Log.i(TAG, "Sent messge to my Succ2" + succ2);
        Log.i(TAG, "WOKEUP messages sent to all coods and succs. Now waiting.");
        return false;
    }

    private Uri buildUri(String sch, String aut) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(aut);
        uriBuilder.scheme(sch);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String query_given = selection;
        String query_hash = new String();

        try {
            query_hash = genHash(query_given);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String partition = partitionReturn(query_hash);

        Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
            query_given
        });
        if (c.getCount() > 0) {
            c.moveToFirst();
            db.delete("mytab", "key='" + query_given + "'", null);
        }

        if (query_given.equals("\"@\"")) {
            db.delete("mytab", null, null);
        } else if (query_given.equals("\"*\"")) {
            db.delete("mytab", null, null);
            Message msg1 = new Message();
            msg1.setFromPort(myPort);
            msg1.setToPort(succ1);
            msg1.setType(Message.TYPE.DELETE_ALL);
            Message msg2 = new Message();
            msg2.setFromPort(myPort);
            msg2.setToPort(succ2);
            msg2.setType(Message.TYPE.DELETE_ALL);
            Message msg3 = new Message();
            msg3.setFromPort(myPort);
            msg3.setToPort(mycod1);
            msg3.setType(Message.TYPE.DELETE_ALL);
            Message msg4 = new Message();
            msg4.setFromPort(myPort);
            msg4.setToPort(mycod2);
            msg4.setType(Message.TYPE.DELETE_ALL);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4, null);
        } else if (partition.equals(myPort)) {
            db.delete("mytab", "key='" + query_given + "'", null);
            Message msg1 = new Message();
            msg1.setToPort(succ1);
            msg1.setKey(query_given);
            msg1.setFromPort(myPort);
            msg1.setType(Message.TYPE.DELETE_REPLICA);
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            Message msg2 = new Message();
            msg2.setToPort(succ2);
            msg2.setKey(query_given);
            msg2.setFromPort(myPort);
            msg2.setType(Message.TYPE.DELETE_REPLICA);
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, null).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            Log.i("FORWARDING DELETE OPERATION", "Forwarding to AVD" + partition);
            Message msg1 = new Message();
            msg1.setType(Message.TYPE.DELETE_FORWARD);
            msg1.setToPort(partition);
            msg1.setKey(query_given);
            msg1.setFromPort(myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            Message msg2 = new Message();
            msg2.setToPort(returnSuccessor(partition));
            msg2.setKey(query_given);
            msg2.setFromPort(myPort);
            msg2.setType(Message.TYPE.DELETE_REPLICA);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
            Message msg3 = new Message();
            msg3.setToPort(returnSuccessor(returnSuccessor(partition)));
            msg3.setKey(query_given);
            msg3.setFromPort(myPort);
            msg3.setType(Message.TYPE.DELETE_REPLICA);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public int getUID() {
        int new_id;
        factorId = factorId + 1;
        new_id = factorId + 1;
        return new_id;
    }

    public Message createMessage(Message.TYPE type, String key, String value,
    int version, String coord, String toPort, String fromPort, int pid) {
        Message msg = new Message();
        msg.setType(type);
        msg.setKey(key);
        msg.setValue(value);
        msg.setVersion(version);
        msg.setCoord(coord);
        msg.setToPort(toPort);
        msg.setFromPort(fromPort);
        msg.setPid(pid);
        return msg;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.i(TAG, "INSIDE INSERT METHOD, key= " + values.get("key"));
        String got_key = (String) values.get("key");
        String got_value = (String) values.get("value");
        String key_hash = new String();
        try {
            key_hash = genHash(got_key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String partition = partitionReturn(key_hash);

        if (partition.equals(myPort)) {
            int ver;
            Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                got_key
            });
            c.moveToFirst();
            if (c.getCount() > 0) {
                ver = c.getInt(2);
                ver = ver + 1;
            } else {
                ver = 1;
            }
            ContentValues put_values = new ContentValues();
            put_values.put("key", got_key);
            put_values.put("value", got_value);
            put_values.put("version", ver);
            put_values.put("coord", myPort);
            db.insertWithOnConflict("mytab", "", put_values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.i(TAG, "stored successfully in " + myPort);
            Message msg1 = createMessage(Message.TYPE.INSERT_REPLICA, got_key, got_value, ver, myPort, succ1, myPort, 1);
            Message msg2 = createMessage(Message.TYPE.INSERT_REPLICA, got_key, got_value, ver, myPort, succ2, myPort, 1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
            Log.i(TAG, "Messages to replicas sent, from AVD." + myPort);
        } else if (partition.equals("11124")) {

            int
            var = getUID();
            factor.put(var, false);
            Message msg1 = createMessage(Message.TYPE.FORWARD_INSERT, got_key, got_value, 1, "11124", "11124", myPort,
            var);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            //
            factorTime.put(var, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(var) + 2000 || factor.get(var).equals(true)) {
                    break;
                }
            }
            if (factor.get(var).equals(true)) {
                Log.i(TAG, "Forward insertion & Insertion at replicas completed.");
            } else {
                Log.i(TAG, "403:TimedOut, node to which forwarded INSERT crashed. ");
                Message msg2 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11124", "11112", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Message msg3 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11124", "11108", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
                Message msg4 = createMessage(Message.TYPE.FAIL_INSERT, got_key, got_value, 1, "11124", "11124", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);
                Log.i(TAG, "Insert forwarded to replicas because of coordinator failure.");
            }
        } else if (partition.equals("11112")) {


            int
            var = getUID();
            factor.put(var, false);
            Message msg1 = createMessage(Message.TYPE.FORWARD_INSERT, got_key, got_value, 1, "11112", "11112", myPort,
            var);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            factorTime.put(var, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(var) + 2000 || factor.get(var).equals(true)) {
                    break;
                }
            }
            if (factor.get(var).equals(true)) {
                Log.i(TAG, "Forward insertion & Insertion at replicas completed.");
            } else {
                Log.i(TAG, "403:TimedOut, node to which forwarded (INSERT) crashed. ");
                Message msg2 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11112", "11108", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Message msg3 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11112", "11116", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
                Message msg4 = createMessage(Message.TYPE.FAIL_INSERT, got_key, got_value, 1, "11112", "11112", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);
                Log.i(TAG, "Insert forwarded to replicas because of coordinator failure.");
            }
        } else if (partition.equals("11108")) {

            int
            var = getUID();
            factor.put(var, false);
            Message msg1 = createMessage(Message.TYPE.FORWARD_INSERT, got_key, got_value, 1, "11108", "11108", myPort,
            var);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            //
            factorTime.put(var, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(var) + 2000 || factor.get(var).equals(true)) {
                    break;
                }
            }
            if (factor.get(var).equals(true)) {
                Log.i(TAG, "Forward insertion & Insertion at replicas completed.");
            } else {
                Log.i(TAG, "403:TimedOut, node to which forwarded (INSERT) crashed. ");
                Message msg2 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11108", "11116", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Message msg3 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11108", "11120", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
                Message msg4 = createMessage(Message.TYPE.FAIL_INSERT, got_key, got_value, 1, "11108", "11108", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);
                Log.i(TAG, "Insert forwarded to replicas because of coordinator failure.");
            }
        } else if (partition.equals("11116")) {
            //
            int
            var = getUID();
            factor.put(var, false);
            Message msg1 = createMessage(Message.TYPE.FORWARD_INSERT, got_key, got_value, 1, "11116", "11116", myPort,
            var);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            //
            factorTime.put(var, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(var) + 2000 || factor.get(var).equals(true)) {
                    break;
                }
            }
            if (factor.get(var).equals(true)) {
                Log.i(TAG, "Forward insertion & Insertion at replicas completed.");
            } else {
                Log.i(TAG, "403:TimedOut, node to which forwarded (INSERT) crashed. ");
                Message msg2 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11116", "11120", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Message msg3 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11116", "11124", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
                Message msg4 = createMessage(Message.TYPE.FAIL_INSERT, got_key, got_value, 1, "11116", "11116", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);
                Log.i(TAG, "Insert forwarded to replicas because of coordinator failure.");
            }
        } else if (partition.equals("11120")) {

            int
            var = getUID();
            factor.put(var, false);
            Message msg1 = createMessage(Message.TYPE.FORWARD_INSERT, got_key, got_value, 1, "11120", "11120", myPort,
            var);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            //
            factorTime.put(var, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(var) + 2000 || factor.get(var).equals(true)) {
                    break;
                }
            }
            if (factor.get(var).equals(true)) {
                Log.i(TAG, "Forward insertion & Insertion at replicas completed.");
            } else {
                Log.i(TAG, "403:TimedOut, node to which forwarded (INSERT) crashed. ");
                Message msg2 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11120", "11124", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Message msg3 = createMessage(Message.TYPE.FAIL_PUT_REPLICA, got_key, got_value, 1, "11120", "11112", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
                Message msg4 = createMessage(Message.TYPE.FAIL_INSERT, got_key, got_value, 1, "11120", "11120", myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);
                Log.i(TAG, "Insert forwarded to replicas because of coordinator failure.");
            }
        }
        return null;
    }

    public static String partitionReturn(String hashval) {
        String partition = new String();
        if (hashval.compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") <= 0 || hashval.compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") > 0) partition = "11124";
        else if (hashval.compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") <= 0 && hashval.compareTo("177ccecaec32c54b82d5aaafc18a2dadb753e3b1") > 0) partition = "11112";
        else if (hashval.compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") <= 0 && hashval.compareTo("208f7f72b198dadd244e61801abe1ec3a4857bc9") > 0) partition = "11108";
        else if (hashval.compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") <= 0 && hashval.compareTo("33d6357cfaaf0f72991b0ecd8c56da066613c089") > 0) partition = "11116";
        else if (hashval.compareTo("c25ddd596aa7c81fa12378fa725f706d54325d12") <= 0 && hashval.compareTo("abf0fd8db03e5ecb199a9b82929e9db79b909643") > 0) partition = "11120";

        return partition;
    }

    public static String returnSuccessor(String port) {
        String returnPort = new String();

        if (port.equals("11124")) returnPort = "11112";
        else if (port.equals("11112")) returnPort = "11108";
        else if (port.equals("11108")) returnPort = "11116";
        else if (port.equals("11116")) returnPort = "11120";
        else if (port.equals("11120")) returnPort = "11124";

        return returnPort;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
    String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Log.i(TAG, "selection = " + selection);
        Log.i(TAG, "Inside Query Method...." + selection);

        String query_given = selection;
        String query_hash = "";
        String keyCoord = "";
        try {
            query_hash = genHash(query_given);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (!query_given.equals("\"@\"") && !query_given.equals("\"*\"")) {
            keyCoord = partitionReturn(query_hash);
            Log.e(TAG, "Query is:" + query_given + " & it belongs it AVD: " + keyCoord);
        }

        if (query_given.equals("\"@\"")) {
            Cursor c = db.rawQuery("SELECT * FROM mytab", null);
            c.moveToFirst();
            MatrixCursor temp_cur = new MatrixCursor(new String[] {
                "key", "value"
            });

            for (int i = 0; i < c.getCount(); i++) {
                temp_cur.addRow(new Object[] {
                    c.getString(0), c.getString(1)
                });
                c.moveToNext();
            }
            c = temp_cur;
            c.moveToFirst();
            return c;
        } else if (query_given.equals("\"*\"")) {
            Cursor c = db.rawQuery("SELECT * FROM mytab", null);
            c.moveToFirst();
            int count = c.getCount();
            for (int i = 0; i < count; i++) {
                String key = c.getString(0);
                String value = c.getString(1);
                int version = c.getInt(2);
                String coord = c.getString(3);

                if (tuple_result.containsKey(key)) {
                    ArrayList < Object > temp = new ArrayList < > ();
                    temp = tuple_result.get(key);
                    int stored_version = (Integer) temp.get(1);
                    if (stored_version <= version) {
                        ArrayList < Object > new_list = new ArrayList < > ();
                        new_list.add(0, value);
                        new_list.add(1, version);
                        new_list.add(2, coord);
                        tuple_result.put(key, new_list);
                    }
                } else {
                    ArrayList < Object > new_list = new ArrayList < > ();
                    new_list.add(0, value);
                    new_list.add(1, version);
                    new_list.add(2, coord);
                    tuple_result.put(key, new_list);
                }
                c.moveToNext();
            }

            Log.i(TAG, "Sending * query to all...");
            Message msg1 = new Message();
            msg1.setType(Message.TYPE.COLLECT_ALL);
            msg1.setFromPort(myPort);
            msg1.setToPort(mycod1);
            Message msg2 = new Message();
            msg2.setType(Message.TYPE.COLLECT_ALL);
            msg2.setFromPort(myPort);
            msg2.setToPort(mycod2);
            Message msg3 = new Message();
            msg3.setType(Message.TYPE.COLLECT_ALL);
            msg3.setFromPort(myPort);
            msg3.setToPort(succ1);
            Message msg4 = new Message();
            msg4.setType(Message.TYPE.COLLECT_ALL);
            msg4.setFromPort(myPort);
            msg4.setToPort(succ2);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg4);

            Log.i(TAG, "Sent to 2 coods and 2 succs. from= " + myPort);
            int starvar = getUID();
            factorTime.put(starvar, System.currentTimeMillis());
            while (true) {
                //waiting for results
                if (System.currentTimeMillis() > factorTime.get(starvar) + 2000) {
                    break;
                }
            }
            MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                "key", "value"
            });
            for (String key: tuple_result.keySet()) {
                ArrayList < Object > temp_list = tuple_result.get(key);
                String value = (String) temp_list.get(0);
                matrix_cursor.addRow(new Object[] {
                    key, value
                });
            }

            c = matrix_cursor;
            c.moveToFirst();
            return c;
        } else if (keyCoord.equals(myPort)) {
            Log.e(TAG, "Query is:" + query_given + " & it belongs it ME =  " + keyCoord);
            Cursor c = db.rawQuery("SELECT key,value FROM mytab WHERE key = ?", new String[] {
                query_given
            });
            return c;
        } else if (keyCoord.equals(mycod1) || keyCoord.equals(mycod2)) {
            //ask version to the right coordinator
            Log.i(TAG, "Key belongs to my coord: " + keyCoord);
            Log.e(TAG, "Key= " + query_given);
            Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                query_given
            });
            Message msg = new Message();
            int k = getUID();
            msg.setPid(k);
            factor.put(k, false);
            msg.setType(Message.TYPE.ASK_VERSION);
            msg.setKey(query_given);
            msg.setToPort(keyCoord);
            msg.setFromPort(myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            factorTime.put(k, System.currentTimeMillis());
            while (true) {
                ////
                if (System.currentTimeMillis() > factorTime.get(k) + 2000 || factor.get(k).equals(true)) {
                    break;
                }
            }
            if (factor.get(k).equals(true)) {
                Log.i(TAG, "Exited by choice");
                int ver = factorVersion.get(k);
                c.moveToFirst();
                if (ver > c.getInt(2)) { //ver >= c.getInt(2)
                    MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                        "key", "value"
                    });
                    matrix_cursor.addRow(new Object[] {
                        query_given, factorValue.get(k)
                    });
                    matrix_cursor.moveToFirst();
                    c = matrix_cursor;
                    c.moveToFirst();
                    return c;
                } else {
                    MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                        "key", "value"
                    });
                    matrix_cursor.addRow(new Object[] {
                        query_given, c.getString(1)
                    });
                    matrix_cursor.moveToFirst();
                    c = matrix_cursor;
                    c.moveToFirst();
                    return c;
                }
            } else {
                Log.e(TAG, "403:TimedOut.Now attempting to return from replica the value");
                Cursor my_cursor = db.rawQuery("SELECT key,value FROM mytab WHERE key = ?", new String[] {
                    query_given
                });
                Log.i(TAG, "Returned Replicas cursor for failed coordinator, from--" + myPort);
                my_cursor.moveToFirst();
                return my_cursor;
            }
        } else if (keyCoord.equals(succ1) || keyCoord.equals(succ2)) {

            Log.i(TAG, "Key belongs to my successor: " + keyCoord);
            Message msg = new Message();
            int k = getUID();
            msg.setPid(k);
            factor.put(k, false);
            msg.setType(Message.TYPE.FORWARD_QUERY);
            msg.setKey(query_given);
            msg.setToPort(keyCoord);
            msg.setFromPort(myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            factorTime.put(k, System.currentTimeMillis());
            while (true) {
                //
                if (System.currentTimeMillis() > factorTime.get(k) + 2000 || factor.get(k).equals(true)) {
                    break;
                }
            }
            if (factor.get(k).equals(true)) {
                Log.i(TAG, "Exited by choice");
                MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                    "key", "value"
                });
                matrix_cursor.addRow(new Object[] {
                    query_given, factorValue.get(k)
                });
                matrix_cursor.moveToFirst();
                Cursor c = matrix_cursor;
                c.moveToFirst();
                return c;
            } else {
                Log.e(TAG, "403:TimedOut. Attempting to return value by contacting its replica..");
                Message msg5 = new Message();
                int var5 = getUID();
                msg5.setPid(var5);
                factor.put(var5, false);
                msg5.setType(Message.TYPE.FORWARD_QUERY);
                msg5.setKey(query_given);
                msg5.setToPort(returnSuccessor(keyCoord));
                msg5.setFromPort(myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg5);
                factorTime.put(var5, System.currentTimeMillis());
                while (true) {
                    //
                    if (System.currentTimeMillis() > factorTime.get(var5) + 2000 || factor.get(var5).equals(true)) {
                        break;
                    }
                }
                if (factor.get(var5).equals(true)) {
                    Log.i(TAG, "Got response from succ2 bcz succ1 was dead. Exited by choice.");
                    MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                        "key", "value"
                    });
                    matrix_cursor.addRow(new Object[] {
                        query_given, factorValue.get(var5)
                    });
                    matrix_cursor.moveToFirst();
                    Cursor c = matrix_cursor;
                    c.moveToFirst();
                    return c;
                } else {
                    Log.i(TAG, "Didnt get reply from succ2, forwarding query to its successor.");
                    String last_succ = returnSuccessor(returnSuccessor(keyCoord));
                    Message msg6 = new Message();
                    int var6 = getUID();
                    msg6.setPid(var6);
                    factor.put(var6, false);
                    msg6.setType(Message.TYPE.FORWARD_QUERY);
                    msg6.setKey(query_given);
                    msg6.setToPort(last_succ);
                    msg6.setFromPort(myPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg6);
                    factorTime.put(var6, System.currentTimeMillis());
                    while (true) {
                        //
                        if (System.currentTimeMillis() > factorTime.get(var6) + 2000 || factor.get(var6).equals(true)) {
                            break;
                        }
                    }
                    if (factor.get(var6).equals(true)) {
                        Log.i(TAG, "Got response from succ2 bcz succ1 was dead. Exited by choice.");
                        MatrixCursor matrix_cursor = new MatrixCursor(new String[] {
                            "key", "value"
                        });
                        matrix_cursor.addRow(new Object[] {
                            query_given, factorValue.get(var6)
                        });
                        matrix_cursor.moveToFirst();
                        Cursor c = matrix_cursor;
                        c.moveToFirst();
                        return c;
                    } else {
                        Log.e(TAG, "All nodes are dead");
                    }

                }
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
    String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask < ServerSocket, Message, Void > {@Override
        protected Void doInBackground(ServerSocket...sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    BufferedInputStream b = new BufferedInputStream(clientSocket.getInputStream());
                    ObjectInputStream read = new ObjectInputStream(b);
                    Message msg_rec = (Message) read.readObject();
                    Log.i(TAG, "RECEIVED FROM AVD-PORT " + msg_rec.getFromPort() + " With M-TYPE--" + msg_rec.getType().toString());
                    Log.i(TAG, "To Port= " + msg_rec.getToPort() + " From Port= " + msg_rec.getFromPort());
                    publishProgress(msg_rec);
                    read.close();
                    clientSocket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(Message...mgs) {
            Message msg_rec = mgs[0];
            Log.i(TAG, "RECEIVED FROM AVD-PORT " + msg_rec.getFromPort() + " With M-TYPE--" + msg_rec.getType().toString() + " key= " + msg_rec.getKey());
            //IF MESSAGE IS A INSERT REPLICA
            if (msg_rec.t.equals(Message.TYPE.INSERT_REPLICA)) {
                ContentValues values = new ContentValues();
                values.put("key", msg_rec.getKey());
                values.put("value", msg_rec.getValue());
                values.put("version", msg_rec.getVersion());
                values.put("coord", msg_rec.getCoord());
                db.insertWithOnConflict("mytab", "", values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.i(TAG, "Replica insertion successfull.");
            } else if (msg_rec.getType().equals(Message.TYPE.FORWARD_INSERT)) {
                Log.e(TAG, "Received KEY,VALUE :" + msg_rec.getKey() + "  from port" + msg_rec.getFromPort());
                int ver;
                Message msg3 = new Message();
                msg3.setType(Message.TYPE.INSERTION_REPLY);
                msg3.setPid(msg_rec.getPid());
                msg3.setFromPort(myPort);
                msg3.setToPort(msg_rec.getFromPort());
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    msg_rec.getKey()
                });
                c.moveToFirst();
                if (c.getCount() > 0) {
                    ver = c.getInt(2);
                    ver = ver + 1;
                } else {
                    ver = 1;
                }
                ContentValues put_values = new ContentValues();
                put_values.put("key", msg_rec.getKey());
                put_values.put("value", msg_rec.getValue());
                put_values.put("version", ver);
                put_values.put("coord", myPort);
                db.insertWithOnConflict("mytab", "", put_values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.i(TAG, "Forwarded insert-message stored successfully in " + myPort);
                Log.i(TAG, "Now forwarding to succ1 and succ2 for replication");

                //check if succ1 && succ2 are alive or not;

                Message msg1 = createMessage(Message.TYPE.INSERT_REPLICA, msg_rec.getKey(), msg_rec.getValue(), ver, myPort, succ1, myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
                Message msg2 = createMessage(Message.TYPE.INSERT_REPLICA, msg_rec.getKey(), msg_rec.getValue(), ver, myPort, succ2, myPort, 1);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);
                Log.i(TAG, "Messages to replicas sent, from AVD: " + myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg3);
            } else if (msg_rec.getType().equals(Message.TYPE.FAIL_INSERT)) {

                Log.e(TAG, "Received KEY,VALUE :" + msg_rec.getKey() + "  from port" + msg_rec.getFromPort());
                int ver;
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    msg_rec.getKey()
                });
                c.moveToFirst();
                if (c.getCount() > 0) {
                    ver = c.getInt(2);
                    ver = ver + 1;
                } else {
                    ver = 1;
                }
                ContentValues put_values = new ContentValues();
                put_values.put("key", msg_rec.getKey());
                put_values.put("value", msg_rec.getValue());
                put_values.put("version", ver);
                put_values.put("coord", myPort);
                db.insertWithOnConflict("mytab", "", put_values, SQLiteDatabase.CONFLICT_REPLACE);
                Log.i(TAG, "Fail-insert forwarded and stored successfully in " + myPort);

            } else if (msg_rec.getType().equals(Message.TYPE.INSERTION_REPLY)) {
                int pid_rec = msg_rec.getPid();
                factor.put(pid_rec, true);
                Log.i(TAG, "Insertion completed at coord and replicas, now exiting the TIMED-WHILE.");
            } else if (msg_rec.getType().equals(Message.TYPE.FAIL_PUT_REPLICA)) {
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    msg_rec.getKey()
                });
                c.moveToFirst();
                if (c.getCount() > 0) {
                    int ver = c.getInt(2);
                    ver = ver + 1;
                    ContentValues put_values = new ContentValues();
                    put_values.put("key", msg_rec.getKey());
                    put_values.put("value", msg_rec.getValue());
                    put_values.put("version", ver);
                    put_values.put("coord", msg_rec.getCoord());
                    db.insertWithOnConflict("mytab", "", put_values, SQLiteDatabase.CONFLICT_REPLACE);
                    Log.i(TAG, "Replica insertion UNDER FAILURE successfull.");
                } else {
                    ContentValues put_values = new ContentValues();
                    put_values.put("key", msg_rec.getKey());
                    put_values.put("value", msg_rec.getValue());
                    put_values.put("version", 1);
                    put_values.put("coord", msg_rec.getCoord());
                    db.insertWithOnConflict("mytab", "", put_values, SQLiteDatabase.CONFLICT_REPLACE);
                    Log.i(TAG, "Replica insertion UNDER FAILURE successfull.//This key was inserted for the first time.");
                }
            } else if (msg_rec.getType().equals(Message.TYPE.ASK_VERSION)) {
                int my_version;
                String key_received = msg_rec.getKey();
                String value;
                String coord;
                Log.i(TAG, "Asking version at: " + myPort);
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    key_received
                });
                c.moveToFirst();
                if (c.getCount() > 0) {
                    my_version = c.getInt(2);
                    value = c.getString(1);
                    coord = c.getString(3);
                } else {
                    my_version = 1;
                    my_version = c.getInt(2);
                    value = c.getString(1);
                    coord = c.getString(3);

                }

                int pid = msg_rec.getPid();

                Message msg = new Message();
                msg.setKey(key_received);
                msg.setVersion(my_version);
                msg.setValue(value);
                msg.setCoord(coord);
                msg.setPid(pid);
                msg.setType(Message.TYPE.VERSION_RESPONSE);
                msg.setToPort(msg_rec.getFromPort());
                msg.setFromPort(myPort);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            } else if (msg_rec.getType().equals(Message.TYPE.VERSION_RESPONSE)) {
                int pid_rec = msg_rec.getPid();
                String value_rec = msg_rec.getValue();
                int ver_rec = msg_rec.getVersion();
                factor.put(pid_rec, true);
                factorValue.put(pid_rec, value_rec);
                factorVersion.put(pid_rec, ver_rec);
            } else if (msg_rec.getType().equals(Message.TYPE.FORWARD_QUERY)) {
                String key_received = msg_rec.getKey();
                int pid = msg_rec.getPid();
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    key_received
                });
                c.moveToFirst();

                Message msg = new Message();
                msg.setKey(key_received);
                msg.setPid(pid);
                msg.setValue(c.getString(1));
                msg.setType(Message.TYPE.QUERY_RESPONSE);
                msg.setToPort(msg_rec.getFromPort());
                msg.setFromPort(myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            } else if (msg_rec.getType().equals(Message.TYPE.QUERY_RESPONSE)) {
                int pid_rec = msg_rec.getPid();
                String value_rec = msg_rec.getValue();
                factor.put(pid_rec, true);
                factorValue.put(pid_rec, value_rec);
            } else if (msg_rec.getType().equals(Message.TYPE.DELETE_ALL)) {
                db.delete("mytab", null, null);
            } else if (msg_rec.getType().equals(Message.TYPE.COLLECT_ALL)) {

                HashMap < String, ArrayList < Object >> temp_map = new HashMap < > ();
                String toPort = msg_rec.getFromPort();
                Cursor c = db.rawQuery("SELECT * FROM mytab", null);
                c.moveToFirst();
                int count = c.getCount();
                for (int i = 0; i < count; i++) {
                    String key = c.getString(0);
                    String value = c.getString(1);
                    int version = c.getInt(2);
                    String coord = c.getString(3);

                    ArrayList < Object > new_list = new ArrayList < > ();
                    new_list.add(0, value);
                    new_list.add(1, version);
                    new_list.add(2, coord);
                    temp_map.put(key, new_list);
                    c.moveToNext();
                }

                Message msg1 = new Message();
                msg1.setToPort(toPort);
                msg1.setType(Message.TYPE.TAKE_ALL);
                msg1.setFromPort(myPort);
                msg1.setTuple_map(temp_map);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null);
            } else if (msg_rec.getType().equals(Message.TYPE.TAKE_ALL)) {
                Log.i(TAG, "Got TAKE_ALL reply from= " + msg_rec.getFromPort());
                HashMap < String, ArrayList < Object >> temp_map = msg_rec.getTuple_map();
                for (String got_key: temp_map.keySet()) {
                    if (tuple_result.containsKey(got_key)) {
                        ArrayList < Object > stored_list = tuple_result.get(got_key);
                        int stored_version = (Integer) stored_list.get(1);
                        ArrayList < Object > got_list = temp_map.get(got_key);
                        int got_version = (Integer) got_list.get(1);
                        if (got_version >= stored_version) {
                            tuple_result.put(got_key, got_list);
                        }
                    } else {
                        ArrayList < Object > got_list = temp_map.get(got_key);
                        tuple_result.put(got_key, got_list);
                    }
                }
            } else if (msg_rec.getType().equals(Message.TYPE.DELETE_FORWARD)) {

                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    msg_rec.getKey()
                });
                if (c.getCount() > 0) {

                    c.moveToFirst();
                    db.delete("mytab", "key='" + msg_rec.getKey() + "'", null);
                    Message msg1 = new Message();
                    msg1.setToPort(succ1);
                    msg1.setKey(msg_rec.getKey());
                    msg1.setFromPort(myPort);
                    msg1.setType(Message.TYPE.DELETE_REPLICA);
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, null).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                    Message msg2 = new Message();
                    msg2.setToPort(succ2);
                    msg2.setKey(msg_rec.getKey());
                    msg2.setFromPort(myPort);
                    msg2.setType(Message.TYPE.DELETE_REPLICA);
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, null).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            } else if (msg_rec.getType().equals(Message.TYPE.DELETE_REPLICA)) {
                Cursor c = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                    msg_rec.getKey()
                });
                if (c.getCount() > 0) {
                    db.delete("mytab", "key='" + msg_rec.getKey() + "'", null);
                } else {
                    Log.e(TAG, "No Entry found for Replica-Deletion.");
                }
            } else if (msg_rec.getType().equals(Message.TYPE.WOKEUP)) {
                Cursor check_cursor = db.rawQuery("SELECT * FROM mytab", null);
                if (check_cursor.getCount() > 0) {
                    check_cursor.moveToFirst();
                    String got_coord = msg_rec.getFromPort();
                    HashMap < String, ArrayList < Object >> toPass = new HashMap < > ();

                    if (got_coord.equals(succ1) || got_coord.equals(succ2)) {
                        Cursor temp_cursor = db.rawQuery("SELECT * FROM mytab WHERE coord = ?", new String[] {
                            myPort
                        });
                        temp_cursor.moveToFirst();
                        int cursor_count = temp_cursor.getCount();
                        for (int i = 0; i < cursor_count; i++) {
                            ArrayList < Object > temp_arr = new ArrayList < > ();
                            temp_arr.add(temp_cursor.getString(1));
                            temp_arr.add(temp_cursor.getInt(2));
                            temp_arr.add(temp_cursor.getString(3));
                            toPass.put(temp_cursor.getString(0), temp_arr);
                            temp_cursor.moveToNext();
                        }
                        Message msg = new Message();
                        msg.setToPort(msg_rec.getFromPort());
                        msg.setType(Message.TYPE.MORNING_FROM_COORD);
                        msg.setFromPort(myPort);
                        msg.setTuple_map(toPass);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null);
                        Log.i(TAG, "Morning reply sent to my Succ=" + msg_rec.getFromPort() + " with all  My-Tuples it missed");
                    } else if (got_coord.equals(mycod1) || got_coord.equals(mycod2)) {
                        Cursor temp_cursor = db.rawQuery("SELECT * FROM mytab WHERE coord = ?", new String[] {
                            got_coord
                        });
                        temp_cursor.moveToFirst();
                        int cursor_count = temp_cursor.getCount();
                        for (int i = 0; i < cursor_count; i++) {
                            ArrayList < Object > temp_arr = new ArrayList < > ();
                            temp_arr.add(temp_cursor.getString(1));
                            temp_arr.add(temp_cursor.getInt(2));
                            temp_arr.add(temp_cursor.getString(3));
                            toPass.put(temp_cursor.getString(0), temp_arr);
                            temp_cursor.moveToNext();
                        }
                        Message msg = new Message();
                        msg.setToPort(msg_rec.getFromPort());
                        msg.setType(Message.TYPE.MORNING_FROM_SUCCESSOR);
                        msg.setFromPort(myPort);
                        msg.setTuple_map(toPass);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, null);
                        Log.i(TAG, "Morning reply sent to my Cord=" + msg_rec.getFromPort() + " with all Its-Tuples it missed");
                    }
                } else {
                    Log.i(TAG, "Everyone has just woken up. No messages to send.");
                }
            } else if (msg_rec.getType().equals(Message.TYPE.MORNING_FROM_COORD)) {
                HashMap < String, ArrayList < Object >> gotMap = new HashMap < > ();
                gotMap = msg_rec.getTuple_map();
                for (Map.Entry < String, ArrayList < Object >> each_entry: gotMap.entrySet()) {
                    String got_key = each_entry.getKey();
                    ArrayList < Object > got_list = each_entry.getValue();
                    String got_value = (String) got_list.get(0);
                    int got_version = (Integer) got_list.get(1);
                    String got_coord = (String) got_list.get(2);
                    ContentValues values = new ContentValues();
                    values.put("key", got_key);
                    values.put("value", got_value);
                    values.put("version", got_version);
                    values.put("coord", got_coord);
                    db.insertWithOnConflict("mytab", "", values, SQLiteDatabase.CONFLICT_REPLACE);
                }

            } else if (msg_rec.getType().equals(Message.TYPE.MORNING_FROM_SUCCESSOR)) {
                HashMap < String, ArrayList < Object >> receivedMap = new HashMap < > ();
                receivedMap = msg_rec.getTuple_map();
                for (Map.Entry < String, ArrayList < Object >> each_entry: receivedMap.entrySet()) {
                    String got_key = each_entry.getKey();
                    ArrayList < Object > got_list = each_entry.getValue();
                    String got_value = (String) got_list.get(0);
                    int got_version = (Integer) got_list.get(1);
                    String got_coord = (String) got_list.get(2);
                    Cursor c_temp = db.rawQuery("SELECT * FROM mytab WHERE key = ?", new String[] {
                        got_key
                    });
                    c_temp.moveToFirst();
                    if (c_temp.getCount() > 0) {
                        if (c_temp.getInt(2) <= got_version) {
                            ContentValues values = new ContentValues();
                            values.put("key", got_key);
                            values.put("value", got_value);
                            values.put("version", got_version);
                            values.put("coord", got_coord);
                            db.insertWithOnConflict("mytab", "", values, SQLiteDatabase.CONFLICT_REPLACE);
                        }
                    } else {
                        ContentValues val = new ContentValues();
                        val.put("key", got_key);
                        val.put("value", got_value);
                        val.put("version", got_version);
                        val.put("coord", got_coord);
                        db.insertWithOnConflict("mytab", "", val, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                }
            }
        }
    }
    private class ClientTask extends AsyncTask < Message, Void, Void > {@Override
        protected Void doInBackground(Message...msgs) {
            Message msgToSend = msgs[0]; {
                if (msgToSend.getType() == Message.TYPE.FORWARD_INSERT) Log.i("forwarding insert", "KEY BEING forwarded IS " + msgToSend.getKey() + " to port-->" + msgToSend.getToPort() + " from port-->" + msgToSend.getFromPort());
                else if (msgToSend.getType() == Message.TYPE.INSERT_REPLICA) Log.i("forwarding for replication", "KEY BEING forwarded IS " + msgToSend.getKey() + " to port-->" + msgToSend.getToPort() + " from port-->" + msgToSend.getFromPort());
                else if (msgToSend.getType() == Message.TYPE.ASK_VERSION) Log.i("Asking for the version from the coordinator", "KEY BEING forwarded IS " + msgToSend.getKey() + " to port-->" + msgToSend.getToPort() + " from port-->" + msgToSend.getFromPort());
                else if (msgToSend.getType() == Message.TYPE.VERSION_RESPONSE) Log.i("Asking for the version from the coordinator", "KEY BEING forwarded IS " + msgToSend.getKey() + " to port-->" + msgToSend.getToPort() + " from port-->" + msgToSend.getFromPort());
                else if (msgToSend.getType() == Message.TYPE.DELETE_ALL) Log.i("DELETE ALL QUERY GOING ON", "KEY BEING forwarded IS " + msgToSend.getKey() + " to port-->" + msgToSend.getToPort() + " from port-->" + msgToSend.getFromPort());

                Log.i(TAG, "SENDING FROM AVD-PORT " + msgToSend.getFromPort() + " With M-TYPE--" + msgToSend.getType().toString() + " key= " + msgToSend.getKey());
                // ARTW send it
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
                        10, 0, 2, 2
                    }),
                    Integer.parseInt(msgToSend.getToPort()));
                    BufferedOutputStream b = new BufferedOutputStream(socket.getOutputStream());
                    ObjectOutputStream objsend = new ObjectOutputStream(b);
                    objsend.writeObject(msgToSend);
                    objsend.flush();
                    objsend.close();
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "Client IOException");
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b: sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    //My DB helper class
    class dbhelp extends SQLiteOpenHelper {
        public static final String db_name = "my_data.db";
        public dbhelp(Context context) {
            super(context, db_name, null, 4);
        }
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE mytab (key TEXT PRIMARY KEY, value TEXT NOT NULL, version INT , coord TEXT );");
            Log.i(TAG, "mytab created for node: " + (Integer.parseInt(myPort) / 2));
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {}
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {}
    }
}