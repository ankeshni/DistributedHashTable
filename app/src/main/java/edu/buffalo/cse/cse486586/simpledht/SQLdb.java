package edu.buffalo.cse.cse486586.simpledht;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.TAG;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.delim;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.hash_to_port;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.myPort;
import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtActivity.topology;

/**
 * Created by Ankesh N. Bhoi on 02/11/2018.
 */

public class SQLdb extends SQLiteOpenHelper {

    int message=1,port=2,type=0;

    public static final String db_name = "kvalpair.db";
    public static final String tbl_name = "kvalpair";


    public SQLdb(Context context) {
        super(context, db_name, null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        sqLiteDatabase.execSQL("create table " + tbl_name + " (`key` TEXT PRIMARY KEY,`value` TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS kvalpairs.db");
        onCreate(sqLiteDatabase);
    }

    public void insert(ContentValues values,int force) {
        if(force==1){
            SQLiteDatabase db = this.getWritableDatabase();
            Object key = values.get("key");
            db.delete(tbl_name, "key=" + "'" + key + "'", null);
            final long check = db.insert(tbl_name, null, values);
            Log.v("insert",values.toString()+" force");
        }
        else if ((SimpleDhtActivity.nxt_node).equals(SimpleDhtActivity.my_avd_hash)) {//only one avd
            SQLiteDatabase db = this.getWritableDatabase();
            Object key = values.get("key");
            db.delete(tbl_name, "key=" + "'" + key + "'", null);
            final long check = db.insert(tbl_name, null, values);
            Log.v("insert",values.toString()+" one node");
        } else if (SimpleDhtProvider.genHash(values.get("key").toString()).equals(SimpleDhtActivity.my_avd_hash)) {//key hash=myhash
            SQLiteDatabase db = this.getWritableDatabase();
            Object key = values.get("key");
            db.delete(tbl_name, "key=" + "'" + key + "'", null);
            final long check = db.insert(tbl_name, null, values);
            Log.v("insert",values.toString()+"key hash same");
        } else {//insert at other avd
            List<String> position_finder = SimpleDhtActivity.topology;
            position_finder.add(SimpleDhtProvider.genHash(values.get("key").toString()));
            Collections.sort(position_finder);
            if (position_finder.indexOf(SimpleDhtProvider.genHash(values.get("key").toString())) == position_finder.size() - 1) {//if last index
                //send key val to port
                Log.v("remote insert",values.toString()+SimpleDhtActivity.hash_to_port.get(position_finder.get(0)));
                new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert",values.get("key").toString()+SimpleDhtActivity.delim+values.get("value").toString(), SimpleDhtActivity.hash_to_port.get(position_finder.get(0)));

            } else {
                int pos = position_finder.indexOf(SimpleDhtProvider.genHash(values.get("key").toString()));
                //send key val to port
                Log.v("remote insert",values.toString()+SimpleDhtActivity.hash_to_port.get(position_finder.get(pos+1)));
                new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", values.get("key").toString()+SimpleDhtActivity.delim+values.get("value").toString(), SimpleDhtActivity.hash_to_port.get(position_finder.get(pos + 1)));
            }
            position_finder.remove(SimpleDhtProvider.genHash(values.get("key").toString()));
        }

    }


    public Cursor query_response(String selection,int force) throws ExecutionException, InterruptedException {
        if (force==1) {//query from server
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select * from " + tbl_name+ " WHERE `key`= " + "'" + selection + "'", null);
            //res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if ((selection.equals("*") || selection.equals("@")) && (SimpleDhtActivity.nxt_node).equals(SimpleDhtActivity.my_avd_hash)) {//one node
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select * from " + tbl_name, null);
            //res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if (selection.equals("@")) {//multiple nodes
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select * from " + tbl_name, null);
            //res.moveToFirst();
            int x = res.getCount();
            return res;
        }
        else if ((SimpleDhtActivity.nxt_node).equals(SimpleDhtActivity.my_avd_hash)) {//my hash == key hash
            SQLiteDatabase db = this.getWritableDatabase();
            Cursor res = db.rawQuery("select * from " + tbl_name + " WHERE `key`= " + "'" + selection + "'", null);
            res.moveToFirst();
            return res;
        }
         else if(selection.equals("*")){
            return  new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*",selection,myPort).get();
        }
        else if(selection.equals("i dont know why i need this but return null at the end stops working if i dont do this")){}//hack
        else {
            List<String>position_finder=new ArrayList<String>();
            position_finder=topology;
            position_finder.add(SimpleDhtProvider.genHash(selection));
            Collections.sort(position_finder);


            if (position_finder.indexOf(SimpleDhtProvider.genHash(selection)) == position_finder.size() - 1){//query node 0
                //if last index
                Log.v("remote query",selection+SimpleDhtActivity.hash_to_port.get(position_finder.get(0)));
                String prt=SimpleDhtActivity.hash_to_port.get(position_finder.get(0));
                position_finder.remove(SimpleDhtProvider.genHash(selection));
                return  new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query",selection,prt).get();

            }
            else{//any other index query next node of selection hash
                int pos = position_finder.indexOf(SimpleDhtProvider.genHash(selection.toString()));
                //send key val to port
                Log.v("remote query",selection+SimpleDhtActivity.hash_to_port.get(position_finder.get(pos+1)));
                String prt=SimpleDhtActivity.hash_to_port.get(position_finder.get(pos + 1));
                position_finder.remove(SimpleDhtProvider.genHash(selection));
                return new send().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", selection, prt).get();
            }

        }
        return null;
    }

    public void delete(String selection) {
        String key = selection;
        SQLiteDatabase db = this.getWritableDatabase();
        //db.delete(tbl_name, "key=" + "'" + key + "'", null);
        db.delete(tbl_name,null,null);
    }

    private class send extends AsyncTask<String, Void, Cursor> {

        @Override
        protected Cursor doInBackground(String... msgs) {
            try {
                if(msgs[type].equals("*")){
                    String[] rows={"key","value"};
                    MatrixCursor c=new MatrixCursor(rows);

                    //for(int i =Integer.parseInt(SimpleDhtActivity.REMOTE_PORT0);i<=11124;i=i+4){
                    for(String i:topology){
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(hash_to_port.get(i)));
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        bw.write("*"+"\n");
                        bw.flush();
                        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String keys = br.readLine();Log.v("*k",keys);
                        String values=br.readLine();Log.v("*v",values);
                        String[] k=keys.split(delim);
                        String[] v=values.split(delim);
                        Log.v("kval","klen "+k.length+" vlen "+v.length);
                        for(int j=0;j<k.length;j++){
                            c.addRow(new Object[] {k[j],v[j]});
                        }

                    }
                  return c;
                }
                else if(msgs.length==3){
                Log.v("sender up",msgs[message] );
                Socket socket;
                int ji = Integer.parseInt(msgs[port]);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), ji);
                String msgToSend = msgs[type]+delim+msgs[message];
                if (msgs[type].equals("insert")) {
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v("send", msgToSend);
                }
                else if (msgs[type].equals("query")) {
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bw.write(msgToSend + "\n");
                    bw.flush();
                    Log.v("send", msgToSend);

                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String received_message = br.readLine();
                    String[] str=received_message.split(delim);
                    Cursor c;
                    String[]stri ={"key","value"};
                    MatrixCursor mc=new MatrixCursor(stri);
                    mc.addRow(new Object[] {str[0],str[1]});
                    return mc;
                }
            }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}