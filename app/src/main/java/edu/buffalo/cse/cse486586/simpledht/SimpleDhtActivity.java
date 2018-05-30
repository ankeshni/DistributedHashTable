package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class SimpleDhtActivity extends Activity {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    public static int numbr_of_nodes=1;
    public static String my_avd_hash=null;
    public static String delim="!@#%&";
    int port=0;//Index of port argument in input of client task
    static String myPort;
    int message=1;//Index of messaage argument in input string of client task
    public  static List<String> hashed_avds_list = new ArrayList<String>();
    public  static HashMap<String,String>hash_to_port= new HashMap<String, String>();
    public  static List<String>topology = new ArrayList<String>();
    public static String nxt_node=null;
    public static String prev_node=null;



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        my_avd_hash=SimpleDhtProvider.genHash(portStr);
        //Display my port number and avd number
        String strReceived = "my port is "+myPort+"I am avd "+portStr;
        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
        remoteTextView.append(strReceived + "\t\n");
        TextView localTextView = (TextView) findViewById(R.id.textView1);
        localTextView.append("\n");
        //Start Server
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        //fill up lookup data structures
        for(int i=5554;i<=5562;i+=2){
            hashed_avds_list.add(SimpleDhtProvider.genHash(Integer.toString(i)));
            hash_to_port.put(SimpleDhtProvider.genHash(Integer.toString(i)),Integer.toString(i*2));
        }
        Collections.sort(hashed_avds_list);
        topology.add(SimpleDhtProvider.genHash(portStr));
        nxt_node=SimpleDhtProvider.genHash(portStr);
        prev_node=SimpleDhtProvider.genHash(portStr);

        //register yourself to the ring
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,REMOTE_PORT0,"request to join ring my portnumber is"+delim+myPort);


    }




    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                Log.v("server up","running at port"+myPort);
                while (true) {
                    Socket accepted_socket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(accepted_socket.getInputStream()));
                    String received_message = br.readLine();
                    Log.v("server",received_message);
                    if (received_message != null){
                        String [] str=received_message.split(delim);
                        if(str[0].equals("request to join ring my portnumber is")){
                            if(!SimpleDhtProvider.genHash(Integer.toString(Integer.parseInt(str[1])/2)).equals(my_avd_hash))
                            topology.add(SimpleDhtProvider.genHash(Integer.toString(Integer.parseInt(str[1])/2)));
                            Collections.sort(topology);
                            brodcast_new_topology();
                        }
                        else if(str[0].equals("updated topology")){
                            topology.clear();
                            for(int i=1;i<str.length;i++){
                                topology.add(str[i]);
                                numbr_of_nodes=topology.size();
                            }
                            Collections.sort(topology);
                            int i=topology.indexOf(my_avd_hash);
                            if(i==0){
                                if(numbr_of_nodes==1)
                                    nxt_node= topology.get(0);
                                else
                                    nxt_node= topology.get(1);
                                prev_node= topology.get(topology.size() - 1);
                            }
                            else if(i==topology.size()-1){
                                nxt_node= topology.get(0);
                                prev_node= topology.get(topology.size() - 2);
                            }
                            else{
                                nxt_node= topology.get(i + 1);
                                prev_node= topology.get(i - 1);
                            }
                        }
                        else if(str[0].equals("insert")){
                            ContentValues value=new ContentValues();
                            value.put("key",str[1]);
                            value.put("value",str[2]);
                           //force insert
                            SimpleDhtProvider.myDb.insert(value,1);
                            //Log.v("server","force insert");
                        }
                        else if(str[0].equals("query")){
                            Cursor c=SimpleDhtProvider.myDb.query_response(str[1],1);
                            c.moveToFirst();
                            int keyIndex = c.getColumnIndex("key");
                            int valueIndex = c.getColumnIndex("value");
                            String returnKey = c.getString(keyIndex);
                            String returnValue = c.getString(valueIndex);

                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                            bw.write(returnKey+delim+returnValue + "\n");
                            bw.flush();
                        }
                        else if(str[0].equals("*")){
                            Cursor c=SimpleDhtProvider.myDb.query_response("@",0);
                            String keys=delim;
                            String values=delim;
                            c.moveToFirst();
                            while(!c.isAfterLast()){
                              keys+=c.getString(c.getColumnIndex("key"))+delim;
                              values+=c.getString(c.getColumnIndex("value"))+delim;
                              c.moveToNext();
                            }
                            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(accepted_socket.getOutputStream()));
                            bw.write(keys+"\n");
                            //bw.flush();
                            bw.write(values+"\n");
                            bw.flush();
                        }
                    }
                    accepted_socket.close();
                    br.close();
                    Log.v("topology",topology.toString()+" nodes "+numbr_of_nodes+" prev "+prev_node+" me "+my_avd_hash+" next "+nxt_node);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private void brodcast_new_topology() {
        String msg="updated topology"+delim;
        for(String i:topology){
            msg+=i+delim;
        }
        try {
            Socket socket;
            for(int ji =Integer.parseInt(REMOTE_PORT0);ji<=11124;ji=ji+4){
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),ji);

                String msgToSend = msg;
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bw.write(msgToSend+"\n");
                bw.flush();
                //socket.close();
            }

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }
    }

    protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");
        }
        private class ClientTask extends AsyncTask<String, Void, Void> {

            @Override
            protected Void doInBackground(String... msgs) {
                try {
                    Log.v("client up","client running at "+myPort);
                    Socket socket;
                    int ji =Integer.parseInt(msgs[port]);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),ji);
                    String msgToSend = msgs[message];
                    if(msgToSend.equals("request to join ring my portnumber is"+delim+myPort)) {
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        bw.write(msgToSend+"\n");
                        bw.flush();
                        Log.v("client","request to join ring my portnumber is"+delim+myPort);
                    }
                }
                 catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

                return null;
            }
        }




    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}


