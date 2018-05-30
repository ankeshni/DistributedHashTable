package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by Ankesh N. Bhoi on 03/31/2018.
 */

public class Node {
    Node(String my_port,String next_port,String previous_port){
      this.my_port=my_port;
      this.avd_no=Integer.toString((Integer.parseInt(avd_no)/2));
      this.hashed_avd_no=SimpleDhtProvider.genHash(this.avd_no);
      this.next_port=next_port;
      this.previous_port=previous_port;

    }
    public String my_port;
    public String avd_no;
    public String hashed_avd_no;
    public String next_port;
    public String previous_port;
}
