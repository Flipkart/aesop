package com.flipkart.aesop.elasticsearchdatalayer;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
/**
 * Created with IntelliJ IDEA.
 * User: pratyay.banerjee
 * Date: 03/12/14
 * Time: 4:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class DialServer {
    private static final String url = "http://localhost:30000";
    private static final String requestMethod = "GET";
    private static final int maxAttempts = 5;
    private static final int retryPeriod = 10000;
    public static void checkServer() {
        for (int i = 0; i <= maxAttempts; i++) {
            try {
                int responseCode = sendGet();
                if(responseCode==200)
                {
                    System.out.println("Break Loop and Return");
                    break;
                }
            } catch (ConnectException e) {
                if(i==maxAttempts)
                {
                    System.out.println("Max Attempts Have Reached for Server Response...ALERT Now");
                }
                else
                {
                    System.out.println("Connect Exception..Try Again Till Max Attempts...");
                    try {
                        Thread.currentThread().sleep(retryPeriod);
                    } catch (InterruptedException e1) {
// TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }
            } catch (Exception e) {
                System.out.println("Generic Exception Connecting To Server...");
                e.printStackTrace();
            }
        }
    }
    private static int sendGet() throws Exception {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
// optional default is GET
        con.setRequestMethod(requestMethod);
        System.out.println(con.getInputStream());
        int responseCode = con.getResponseCode();
        System.out.println("Response Code : " + responseCode);
        con.disconnect();
        return responseCode;
    }
}
