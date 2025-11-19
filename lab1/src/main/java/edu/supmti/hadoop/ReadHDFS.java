package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (args.length < 1) {
            System.err.println(
                    "Usage : ReadHDFS <filepath>");
            System.exit(1);
        }
        Path nomcomplet = new Path(args[0]); //"/user/root/purchases.txt"
        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println(line);
        inStream.close();
        fs.close();
    }
}
