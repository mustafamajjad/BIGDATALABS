package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (args.length < 2) {
            System.err.println("Usage : ReadHDFS  <filepath> <text-to-write>");
            System.exit(1);
        }
        Path nomcomplet = new Path(args[0]);
        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet);
            outStream.writeUTF("Bonjour tout le monde !\n");
            outStream.writeUTF(args[1]);
            outStream.close();
        } else {
            System.err.println("file exists");
        }
        fs.close();
    }
}