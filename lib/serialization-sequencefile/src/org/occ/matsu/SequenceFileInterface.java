package org.occ.matsu;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public class SequenceFileInterface {
    static List<String> shoppingList;
    static SequenceFile.Reader reader = null;
    static SequenceFile.Writer writer = null;

    public static void openForReading(String fileName, String[] requestedItems) throws IOException {
        // NOTE: only local files have been implemented.
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        reader = new SequenceFile.Reader(fileSystem, new Path(fileName), configuration);

        shoppingList = new ArrayList<String>(requestedItems.length);
        for (String item : requestedItems) {
            shoppingList.add(item);
        }
    }

    public static String[] readNext() throws IOException {
        if (reader == null) { throw new IOException(); }

        Text key = new Text();
        while (reader.next(key)) {
            if (shoppingList.contains(key.toString())) {
                Text value = new Text();
                reader.getCurrentValue(value);

                String[] output = new String[2];
                output[0] = key.toString();
                output[1] = value.toString();
                return output;
            }
        }
        return null;
    }

    public static void openForWriting(String fileName, boolean local) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem;
        if (local) {
            configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            fileSystem = FileSystem.getLocal(configuration);
        } else {
            // NOTE: doesn't work.  Use local files only.
            configuration.addResource(new Path("/etc/hadoop/conf.pseudo.mr1/core-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf.pseudo.mr1/hdfs-site.xml"));
            fileSystem = FileSystem.get(configuration);
        }
        writer = new SequenceFile.Writer(fileSystem, configuration, new Path(fileName), Text.class, Text.class);
    }

    public static void write(String key, byte[] value) throws IOException {
        if (writer == null) { throw new IOException(); }
        writer.append(new Text(key), new Text(value));
    }

    public static void sync() throws IOException {
        if (writer == null) { throw new IOException(); }
        writer.sync();
    }

    public static void closeWriting() throws IOException {
        if (writer == null) { throw new IOException(); }
        writer.close();
    }
}
