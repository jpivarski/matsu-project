package org.occ.matsu;

import java.net.URI;
import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public class SequenceFileInterface {
    static SequenceFile.Reader reader = null;
    static SequenceFile.Writer writer = null;

    public static void openForReading(String fileName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        reader = new SequenceFile.Reader(fileSystem, new Path(fileName), configuration);
    }

    public static String read(String key) throws IOException {
        if (reader == null) { throw new IOException(); }

        reader.sync(0);
        Text testkey = new Text();
        while (reader.next(testkey)) {
            if (testkey.toString().equals(key)) {
                Text value = new Text();
                reader.getCurrentValue(value);
                return value.toString();
            }
        }
        return null;
    }

    public static void openForWriting(String fileName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        writer = new SequenceFile.Writer(fileSystem, configuration, new Path(fileName), Text.class, Text.class);
    }

    public static void write(String key, String value) throws IOException {
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
