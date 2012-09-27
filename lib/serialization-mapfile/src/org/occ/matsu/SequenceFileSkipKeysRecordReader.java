package org.occ.matsu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.SequenceFile;

public class SequenceFileSkipKeysRecordReader implements RecordReader<Text, Text> {
    protected Configuration configuration;
    private SequenceFile.Reader reader;
    private long start;
    private long end;
    boolean more = true;

    public SequenceFileSkipKeysRecordReader() { }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException {
        Path path = ((FileSplit)(split)).getPath();
        FileSystem fs = path.getFileSystem(conf);

        configuration = conf;
        reader = new SequenceFile.Reader(fs, path, conf);

        start = ((FileSplit)(split)).getStart();
        end = start + split.getLength();
        if (start > reader.getPosition()) {
            reader.sync(start);
        }

        start = reader.getPosition();
        more = start < end;

        System.err.println(configuration.get("fs.file.impl"));
    }

    public Class getKeyClass() { return reader.getKeyClass(); }
    public Class getValueClass() { return reader.getValueClass(); }
    public Text createKey() { return new Text(); }
    public Text createValue() { return new Text(); }

    public synchronized boolean next(Text key, Text value) throws IOException {
        if (!more) return false;
        long pos = reader.getPosition();
        boolean remaining = reader.next(key);

        // HERE

        if (remaining) {
            reader.getCurrentValue(value);
        }
        if (pos >= end  &&  reader.syncSeen()) {
            more = false;
        } else {
            more = remaining;
        }
        return more;
    }

    public float getProgress() throws IOException {
        if (end == start) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (reader.getPosition() - start) / (float)(end - start));
        }
    }

    public synchronized long getPos() throws IOException {
        return reader.getPosition();
    }

    public synchronized void close() throws IOException {
        reader.close();
    }

}
