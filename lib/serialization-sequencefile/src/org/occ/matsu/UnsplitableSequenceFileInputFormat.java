package org.occ.matsu;

import org.occ.matsu.SequenceFileSkipKeysRecordReader;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;

public class UnsplitableSequenceFileInputFormat extends FileInputFormat<Text, Text> {
    SequenceFileSkipKeysRecordReader reader = null;

    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
	reader = new SequenceFileSkipKeysRecordReader();
	reader.initialize(split, (Configuration)conf);
        return reader;
    }

    protected boolean isSplitable(JobContext context, Path file) {
	return false;
    }
}
