Running a "map-reduce" job on the command-line:

cat /tmp/input.serialized | python tile-mapper.py | sort | python tile-reducer.py

Running a real map-reduce job:

hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=true -D org.occ.matsu.restrictBandsTo='["B016","B023","B029"]' -D mapred.reduce.tasks=1 -D mapred.min.split.size=107374182400 -libjars /opt/matsuSequenceFileInterface.jar -input sequencefiles/*.sequencefile -mapper tile-mapper.py -reducer tile-reducer.py -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file tile-mapper.py -file tile-reducer.py -file ../CONFIG.ini -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -output output1

Note: org.occ.matsu.restrictBands and org.occ.matsu.restrictBandsTo must be equal to mapper.restrictBands and mapper.restrictBandsTo in CONFIG.ini!

A large map-reduce job:

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=true -D org.occ.matsu.restrictBandsTo='["B05","B04","B02"]' -D mapred.reduce.tasks=32 -D mapred.min.split.size=107374182400 -D stream.map.output.field.separator=. -D stream.num.map.output.key.fields=2 -D map.output.key.field.separator=. -D num.key.fields.for.partition=1 -libjars /opt/matsuSequenceFileInterface.jar -input /user/hadoop/sequence-files/eo1/ali_l1g/2012/0*/*.sequence -mapper tile-mapper.py -reducer tile-reducer.py -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file tile-mapper.py -file tile-reducer.py -file ../CONFIG.ini -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner -output out-2012-10-02-g
