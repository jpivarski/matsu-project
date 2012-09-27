Running a "map-reduce" job on the command-line:

cat /tmp/input.serialized | python tile-mapper.py | sort | python tile-reducer.py

Running a real map-reduce job:

hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D mapred.reduce.tasks=1 -D mapred.min.split.size=107374182400 -libjars ../lib/serialization-mapfile/matsuSequenceFileInterface.jar -input sequencefiles/*.sequencefile -mapper tile-mapper.py -reducer tile-reducer.py -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file tile-mapper.py -file tile-reducer.py -file ../CONFIG.ini -cmdenv PYTHONPATH=/home/pivarski/lib/python -cmdenv LD_LIBRARY_PATH=/home/pivarski/lib -output output1
