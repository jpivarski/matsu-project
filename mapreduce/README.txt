Mapper-only jobs:

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=true -D org.occ.matsu.restrictBandsTo='["B05","B04","B02"]' -D mapred.reduce.tasks=0 -D mapred.min.split.size=107374182400 -libjars /opt/matsuSequenceFileInterface.jar -input /user/hadoop/sequence-files/eo1/ali_l1g/2012/*/*.sequence -mapper mapper-L1G-tiles-Accumulo.py -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file mapper-L1G-tiles-Accumulo.py -file utilities.py -file ../jobconfig.ini -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -output out-2012-10-03-f

# need to update this!
