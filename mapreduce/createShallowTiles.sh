#!/usr/bin/env bash

export OUTPUTDIR=output-reducer

# At each stage, the number of reducers must be equal to 2**(zoomDepthWidest+1).
# Arguments to mapper-nestedKeys.py and reducer-collate-Accumulo.py are zoomDepthNarrowest zoomDepthWidest.

python listOfTiles.py 10 > /tmp/tiles_T10.txt
/opt/hadoop/bin/hadoop fs -copyFromLocal /tmp/tiles_T10.txt .

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D mapred.reduce.tasks=1024 -mapper "mapper-nestedKeys.py 10 9" -reducer "reducer-collate-Accumulo.py 10 9" -file mapper-nestedKeys.py -file reducer-collate-Accumulo.py -file utilities.py -file ../jobconfig.ini -cmdenv PYTHONPATH=/opt/lib/python -input tiles_T09.txt -output $OUTPUTDIR-10-09

python listOfTiles.py 9 > /tmp/tiles_T09.txt
/opt/hadoop/bin/hadoop fs -copyFromLocal /tmp/tiles_T09.txt .

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D mapred.reduce.tasks=64 -mapper "mapper-nestedKeys.py 9 5" -reducer "reducer-collate-Accumulo.py 9 5" -file mapper-nestedKeys.py -file reducer-collate-Accumulo.py -file utilities.py -file ../jobconfig.ini -cmdenv PYTHONPATH=/opt/lib/python -input tiles_T09.txt -output $OUTPUTDIR-09-05

# When there's only one process, it doesn't need to be in Hadoop.
python listOfTiles.py 5 > /tmp/tiles_T05.txt
cat /tmp/tiles_T05.txt | ./mapper-nestedKeys.py 5 0 | ./reducer-collate-Accumulo.py 5 0
