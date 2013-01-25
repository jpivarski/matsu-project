#!/usr/bin/env bash

export OUTPUTDIR=output-createPolygons
export LOGFILE=/tmp/createPolygons
export RESTRICT_BANDS=true
echo "[]" > /tmp/bands.json
export RESTRICT_BANDS_TO=`cat /tmp/bands.json`

/opt/hadoop/bin/hadoop fs -rmr $OUTPUTDIR

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=$RESTRICT_BANDS -D org.occ.matsu.restrictBandsTo=$RESTRICT_BANDS_TO -D mapred.reduce.tasks=0 -D mapred.min.split.size=107374182400 -libjars /opt/matsuSequenceFileInterface.jar -input /user/hadoop/sequence-files/eo1/ali_l1g/2012/*/*.sequence -mapper "\"mapper-L1G-polygons-Accumulo.py bands.json\"" -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file mapper-L1G-polygons-Accumulo.py -file utilities.py -file ../jobconfig.ini -file /tmp/bands.json -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -output $OUTPUTDIR

/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=$RESTRICT_BANDS -D org.occ.matsu.restrictBandsTo=$RESTRICT_BANDS_TO -D mapred.reduce.tasks=0 -D mapred.min.split.size=107374182400 -libjars /opt/matsuSequenceFileInterface.jar -input /user/hadoop/sequence-files/eo1/hyperion_l1g/2012/*/*.sequence -mapper "\"mapper-L1G-polygons-Accumulo.py bands.json\"" -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file mapper-L1G-polygons-Accumulo.py -file utilities.py -file ../jobconfig.ini -file /tmp/bands.json -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -output $OUTPUTDIR
