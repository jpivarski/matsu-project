#!/usr/bin/env bash

export OUTPUTDIR=output-createDeepTiles
export LOGFILE=/tmp/createDeepTiles
export RESTRICT_BANDS=true
cat > /tmp/bands.json << EOF
["B05","B04","B02"]
EOF
# if RESTRICT_BANDS is false, set bands.json to 'null' (unquoted)

export RESTRICT_BANDS_TO=`cat /tmp/bands.json`
for SMALLCHUNK in '/user/hadoop/sequence-files/eo1/ali_l1g/2012/2[3-5]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/2[1-2]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/{19,20}*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/1[7-8]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/1[5-6]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/1[3-4]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/1[0-2]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/0[7-9]*/*.sequence' \
                  '/user/hadoop/sequence-files/eo1/ali_l1g/2012/0[0-6]*/*.sequence'; do
    x=$RANDOM;
    echo SMALLCHUNK $SMALLCHUNK x $x;
    (/opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -D org.occ.matsu.restrictBands=$RESTRICT_BANDS -D org.occ.matsu.restrictBandsTo=$RESTRICT_BANDS_TO -D mapred.reduce.tasks=0 -D mapred.min.split.size=107374182400 -libjars /opt/matsuSequenceFileInterface.jar -input $SMALLCHUNK -mapper "\"mapper-L1G-tiles-Accumulo.py bands.json\"" -inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat -file mapper-L1G-tiles-Accumulo.py -file utilities.py -file ../jobconfig.ini -file /tmp/bands.json -cmdenv PYTHONPATH=/opt/lib/python -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -output $OUTPUTDIR-$x > $LOGFILE-$x.log 2>&1 &)

done
