#!/usr/bin/env bash

export OUTPUTDIR=output-createDeepTiles
export RESTRICT_BANDS=true
cat > /tmp/bands.json << EOF
["B02","B03","B04","B05","B06","B07","B08","B09","B10"]
EOF
# if RESTRICT_BANDS is false, set bands.json to 'null' (unquoted)
export RESTRICT_BANDS_TO=`cat /tmp/bands.json`

/opt/hadoop/bin/hadoop fs -ls /user/hadoop/sequence-files/eo1/ali_l1g/2012/*/*.sequence | awk '($5 > 1000){print $8}' | sed 's/^.*ali_l1g\/2012\///' | sed 's/.sequence$//' > fileNames.txt

for SMALLCHUNK in `perl -e '$c = 0; foreach (<STDIN>) { $c++; chop; print $_; if ($c % 10 == 0) { print "\n" } else { print " "} }' < fileNames.txt | sed 's/ $//' | sed 's/ /,/g'`; do
    x=$RANDOM;
    echo SMALLCHUNK $SMALLCHUNK x $x;
    /opt/hadoop/bin/hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar \
    	-D org.occ.matsu.restrictBands=$RESTRICT_BANDS \
    	-D org.occ.matsu.restrictBandsTo=$RESTRICT_BANDS_TO \
    	-D mapred.reduce.tasks=0 \
    	-D mapred.min.split.size=107374182400 \
    	-libjars /opt/matsuSequenceFileInterface.jar \
    	-input "/user/hadoop/sequence-files/eo1/ali_l1g/2012/{$SMALLCHUNK}.sequence" \
    	-mapper "\"mapper-L1G-tiles-Accumulo.py bands.json\"" \
    	-inputformat org.occ.matsu.UnsplitableSequenceFileInputFormat \
    	-file mapper-L1G-tiles-Accumulo.py \
    	-file utilities.py \
    	-file modules/flood_detection_R.py \
    	-file modules/trainingSet.txt \
    	-file ../jobconfig.ini \
    	-file /tmp/bands.json \
    	-cmdenv PYTHONPATH=/opt/lib/python \
    	-cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib \
    	-output $OUTPUTDIR-$x
done
