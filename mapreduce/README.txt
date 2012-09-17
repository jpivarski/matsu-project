Running a "map-reduce" job on the command-line, with CO2:

cat /tmp/input.serialized | python tile-mapper.py modules/co2-detection.py | sort | python tile-reducer.py "[{\"layer\": \"RGB\", \"bands\": [\"B029\", \"B023\", \"B016\"], \"outputType\": \"RGB\", \"minRadiance\": 0.0, \"maxRadiance\": \"sun\"}, {\"layer\": \"CO2\", \"bands\": [\"CO2\"], \"outputType\": \"yellow\", \"minRadiance\": 0.0, \"maxRadiance\": 80.0}]"

Arguments to tile-mapper.py: list of module paths

Arguments to tile-reducer.py: JSON-formatted configuration:
    [{"layer": layerName, "bands", [band1, band2, band3], "outputType": "RGB", "yellow", etc.,
      "minRadiance": number or string percentage, "maxRadiance": number or string percentage},
     {another layer...},
     ...
    ]

Running a real map-reduce job from the commandline, with CO2:

hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -mapper 'tile-mapper.py co2-detection.py' -reducer 'tile-reducer.py "[{\"layer\": \"RGB\", \"bands\": [\"B029\", \"B023\", \"B016\"], \"outputType\": \"RGB\", \"minRadiance\": 0.0, \"maxRadiance\": \"sun\"}, {\"layer\": \"CO2\", \"bands\": [\"CO2\"], \"outputType\": \"yellow\", \"minRadiance\": 0.0, \"maxRadiance\": 80.0}]"' -input IcelandicVolcano-erupting.serialized -output output-02 -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -cmdenv PYTHONPATH=/opt/lib/python -file tile-mapper.py -file tile-reducer.py -file modules/co2-detection.py -file ../CONFIG.ini
