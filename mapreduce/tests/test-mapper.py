#!/usr/bin/env python

import sys
import json

import numpy

import GeoPictureSerializer

ONLY_LOAD_BANDS = set(["B016", "B029"])

if __name__ == "__main__":
    # enforce a structure on SequenceFile entries to be sure that Hadoop isn't splitting it up among multiple mappers
    name, metadata = sys.stdin.readline().rstrip().split("\t")
    if name != "metadata":
        raise IOError("First entry in the SequenceFile is \"%s\" rather than metadata" % name)
    metadata = json.loads(metadata)

    name, bands = sys.stdin.readline().rstrip().split("\t")
    if name != "bands":
        raise IOError("Second entry in the SequenceFile is \"%s\" rather than bands" % name)
    bands = json.loads(bands)

    name, shape = sys.stdin.readline().rstrip().split("\t")
    if name != "shape":
        raise IOError("Third entry in the SequenceFile is \"%s\" rather than shape" % name)
    shape = json.loads(shape)

    # drop undesired bands
    onlyload = sorted(ONLY_LOAD_BANDS.intersection(bands))
    shape = (shape[0], shape[1], len(onlyload))

    # make a master image to fill
    geoPicture = GeoPictureSerializer.GeoPicture()
    geoPicture.metadata = metadata
    geoPicture.bands = onlyload
    geoPicture.picture = numpy.empty(shape, dtype=numpy.float)

    # load individual bands from the SequenceFile and add them to the master image, if desired
    bandsSeen = []
    for line in sys.stdin.xreadlines():
        band, data = line.rstrip().split("\t")
        bandsSeen.append(band)

        if band not in bands:
            raise IOError("SequenceFile contains \"%s\" when it should only have %s bands" % (band, str(bands)))
        
        if band in onlyload:
            sys.stderr.write("There should be something here: %s\n" % data[:10])

            index = onlyload.index(band)
            oneBandPicture = GeoPictureSerializer.deserialize(data)

            if oneBandPicture.picture.shape[0:2] != geoPicture.picture.shape[0:2]:
                raise IOError("SequenceFile band \"%s\" has shape %s instead of %d by %d by 1" % (band, oneBandPicture.picture.shape, shape[0], shape[1]))

            geoPicture.picture[:,:,index] = oneBandPicture.picture[:,:,0]

        else:
            sys.stderr.write("SHOULD BE EMPTY: %s\n" % data[:10])

        if len(bandsSeen) == len(bands):
            break

    for band in bands:
        if band not in bandsSeen:
            raise IOError("SequenceFile does not contain \"%s\" when it should have %s" % (band, str(bands)))

    print geoPicture.picture
