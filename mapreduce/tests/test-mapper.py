#!/usr/bin/env python

import sys

import GeoPictureSerializer

if __name__ == "__main__":
    print "new file"
    for line in sys.stdin.xreadlines():
        band, data = line.rstrip().split("\t")
        geoPicture = GeoPictureSerializer.deserialize(data)
        print band, geoPicture.bands, geoPicture.picture.shape
    print "done"
