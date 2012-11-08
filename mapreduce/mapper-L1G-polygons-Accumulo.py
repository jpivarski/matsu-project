#!/usr/bin/env python

import sys
import json

import GeoPictureSerializer

################################################################################## input methods

def inputOneLine(inputStream):
    line = inputStream.readline()
    if not line: raise IOError("No input")
    return GeoPictureSerializer.deserialize(line).metadata

def inputSequenceFile(inputStream):
    # enforce a structure on SequenceFile entries to be sure that Hadoop isn't splitting it up among multiple mappers
    name, metadata = sys.stdin.readline().rstrip().split("\t")
    if name != "metadata":
        raise IOError("First entry in the SequenceFile is \"%s\" rather than metadata" % name)
    metadata = json.loads(metadata)

    metadata_noUnicode = {}
    for key, value in metadata.items():
        metadata_noUnicode[str(key)] = str(value)
    metadata = metadata_noUnicode

    # read to the end of the input stream without saving what it contains (just to avoid broken pipe error)
    chunk = "."
    while chunk:
        chunk = sys.stdin.read(4096)

    return metadata

################################################################################## make polygon and pass through metadata

def makePolygon(geoPicture):
    product_metadata = json.loads(geoPicture.metadata["L1T"])["PRODUCT_METADATA"]

    polygon = [(float(product_metadata["PRODUCT_UL_CORNER_LON"]), float(product_metadata["PRODUCT_UL_CORNER_LAT"])),
               (float(product_metadata["PRODUCT_UR_CORNER_LON"]), float(product_metadata["PRODUCT_UR_CORNER_LAT"])),
               (float(product_metadata["PRODUCT_LR_CORNER_LON"]), float(product_metadata["PRODUCT_LR_CORNER_LAT"])),
               (float(product_metadata["PRODUCT_LL_CORNER_LON"]), float(product_metadata["PRODUCT_LL_CORNER_LAT"])),
               (float(product_metadata["PRODUCT_UL_CORNER_LON"]), float(product_metadata["PRODUCT_UL_CORNER_LAT"]))]

    






################################################################################## entry point

if __name__ == "__main__":
