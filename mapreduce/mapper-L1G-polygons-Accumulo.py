#!/usr/bin/env python

import sys
import json
import time
import datetime
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import jpype

import GeoPictureSerializer
from utilities import *

################################################################################## input methods

def inputOneLine(inputStream):
    line = inputStream.readline()
    if not line: raise IOError("No input")
    return GeoPictureSerializer.deserialize(line)

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

    geoPicture = GeoPictureSerializer.GeoPicture()
    geoPicture.metadata = metadata
    return geoPicture

################################################################################## make polygon and pass through metadata

class LonLat(object):
    def __init__(self, lon, lat):
        self.lon = float(lon)
        self.lat = float(lat)

    def wrappedUp(self):
        return LonLat(self.lon + 360.0, self.lat)

    def wrappedDown(self):
        return LonLat(self.lon - 360.0, self.lat)

    def __add__(self, other):
        return LonLat(self.lon + other.lon, self.lat + other.lat)

    def __sub__(self, other):
        return LonLat(self.lon - other.lon, self.lat - other.lat)

    def __div__(self, number):
        return LonLat(self.lon/number, self.lat/number)

    @property
    def t(self):
        return (self.lon, self.lat)

def makePolygon(geoPicture, depth):
    product_metadata = json.loads(geoPicture.metadata["L1T"])["PRODUCT_METADATA"]

    points = [LonLat(product_metadata["PRODUCT_UL_CORNER_LON"], product_metadata["PRODUCT_UL_CORNER_LAT"]),
              LonLat(product_metadata["PRODUCT_UR_CORNER_LON"], product_metadata["PRODUCT_UR_CORNER_LAT"]),
              LonLat(product_metadata["PRODUCT_LR_CORNER_LON"], product_metadata["PRODUCT_LR_CORNER_LAT"]),
              LonLat(product_metadata["PRODUCT_LL_CORNER_LON"], product_metadata["PRODUCT_LL_CORNER_LAT"])]
              
    polygon = [points[0].t, points[1].t, points[2].t, points[3].t, points[0].t]

    wrappedPoints = [points[0]]
    for p in points[1:]:
        if p.lon - points[0].lon > 180.0:
            wrappedPoints.append(p.wrappedDown())
        elif p.lon - points[0].lon < -180.0:
            wrappedPoints.append(p.wrappedUp())
        else:
            wrappedPoints.append(p)

    center = (wrappedPoints[0] + wrappedPoints[1] + wrappedPoints[2] + wrappedPoints[3]) / 4.0

    if center > 180.0:
        center = center.wrappedDown()
    elif center < -180.0:
        center = center.wrappedUp()

    depth, longIndex, latIndex = tileIndex(depth, center.lon, center.lat)
    geoPicture.metadata["depth"] = depth
    geoPicture.metadata["longIndex"] = longIndex
    geoPicture.metadata["latIndex"] = latIndex
    geoPicture.metadata["tileName"] = tileName(depth, longIndex, latIndex)
    geoPicture.metadata["geoCenter"] = "lat=%.3f&lng=%.3f&z=10" % (center.lat, center.lon)
    geoPicture.metadata["identifier"] = hash(tuple(polygon)) % (2**64)   # unique unsigned 64-bit value
    geoPicture.metadata["timestamp"] = time.mktime(datetime.datetime.strptime(product_metadata["START_TIME"], "%Y %j %H:%M:%S").timetuple())

    geoPicture.metadata["analytic"] = "polygon-producer"
    geoPicture.metadata["version"] = [0, 8, 0]

    key = "%s-%010d-%s" % (geoPicture.metadata["tileName"], geoPicture.metadata["timestamp"], geoPicture.metadata["identifier"])

    return key, polygon, geoPicture.metadata

################################################################################## entry point

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(["../jobconfig.ini", "jobconfig.ini"])

    useSequenceFiles = (config.get("DEFAULT", "mapper.useSequenceFiles").lower() == "true")
    if useSequenceFiles:
        geoPicture = inputSequenceFile(sys.stdin)
    else:
        geoPicture = inputOneLine(sys.stdin)

    key, polygon, metadata = makePolygon(geoPicture, int(config.get("DEFAULT", "mapper.zoomDepthNarrowest")))

    JAVA_VIRTUAL_MACHINE = config.get("DEFAULT", "lib.jvm")
    ACCUMULO_INTERFACE = config.get("DEFAULT", "accumulo.interface")
    ACCUMULO_DB_NAME = config.get("DEFAULT", "accumulo.db_name")
    ZOOKEEPER_LIST = config.get("DEFAULT", "accumulo.zookeeper_list")
    ACCUMULO_USER_NAME = config.get("DEFAULT", "accumulo.user_name")
    ACCUMULO_PASSWORD = config.get("DEFAULT", "accumulo.password")
    ACCUMULO_TABLE_NAME = config.get("DEFAULT", "accumulo.polygons_table_name")
    try:
        jpype.startJVM(JAVA_VIRTUAL_MACHINE, "-Djava.class.path=%s" % ACCUMULO_INTERFACE)
        AccumuloInterface = jpype.JClass("org.occ.matsu.AccumuloInterface")
        AccumuloInterface.connectForWriting(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    try:
        AccumuloInterface.polygon_write(key, json.dumps(metadata), json.dumps(polygon))
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    print key, polygon, metadata
