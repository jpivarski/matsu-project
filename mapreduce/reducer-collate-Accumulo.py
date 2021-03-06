#!/usr/bin/env python

import sys
import time
import struct
from io import BytesIO
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import jpype
from PIL import Image
import numpy
from scipy.ndimage.interpolation import affine_transform

from utilities import *

################################################################################## collate

def collate(keylist, AccumuloInterface, splineOrder, heartbeat=None):
    parentToChildMap = makeParentChildMap(keylist)
    number = 0
    outOf = len(parentToChildMap)
    for parentKey, childKeys in parentToChildMap.items():
        number += 1
        if heartbeat is not None:
            heartbeat.write("%s %d/%d: Building %s from %s...\n" % (time.strftime("%H:%M:%S"), number, outOf, parentKey, str(childKeys)))

        childImages = []
        for key in childKeys:
            if heartbeat is not None:
                heartbeat.write("%s     loading %s...\n" % (time.strftime("%H:%M:%S"), key))

            try:
                l2pngBytes = AccumuloInterface.readL2png(key)
            except jpype.JavaException as exception:
                raise RuntimeError(exception.stacktrace())

            try:
                metadata = AccumuloInterface.readMetadata(key)
            except jpype.JavaException as exception:
                raise RuntimeError(exception.stacktrace())

            if heartbeat is not None:
                heartbeat.write("%s     converting %s...\n" % (time.strftime("%H:%M:%S"), key))

            childImages.append(numpy.asarray(Image.open(BytesIO(struct.pack("%db" % len(l2pngBytes), *l2pngBytes)))))

        if heartbeat is not None:
            heartbeat.write("%s     shrinking and overlaying %d images...\n" % (time.strftime("%H:%M:%S"), len(childKeys)))

        # these metadata fields would be misleading if applied to parent tiles
        metadata = json.loads(metadata)
        for x in "latIndex", "longIndex", "geoCenter", "tileName", "depth":
            if x in metadata:
                del metadata[x]
        metadata = json.dumps(metadata)

        # rasterYSize, rasterXSize = childImages[0].shape[0:2]
        # outputRed = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
        # outputGreen = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
        # outputBlue = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
        # outputMask = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
            
        # for key, image in zip(childKeys, childImages):
        #     inputRed = image[:,:,0]
        #     inputGreen = image[:,:,1]
        #     inputBlue = image[:,:,2]
        #     inputMask = image[:,:,3]

        #     trans = numpy.matrix([[2., 0.], [0., 2.]])
        #     offset = 0., 0.

        #     inputRed = affine_transform(inputRed, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        #     inputGreen = affine_transform(inputGreen, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        #     inputBlue = affine_transform(inputBlue, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        #     inputMask = affine_transform(inputMask, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)

        #     depth, longIndex, latIndex, layer, timestamp = key.split("-")
        #     depth = int(depth[1:])
        #     longIndex = int(longIndex)
        #     latIndex = int(latIndex)

        #     longOffset, latOffset = tileOffset(depth, longIndex, latIndex)
        #     if longOffset == 0:
        #         longSlice = slice(0, rasterXSize/2)
        #     else:
        #         longSlice = slice(rasterXSize/2, rasterXSize)
        #     if latOffset == 0:
        #         latSlice = slice(rasterYSize/2, rasterYSize)
        #     else:
        #         latSlice = slice(0, rasterYSize/2)

        #     outputRed[latSlice,longSlice] = inputRed[0:rasterYSize/2,0:rasterXSize/2]
        #     outputGreen[latSlice,longSlice] = inputGreen[0:rasterYSize/2,0:rasterXSize/2]
        #     outputBlue[latSlice,longSlice] = inputBlue[0:rasterYSize/2,0:rasterXSize/2]
        #     outputMask[latSlice,longSlice] = inputMask[0:rasterYSize/2,0:rasterXSize/2]

        # parentImage = Image.fromarray(numpy.dstack((outputRed, outputGreen, outputBlue, outputMask)))

        parentImage = Image.fromarray(zoomOutImage(parentKey, childKeys, childImages, splineOrder))
        buff = BytesIO()
        parentImage.save(buff, "PNG", options="optimize")

        if heartbeat is not None:
            heartbeat.write("%s     writing to Accumulo key %s...\n" % (time.strftime("%H:%M:%S"), parentKey))

        try:
            AccumuloInterface.write(parentKey, metadata, buff.getvalue())
        except jpype.JavaException as exception:
            raise RuntimeError(exception.stacktrace())

    if heartbeat is not None:
        heartbeat.write("%s     flushing Accumulo...\n" % time.strftime("%H:%M:%S"))

    try:
        AccumuloInterface.flush()
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())
    
    return parentToChildMap.keys()

################################################################################## entry point

if __name__ == "__main__":
    heartbeat = Heartbeat(stdout=False, stderr=True, reporter=False)
    heartbeat.write("%s Enter reducer-collate-Accumulo.py...\n" % time.strftime("%H:%M:%S"))

    config = configparser.ConfigParser()
    config.read(["../jobconfig.ini", "jobconfig.ini"])

    heartbeat.write("%s Starting the Java Virtual Machine...\n" % time.strftime("%H:%M:%S"))
    JAVA_VIRTUAL_MACHINE = config.get("DEFAULT", "lib.jvm")
    ACCUMULO_INTERFACE = config.get("DEFAULT", "accumulo.interface")
    ACCUMULO_DB_NAME = config.get("DEFAULT", "accumulo.db_name")
    ZOOKEEPER_LIST = config.get("DEFAULT", "accumulo.zookeeper_list")
    ACCUMULO_USER_NAME = config.get("DEFAULT", "accumulo.user_name")
    ACCUMULO_PASSWORD = config.get("DEFAULT", "accumulo.password")
    ACCUMULO_TABLE_NAME = config.get("DEFAULT", "accumulo.table_name")
    try:
        jpype.startJVM(JAVA_VIRTUAL_MACHINE, "-Djava.class.path=%s" % ACCUMULO_INTERFACE)
        AccumuloInterface = jpype.JClass("org.occ.matsu.AccumuloInterface")
        AccumuloInterface.connectForReading(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    zoomDepthNarrowest = int(sys.argv[1])
    zoomDepthWidest = int(sys.argv[2])
    if zoomDepthWidest >= zoomDepthNarrowest:
        raise Exception("mapreduce.zoomDepthWidest must be a smaller number (lower zoom level) than mapreduce.zoomDepthNarrowest")

    # heartbeat.write("%s Extracting all T%02d-xxxxx-yyyyy keys from the database...\n" % (time.strftime("%H:%M:%S"), zoomDepthNarrowest))
    # keys = {}
    # try:
    #     keys[zoomDepthNarrowest] = AccumuloInterface.getKeys("T%02d-" % zoomDepthNarrowest, "T%02d-" % (zoomDepthNarrowest + 1))
    # except jpype.JavaException as exception:
    #     raise RuntimeError(exception.stacktrace())

    keys = {zoomDepthNarrowest: []}
    for line in sys.stdin.xreadlines():
        k, v = line.rstrip().split("\t", 1)
        keys[zoomDepthNarrowest].append(v)

    try:
        AccumuloInterface.connectForWriting(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    heartbeat.write("%s Collating up to T%02d-xxxxx-yyyyy...\n" % (time.strftime("%H:%M:%S"), zoomDepthWidest))
    splineOrder = int(config.get("DEFAULT", "mapper.splineOrder"))
    for depth in xrange(zoomDepthNarrowest, zoomDepthWidest, -1):
        heartbeat.write("%s About to merge %d images for T%02d -> T%02d...\n" % (time.strftime("%H:%M:%S"), len(keys[depth]), depth, depth - 1))
        beforeTime = time.time()
        keys[depth - 1] = collate(keys[depth], AccumuloInterface, splineOrder, heartbeat=heartbeat)
        afterTime = time.time()
        heartbeat.write("%s It took %g minutes to merge %d images.\n" % (time.strftime("%H:%M:%S"), (afterTime - beforeTime)/60., len(keys[depth])))

    heartbeat.write("%s Finished everything; shutting down...\n" % time.strftime("%H:%M:%S"))
    heartbeat.write("%s     shut down Accumulo\n" % time.strftime("%H:%M:%S"))
    try:
        AccumuloInterface.finishedWriting()
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())
