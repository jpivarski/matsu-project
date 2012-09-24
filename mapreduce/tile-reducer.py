#!/usr/bin/env python

import sys
import subprocess
from io import BytesIO
import base64
import json
import math
from math import floor
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import numpy
from PIL import Image
from scipy.ndimage.interpolation import affine_transform
import jpype

import GeoPictureSerializer

def tileIndex(depth, longitude, latitude):
    "Inputs a depth and floating-point longitude and latitude, outputs a triple of index integers."
    if abs(latitude) > 90.: raise ValueError("Latitude cannot be %s" % str(latitude))
    longitude += 180.
    latitude += 90.
    while longitude <= 0.: longitude += 360.
    while longitude > 360.: longitude -= 360.
    longitude = int(floor(longitude/360. * 2**(depth+1)))
    latitude = min(int(floor(latitude/180. * 2**(depth+1))), 2**(depth+1) - 1)
    return depth, longitude, latitude

def tileName(depth, longIndex, latIndex):
    "Inputs an index-triple, outputs a string-valued name for the index."
    return "T%02d-%05d-%05d" % (depth, longIndex, latIndex)  # constant length up to depth 15

def tileCorners(depth, longIndex, latIndex):
    "Inputs an index-triple, outputs the floating-point corners of the tile."
    longmin = longIndex*360./2**(depth+1) - 180.
    longmax = (longIndex + 1)*360./2**(depth+1) - 180.
    latmin = latIndex*180./2**(depth+1) - 90.
    latmax = (latIndex + 1)*180./2**(depth+1) - 90.
    return longmin, longmax, latmin, latmax

def tileParent(depth, longIndex, latIndex):
    "Returns the (depth-1, longIndex, latIndex) that contains this tile."
    return depth - 1, longIndex // 2, latIndex // 2

def tileOffset(depth, longIndex, latIndex):
    "Returns the corner this tile occupies in its parent's frame."
    return longIndex % 2, latIndex % 2

def reduce_tiles(tiles, inputStream, outputDirectory=None, outputAccumulo=None, mergeTimestamps=False, configuration=[{"layer": "RGB", "bands": ["B029", "B023", "B016"], "outputType": "RGB", "minRadiance": 0., "maxRadiance": "sun"}]):
    """Performs the part of the reducing step of the Hadoop map-reduce job that depends on key-value input.

    Reduce key: tile coordinate and timestamp
    Reduce value: transformed picture
    Actions: overlay images in time-order to produce one image per tile coordinate.

        * tiles: dictionary of tiles built up by this procedure (this function adds to tiles)
        * inputStream: usually sys.stdin; should be key-value pairs.
        * outputDirectory: local filesystem directory for debugging output (usually the output goes to Accumulo)
        * outputAccumulo: Java virtual machine containing AccumuloInterface classes
        * mergeTimestamps: if False, timestamp is part of the key and images from different times are separate;
                           if True, images of the same place from different times are overlaid into a single image
        * configuration: list of layer configurations with the following format
                         {"layer": layerName, "bands", [band1, band2, band3], "outputType": "RGB", "yellow", etc.,
                          "minRadiance": number or string percentage, "maxRadiance": number or string percentage}

    If configuration is incorrectly formatted, the behavior is undefined.
    """

    while True:
        line = inputStream.readline()
        if not line: break

        for config in configuration:
            layer = config["layer"]
            bands = config["bands"]
            outputType = config["outputType"]
            minRadiance = config["minRadiance"]
            maxRadiance = config["maxRadiance"]

            if isinstance(minRadiance, basestring) and minRadiance[-1] == "%":
                try:
                    minPercent = float(minRadiance[:-1])
                except ValueError:
                    minPercent = 5.
                minRadiance = None

            if isinstance(maxRadiance, basestring) and maxRadiance[-1] == "%":
                try:
                    maxPercent = float(maxRadiance[:-1])
                except ValueError:
                    maxPercent = 95.
                maxRadiance = None

            tabPosition = line.index("\t")
            key = line[:tabPosition]
            value = line[tabPosition+1:-1]

            depth, longIndex, latIndex, timestamp = map(int, key.lstrip("T").split("-"))

            try:
                geoPicture = GeoPictureSerializer.deserialize(value)
            except IOError:
                continue

            if mergeTimestamps:
                outputKey = depth, longIndex, latIndex, layer
            else:
                outputKey = depth, longIndex, latIndex, layer, timestamp

            if outputKey not in tiles:
                shape = geoPicture.picture.shape[:2]
                outputRed = numpy.zeros(shape, dtype=numpy.uint8)
                outputGreen = numpy.zeros(shape, dtype=numpy.uint8)
                outputBlue = numpy.zeros(shape, dtype=numpy.uint8)
                outputMask = numpy.zeros(shape, dtype=numpy.uint8)
                tiles[outputKey] = (outputRed, outputGreen, outputBlue, outputMask)
            outputRed, outputGreen, outputBlue, outputMask = tiles[outputKey]

            if outputType == "RGB":
                red = geoPicture.picture[:,:,geoPicture.bands.index(bands[0])]
                green = geoPicture.picture[:,:,geoPicture.bands.index(bands[1])]
                blue = geoPicture.picture[:,:,geoPicture.bands.index(bands[2])]

                if maxRadiance == "sun":
                    l1t = json.loads(geoPicture.metadata["L1T"])
                    sunAngle = math.sin(float(l1t["PRODUCT_PARAMETERS"]["SUN_ELEVATION"]) * math.pi/180.)
                    maxRadiance = sunAngle * 500.
                    if maxRadiance < 10.:
                        maxRadiance = 10.

                if minRadiance is None:
                    for b in red, green, blue:
                        bb = b[b > 10.]
                        if len(bb) > 0:
                            r = numpy.percentile(bb, minPercent)
                            if minRadiance is None or r < minRadiance:
                                minRadiance = r

                    if minRadiance is None:
                        for b in red, green, blue:
                            r = numpy.percentile(b, minPercent)
                            if minRadiance is None or r < minRadiance:
                                minRadiance = r

                if maxRadiance is None:
                    for b in red, green, blue:
                        bb = b[b > 10.]
                        if len(bb) > 0:
                            r = numpy.percentile(bb, maxPercent)
                            if maxRadiance is None or r > maxRadiance:
                                maxRadiance = r

                    if maxRadiance is None:
                        for b in red, green, blue:
                            r = numpy.percentile(b, maxPercent)
                            if maxRadiance is None or r > maxRadiance:
                                maxRadiance = r

                if maxRadiance == minRadiance:
                    minRadiance = min(red.min(), green.min(), blue.min())
                    maxRadiance = max(red.max(), green.max(), blue.max())
                if maxRadiance == minRadiance:
                    minRadiance, maxRadiance = 0., 1.

                red = numpy.minimum(numpy.maximum((red - minRadiance) / (maxRadiance - minRadiance) * 255, 0), 255)
                green = numpy.minimum(numpy.maximum((green - minRadiance) / (maxRadiance - minRadiance) * 255, 0), 255)
                blue = numpy.minimum(numpy.maximum((blue - minRadiance) / (maxRadiance - minRadiance) * 255, 0), 255)
                mask = numpy.minimum(numpy.maximum(geoPicture.picture[:,:,geoPicture.bands.index("MASK")] * 255, 0), 255)

                condition = (mask > 0.5)
                outputRed[condition] = red[condition]
                outputGreen[condition] = green[condition]
                outputBlue[condition] = blue[condition]
                outputMask[condition] = mask[condition]

            else:
                b = geoPicture.picture[:,:,geoPicture.bands.index(bands[0])]

                if minRadiance is None:
                    bb = b[b > 10.]
                    if len(bb) > 0:
                        minRadiance = numpy.percentile(bb, minPercent)
                    else:
                        minRadiance = numpy.percentile(b, minPercent)

                if maxRadiance is None:
                    bb = b[b > 10.]
                    if len(bb) > 0:
                        maxRadiance = numpy.percentile(bb, maxPercent)
                    else:
                        maxRadiance = numpy.percentile(b, maxPercent)

                if maxRadiance == minRadiance:
                    minRadiance = b.min()
                    maxRadiance = b.max()
                if maxRadiance == minRadiance:
                    minRadiance, maxRadiance = 0., 1.

                b = numpy.minimum(numpy.maximum((b - minRadiance) / (maxRadiance - minRadiance) * 255, 0), 255)

                mask = numpy.minimum(numpy.maximum(geoPicture.picture[:,:,geoPicture.bands.index("MASK")] * 255, 0), 255)
                condition = (mask > 0.5)

                if outputType == "yellow":
                    outputRed[condition] = b[condition]
                    outputGreen[condition] = b[condition]
                    outputMask[condition] = b[condition]

                else:
                    raise NotImplementedError

            if mergeTimestamps:
                outputKey = "%s-%s" % (tileName(depth, longIndex, latIndex), layer)
            else:
                outputKey = "%s-%s-%d" % (tileName(depth, longIndex, latIndex), layer, timestamp)

            image = Image.fromarray(numpy.dstack((outputRed, outputGreen, outputBlue, outputMask)))
            if outputDirectory is not None:
                image.save("%s/%s.png" % (outputDirectory, outputKey), "PNG", options="optimize")
            if outputAccumulo is not None:
                buff = BytesIO()
                image.save(buff, "PNG", options="optimize")
                outputAccumulo.write(outputKey, "{}", buff.getvalue())

def collate(depth, tiles, outputDirectory=None, outputAccumulo=None, layer="RGB", splineOrder=3):
    """Performs the part of the reducing step of the Hadoop map-reduce job after all data have been collected.

    Actions: combine deep images to produce more shallow images.

        * depth: depth of input images; output images have (depth - 1)
        * tiles: dictionary of tiles built up by this procedure (this function adds to tiles)
        * outputDirectory: local filesystem directory for debugging output (usually the output goes to Accumulo)
        * outputAccumulo: Java virtual machine containing AccumuloInterface classes
        * layer: name of the layer
        * splineOrder: order of the spline used to calculate the affine_transformation (see SciPy docs); must be between 0 and 5
    """
    
    for key in tiles.keys():
        if len(key) == 4:
            mergeTimestamps = True
            depthIndex, longIndex, latIndex, l = key
        elif len(key) == 5:
            mergeTimestamps = False
            depthIndex, longIndex, latIndex, l, timestamp = key
        else: raise Exception("Somehow the key structure broke mid-processing...")

        if l == layer and depthIndex == depth:
            parentDepth, parentLongIndex, parentLatIndex = tileParent(depthIndex, longIndex, latIndex)

            if mergeTimestamps:
                outputKey = parentDepth, parentLongIndex, parentLatIndex, layer
            else:
                outputKey = parentDepth, parentLongIndex, parentLatIndex, layer, timestamp

            if outputKey not in tiles:
                shape = tiles[key][0].shape
                outputRed = numpy.zeros(shape, dtype=numpy.uint8)
                outputGreen = numpy.zeros(shape, dtype=numpy.uint8)
                outputBlue = numpy.zeros(shape, dtype=numpy.uint8)
                outputMask = numpy.zeros(shape, dtype=numpy.uint8)
                tiles[outputKey] = outputRed, outputGreen, outputBlue, outputMask

            outputRed, outputGreen, outputBlue, outputMask = tiles[outputKey]
            rasterYSize, rasterXSize = outputRed.shape

            inputRed, inputGreen, inputBlue, inputMask = tiles[key]

            trans = numpy.matrix([[2., 0.], [0., 2.]])
            offset = 0., 0.

            affine_transform(inputRed, trans, offset, (rasterYSize, rasterXSize), inputRed, splineOrder)
            affine_transform(inputGreen, trans, offset, (rasterYSize, rasterXSize), inputGreen, splineOrder)
            affine_transform(inputBlue, trans, offset, (rasterYSize, rasterXSize), inputBlue, splineOrder)
            affine_transform(inputMask, trans, offset, (rasterYSize, rasterXSize), inputMask, splineOrder)

            longOffset, latOffset = tileOffset(depthIndex, longIndex, latIndex)
            if longOffset == 0:
                longSlice = slice(0, rasterXSize/2)
            else:
                longSlice = slice(rasterXSize/2, rasterXSize)
            if latOffset == 0:
                latSlice = slice(rasterYSize/2, rasterYSize)
            else:
                latSlice = slice(0, rasterYSize/2)

            outputRed[latSlice,longSlice] = inputRed[0:rasterYSize/2,0:rasterXSize/2]
            outputGreen[latSlice,longSlice] = inputGreen[0:rasterYSize/2,0:rasterXSize/2]
            outputBlue[latSlice,longSlice] = inputBlue[0:rasterYSize/2,0:rasterXSize/2]
            outputMask[latSlice,longSlice] = inputMask[0:rasterYSize/2,0:rasterXSize/2]

            if mergeTimestamps:
                outputKey = "%s-%s" % (tileName(parentDepth, parentLongIndex, parentLatIndex), layer)
            else:
                outputKey = "%s-%s-%d" % (tileName(parentDepth, parentLongIndex, parentLatIndex), layer, timestamp)

            image = Image.fromarray(numpy.dstack((outputRed, outputGreen, outputBlue, outputMask)))
            if outputDirectory is not None:
                image.save("%s/%s.png" % (outputDirectory, outputKey), "PNG", options="optimize")
            if outputAccumulo is not None:
                buff = BytesIO()
                image.save(buff, "PNG", options="optimize")
                outputAccumulo.write(outputKey, "{}", buff.getvalue())

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(["../CONFIG.ini", "CONFIG.ini"])

    JAVA_VIRTUAL_MACHINE = config.get("DEFAULT", "lib.jvm")

    ACCUMULO_INTERFACE = config.get("DEFAULT", "accumulo.interface")
    ACCUMULO_DB_NAME = config.get("DEFAULT", "accumulo.db_name")
    ZOOKEEPER_LIST = config.get("DEFAULT", "accumulo.zookeeper_list")
    ACCUMULO_USER_NAME = config.get("DEFAULT", "accumulo.user_name")
    ACCUMULO_PASSWORD = config.get("DEFAULT", "accumulo.password")
    ACCUMULO_TABLE_NAME = config.get("DEFAULT", "accumulo.table_name")

    jpype.startJVM(JAVA_VIRTUAL_MACHINE, "-Djava.class.path=%s" % ACCUMULO_INTERFACE)
    AccumuloInterface = jpype.JClass("org.occ.matsu.AccumuloInterface")

    AccumuloInterface.connectForWriting(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)

    configuration = json.loads(sys.argv[1])
    layers = [c["layer"] for c in configuration]

    tiles = {}
    reduce_tiles(tiles, sys.stdin, outputAccumulo=AccumuloInterface, mergeTimestamps=False, configuration=configuration)

    for depth in xrange(10, 1, -1):
        for layer in layers:
            collate(depth, tiles, outputAccumulo=AccumuloInterface, layer=layer)

    AccumuloInterface.finishedWriting()
