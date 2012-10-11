#!/usr/bin/env python

import sys
import time
import datetime
import json
import math
import base64
from io import BytesIO
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import numpy
from PIL import Image
from scipy.ndimage.interpolation import affine_transform
from osgeo import osr
import jpype

import GeoPictureSerializer
from utilities import *

################################################################################## input methods

def inputOneLine(inputStream):
    line = inputStream.readline()
    if not line: raise IOError("No input")
    return GeoPictureSerializer.deserialize(line)

def inputSequenceFile(inputStream, incomingBandRestriction):
    # enforce a structure on SequenceFile entries to be sure that Hadoop isn't splitting it up among multiple mappers
    name, metadata = sys.stdin.readline().rstrip().split("\t")
    if name != "metadata":
        raise IOError("First entry in the SequenceFile is \"%s\" rather than metadata" % name)
    metadata = json.loads(metadata)

    metadata_noUnicode = {}
    for key, value in metadata.items():
        metadata_noUnicode[str(key)] = str(value)
    metadata = metadata_noUnicode

    name, bands = sys.stdin.readline().rstrip().split("\t")
    if name != "bands":
        raise IOError("Second entry in the SequenceFile is \"%s\" rather than bands" % name)
    bands = json.loads(bands)

    bands_noUnicode = [str(b) for b in bands]
    bands = bands_noUnicode

    name, shape = sys.stdin.readline().rstrip().split("\t")
    if name != "shape":
        raise IOError("Third entry in the SequenceFile is \"%s\" rather than shape" % name)
    shape = json.loads(shape)

    if incomingBandRestriction is not None:
        # drop undesired bands
        onlyload = sorted(incomingBandRestriction.intersection(bands))
        shape = (shape[0], shape[1], len(onlyload))
    else:
        onlyload = bands
        shape = tuple(shape)

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
            index = onlyload.index(band)
            oneBandPicture = GeoPictureSerializer.deserialize(data)

            if oneBandPicture.picture.shape[0:2] != geoPicture.picture.shape[0:2]:
                raise IOError("SequenceFile band \"%s\" has shape %s instead of %d by %d by 1" % (band, oneBandPicture.picture.shape, shape[0], shape[1]))

            geoPicture.picture[:,:,index] = oneBandPicture.picture[:,:,0]

        if len(bandsSeen) == len(bands):
            break

    for band in bands:
        if band not in bandsSeen:
            raise IOError("SequenceFile does not contain \"%s\" when it should have %s" % (band, str(bands)))

    return geoPicture

################################################################################## apply modules, add bands, remove bands

def loadModules(modules):
    loadedModules = []
    if modules is not None:
        for module in modules:
            globalVars = {}
            exec(compile(open(module).read(), module, "exec"), globalVars)
            loadedModules.append(globalVars["newBand"])
    return loadedModules

def removeBands(geoPicture, outgoingBandRestriction):
    if outgoingBandRestriction is None:
        return geoPicture

    outputBands = []
    for band in geoPicture.bands:
        if band in outgoingBandRestriction:
            outputBands.append(band)
    
    output = numpy.empty((geoPicture.picture.shape[0], geoPicture.picture.shape[1], len(outputBands)), dtype=numpy.float)
    for band in outputBands:
        output[:,:,outputBands.index(band)] = geoPicture.picture[:,:,geoPicture.bands.index(band)]

    geoPictureOutput = GeoPictureSerializer.GeoPicture()
    geoPictureOutput.metadata = geoPicture.metadata
    geoPictureOutput.bands = outputBands
    geoPictureOutput.picture = output
    return geoPictureOutput

################################################################################## tiling procedure

def makeTiles(geoPicture, inputBands, depth, longpixels, latpixels, numLatitudeSections, splineOrder, heartbeat=None):
    outputGeoPictures = []

    # convert GeoTIFF coordinates into degrees
    tlx, weres, werot, tly, nsrot, nsres = json.loads(geoPicture.metadata["GeoTransform"])
    spatialReference = osr.SpatialReference()
    spatialReference.ImportFromWkt(geoPicture.metadata["Projection"])
    coordinateTransform = osr.CoordinateTransformation(spatialReference, spatialReference.CloneGeogCS())
    rasterXSize = geoPicture.picture.shape[1]
    rasterYSize = geoPicture.picture.shape[0]
    rasterDepth = geoPicture.picture.shape[2]

    for section in xrange(numLatitudeSections):
        bottom = (section + 0.0)/numLatitudeSections
        middle = (section + 0.5)/numLatitudeSections
        thetop = (section + 1.0)/numLatitudeSections

        # find the corners to determine which tile(s) this section belongs in
        corner1Long, corner1Lat, altitude = coordinateTransform.TransformPoint(tlx + 0.0*weres*rasterXSize, tly + bottom*nsres*rasterYSize)
        corner2Long, corner2Lat, altitude = coordinateTransform.TransformPoint(tlx + 0.0*weres*rasterXSize, tly + thetop*nsres*rasterYSize)
        corner3Long, corner3Lat, altitude = coordinateTransform.TransformPoint(tlx + 1.0*weres*rasterXSize, tly + bottom*nsres*rasterYSize)
        corner4Long, corner4Lat, altitude = coordinateTransform.TransformPoint(tlx + 1.0*weres*rasterXSize, tly + thetop*nsres*rasterYSize)

        longIndexes = []
        latIndexes = []
        for ti in tileIndex(depth, corner1Long, corner1Lat), tileIndex(depth, corner2Long, corner2Lat), tileIndex(depth, corner3Long, corner3Lat), tileIndex(depth, corner4Long, corner4Lat):
            longIndexes.append(ti[1])
            latIndexes.append(ti[2])

        for ti in [(depth, x, y) for x in xrange(min(longIndexes), max(longIndexes)+1) for y in xrange(min(latIndexes), max(latIndexes)+1)]:
            longmin, longmax, latmin, latmax = tileCorners(*ti)

            # find the origin and orientation of the image (not always exactly north-south-east-west)
            cornerLong, cornerLat, altitude   = coordinateTransform.TransformPoint(tlx, tly)
            originLong, originLat, altitude   = coordinateTransform.TransformPoint(tlx + 0.5*weres*rasterXSize, tly + middle*nsres*rasterYSize)
            leftLong, leftLat, altitude       = coordinateTransform.TransformPoint(tlx + 0.0*weres*rasterXSize, tly + middle*nsres*rasterYSize)
            rightLong, rightLat, altitude     = coordinateTransform.TransformPoint(tlx + 1.0*weres*rasterXSize, tly + middle*nsres*rasterYSize)
            upLong, upLat, altitude           = coordinateTransform.TransformPoint(tlx + 0.5*weres*rasterXSize, tly + bottom*nsres*rasterYSize)
            downLong, downLat, altitude       = coordinateTransform.TransformPoint(tlx + 0.5*weres*rasterXSize, tly + thetop*nsres*rasterYSize)

            if heartbeat is not None:
                heartbeat.write("%s About to make tile %s, centered on lat=%.3f&lng=%.3f&z=10...\n" % (time.strftime("%H:%M:%S"), tileName(*ti), originLat, originLong))

            # do some linear algebra to convert coordinates
            L2PNG_to_geo_trans = numpy.matrix([[(latmin - latmax)/float(latpixels), 0.], [0., (longmax - longmin)/float(longpixels)]])

            L1TIFF_to_geo_trans = numpy.matrix([[(downLat - upLat)/((thetop - bottom)*rasterYSize), (rightLat - leftLat)/rasterXSize], [(downLong - upLong)/((thetop - bottom)*rasterYSize), (rightLong - leftLong)/rasterXSize]])
            geo_to_L1TIFF_trans = L1TIFF_to_geo_trans.I

            trans = geo_to_L1TIFF_trans * L2PNG_to_geo_trans

            offset_in_deg = numpy.matrix([[latmax - cornerLat], [longmin - cornerLong]], dtype=numpy.double)

            # correct for the bottom != 0. case (only if section > 0)
            truncate_correction = L1TIFF_to_geo_trans * numpy.matrix([[int(math.floor(bottom*rasterYSize))], [0.]], dtype=numpy.double)

            # correct for the curvature of the Earth between the top of the section and the bottom of the section (that's why we cut into latitude sections)
            curvature_correction = L1TIFF_to_geo_trans * (geo_to_L1TIFF_trans * numpy.matrix([[leftLat - cornerLat], [leftLong - cornerLong]], dtype=numpy.double) - numpy.matrix([[(middle*rasterYSize)], [0.]], dtype=numpy.double))

            offset = L1TIFF_to_geo_trans.I * (offset_in_deg - truncate_correction - curvature_correction)

            offset = offset[0,0], offset[1,0]

            # lay the GeoTIFF into the output image array
            inputPicture = geoPicture.picture[int(math.floor(bottom*rasterYSize)):int(math.ceil(thetop*rasterYSize)),:,:]
            inputMask = None
            for band in set(inputBands).intersection(geoPicture.bands):
                if inputMask is None:
                    inputMask = (inputPicture[:,:,geoPicture.bands.index(band)] > 0.)
                else:
                    numpy.logical_and(inputMask, (inputPicture[:,:,geoPicture.bands.index(band)] > 0.), inputMask)

            outputMask = numpy.zeros((latpixels, longpixels), dtype=geoPicture.picture.dtype)
            affine_transform(inputMask, trans, offset, (latpixels, longpixels), outputMask, splineOrder)
            if numpy.count_nonzero(outputMask > 0.5) == 0: continue

            offset = offset[0], offset[1], 0.
            trans = numpy.matrix([[trans[0,0], trans[0,1], 0.], [trans[1,0], trans[1,1], 0.], [0., 0., 1.]])

            outputPicture = numpy.zeros((latpixels, longpixels, rasterDepth), dtype=geoPicture.picture.dtype)
            affine_transform(inputPicture, trans, offset, (latpixels, longpixels, rasterDepth), outputPicture, splineOrder)

            # suppress regions that should be zero but might not be because of numerical error in affine_transform
            # this will make more of the picture eligible for zero-suppression (which checks for pixels exactly equal to zero)
            cutMask = (outputMask < 0.01)
            outputBands = []
            for i in xrange(rasterDepth):
                outputBands.append(outputPicture[:,:,i])
                outputBands[-1][cutMask] = 0.
            outputBands.append(outputMask)
            outputBands[-1][cutMask] = 0.

            outputGeoPicture = GeoPictureSerializer.GeoPicture()
            outputGeoPicture.picture = numpy.dstack(outputBands)
            outputGeoPicture.metadata = dict(geoPicture.metadata)
            outputGeoPicture.bands = geoPicture.bands + ["MASK"]

            outputGeoPicture.metadata.update({"depth": depth, "longIndex": x, "latIndex": y, "tileName": tileName(*ti), "geoCenter": "lat=%.3f&lng=%.3f&z=10" % (originLat, originLong)})

            outputGeoPictures.append(outputGeoPicture)

    return outputGeoPictures

################################################################################## create images

def radianceRange(minRadiance, maxRadiance, geoPicture, bandArrays):
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

    if maxRadiance == "sun":
        l1t = json.loads(geoPicture.metadata["L1T"])
        sunAngle = math.sin(float(l1t["PRODUCT_PARAMETERS"]["SUN_ELEVATION"]) * math.pi/180.)
        maxRadiance = sunAngle * 400.
        if maxRadiance < 10.:
            maxRadiance = 10.

    if minRadiance is None:
        for b in bandArrays:
            bb = b[b > 10.]
            if len(bb) > 0:
                r = numpy.percentile(bb, minPercent)
                if minRadiance is None or r < minRadiance:
                    minRadiance = r

        if minRadiance is None:
            for b in bandArrays:
                r = numpy.percentile(b, minPercent)
                if minRadiance is None or r < minRadiance:
                    minRadiance = r

    if maxRadiance is None:
        for b in bandArrays:
            bb = b[b > 10.]
            if len(bb) > 0:
                r = numpy.percentile(bb, maxPercent)
                if maxRadiance is None or r > maxRadiance:
                    maxRadiance = r

        if maxRadiance is None:
            for b in bandArrays:
                r = numpy.percentile(b, maxPercent)
                if maxRadiance is None or r > maxRadiance:
                    maxRadiance = r

    if maxRadiance == minRadiance:
        minRadiance = min([bbb.min() for bbb in bandArrays])
        maxRadiance = max([bbb.max() for bbb in bandArrays])

    if maxRadiance == minRadiance:
        minRadiance, maxRadiance = 0., 1.

    return minRadiance, maxRadiance

def makeImage(geoPicture, layer, bands, outputType, minRadiance, maxRadiance):
    bandsAvailable = True
    for band in bands:
        if band not in geoPicture.bands:
            bandsAvailable = False
    if not bandsAvailable:
        return None

    shape = geoPicture.picture.shape[:2]
    outputRed = numpy.zeros(shape, dtype=numpy.uint8)
    outputGreen = numpy.zeros(shape, dtype=numpy.uint8)
    outputBlue = numpy.zeros(shape, dtype=numpy.uint8)
    outputMask = numpy.zeros(shape, dtype=numpy.uint8)

    if outputType == "RGB":
        red = geoPicture.picture[:,:,geoPicture.bands.index(bands[0])]
        green = geoPicture.picture[:,:,geoPicture.bands.index(bands[1])]
        blue = geoPicture.picture[:,:,geoPicture.bands.index(bands[2])]

        minRadiance, maxRadiance = radianceRange(minRadiance, maxRadiance, geoPicture, [red, green, blue])

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

        minRadiance, maxRadiance = radianceRange(minRadiance, maxRadiance, geoPicture, [b])

        b = numpy.minimum(numpy.maximum((b - minRadiance) / (maxRadiance - minRadiance) * 255, 0), 255)

        mask = numpy.minimum(numpy.maximum(geoPicture.picture[:,:,geoPicture.bands.index("MASK")] * 255, 0), 255)
        condition = (mask > 0.5)

        if outputType == "yellow":
            outputRed[condition] = b[condition]
            outputGreen[condition] = b[condition]
            outputMask[condition] = b[condition]

        else:
            raise NotImplementedError

    return Image.fromarray(numpy.dstack((outputRed, outputGreen, outputBlue, outputMask)))

################################################################################## entry point

if __name__ == "__main__":
    heartbeat = Heartbeat(stdout=False, stderr=True, reporter=True)
    heartbeat.write("%s Enter mapper-L1G-tiles-Accumulo.py...\n" % time.strftime("%H:%M:%S"))

    osr.UseExceptions()

    config = configparser.ConfigParser()
    config.read(["../CONFIG.ini", "CONFIG.ini"])

    modules = json.loads(config.get("DEFAULT", "mapper.modules"))
    if modules == []: modules = None

    heartbeat.write("%s About to load modules...\n" % time.strftime("%H:%M:%S"))
    loadedModules = loadModules(modules)

    useSequenceFiles = (config.get("DEFAULT", "mapper.useSequenceFiles").lower() == "true")
    if useSequenceFiles:
        heartbeat.write("%s About to load a SequenceFile...\n" % time.strftime("%H:%M:%S"))
        incomingBandRestriction = json.loads(config.get("DEFAULT", "mapper.incomingBandRestriction"))
        if incomingBandRestriction is not None:
            incomingBandRestriction = set(incomingBandRestriction)
        geoPicture = inputSequenceFile(sys.stdin, incomingBandRestriction)
    else:
        heartbeat.write("%s About to load a one-line serialized file...\n" % time.strftime("%H:%M:%S"))
        geoPicture = inputOneLine(sys.stdin)

    geoPicture.metadata["timestamp"] = time.mktime(datetime.datetime.strptime(json.loads(geoPicture.metadata["L1T"])["PRODUCT_METADATA"]["START_TIME"], "%Y %j %H:%M:%S").timetuple())
    geoPicture.metadata["analytic"] = "tile-producer"
    geoPicture.metadata["version"] = [1, 1, 0]

    inputBands = geoPicture.bands[:]   # no virtual bands

    for i, newBand in enumerate(loadedModules):
        heartbeat.write("%s About to run module %d...\n" % (time.strftime("%H:%M:%S"), i))
        geoPicture = newBand(geoPicture)

    outgoingBandRestriction = json.loads(config.get("DEFAULT", "mapper.outgoingBandRestriction"))
    if outgoingBandRestriction is not None:
        outgoingBandRestriction = set(outgoingBandRestriction)

    heartbeat.write("%s About to remove unnecessary bands...\n" % time.strftime("%H:%M:%S"))
    geoPicture = removeBands(geoPicture, outgoingBandRestriction)

    depth = int(config.get("DEFAULT", "mapreduce.zoomDepthNarrowest"))
    longpixels = int(config.get("DEFAULT", "mapper.tileLongitudePixels"))
    latpixels = int(config.get("DEFAULT", "mapper.tileLatitudePixels"))
    numLatitudeSections = int(config.get("DEFAULT", "mapper.numberOfLatitudeSections"))
    splineOrder = int(config.get("DEFAULT", "mapper.splineOrder"))

    heartbeat.write("%s About to make tiles...\n" % time.strftime("%H:%M:%S"))
    geoPictureTiles = makeTiles(geoPicture, inputBands, depth, longpixels, latpixels, numLatitudeSections, splineOrder, heartbeat=heartbeat)

    configuration = json.loads(config.get("DEFAULT", "reducer.configuration"))
    outputToAccumulo = (config.get("DEFAULT", "reducer.outputToAccumulo").lower() == "true")
    outputToLocalDirectory = (config.get("DEFAULT", "reducer.outputToLocalDirectory").lower() == "true")
    outputDirectoryName = config.get("DEFAULT", "reducer.outputDirectoryName")
    outputToStdOut = (config.get("DEFAULT", "reducer.outputToStdOut").lower() == "true")

    if outputToAccumulo:
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
            AccumuloInterface.connectForWriting(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
        except jpype.JavaException as exception:
            raise RuntimeError(exception.stacktrace())

    for i, geoPictureTile in enumerate(geoPictureTiles):
        heartbeat.write("%s About to make images for tile %s...\n" % (time.strftime("%H:%M:%S"), geoPictureTile.metadata["tileName"]))

        for config in configuration:
            heartbeat.write("%s     layer %s\n" % (time.strftime("%H:%M:%S"), config["layer"]))

            image = makeImage(geoPictureTile, config["layer"], config["bands"], config["outputType"], config["minRadiance"], config["maxRadiance"])
            if image is None: continue

            outputKey = "%s-%s-%010d" % (geoPictureTile.metadata["tileName"], config["layer"], geoPictureTile.metadata["timestamp"])

            if outputToAccumulo:
                heartbeat.write("%s     write to Accumulo with key %s\n" % (time.strftime("%H:%M:%S"), outputKey))
                buff = BytesIO()
                image.save(buff, "PNG", options="optimize")
                try:
                    AccumuloInterface.write(outputKey, json.dumps(geoPictureTile.metadata), buff.getvalue())
                except jpype.JavaException as exception:
                    raise RuntimeError(exception.stacktrace())

            if outputToLocalDirectory:
                heartbeat.write("%s     write to local filesystem with path %s/%s.png\n" % (time.strftime("%H:%M:%S"), outputDirectoryName, outputKey))
                image.save("%s/%s.png" % (outputDirectoryName, outputKey), "PNG", options="optimize")

            if outputToStdOut:
                heartbeat.write("%s     write to standard output with key %s\n" % (time.strftime("%H:%M:%S"), outputKey))
                buff = BytesIO()
                image.save(buff, "PNG", options="optimize")
                sys.stdout.write("%s\t%s\n" % (outputKey, base64.b64encode(buff.getvalue())))

    heartbeat.write("%s Finished everything; shutting down...\n" % time.strftime("%H:%M:%S"))

    if outputToAccumulo:
        heartbeat.write("%s     shut down Accumulo\n" % time.strftime("%H:%M:%S"))
        try:
            AccumuloInterface.finishedWriting()
        except jpype.JavaException as exception:
            raise RuntimeError(exception.stacktrace())
