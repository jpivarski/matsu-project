#!/usr/bin/env python

import sys
import json
import datetime
import threading
import time
from math import floor, ceil
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import numpy
from scipy.ndimage.interpolation import affine_transform
from osgeo import osr

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

def input_oneLine(inputStream):
    line = inputStream.readline()
    if not line: raise IOError("No input")
    return GeoPictureSerializer.deserialize(line)

def input_SequenceFile(inputStream, incomingBandRestriction):
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

def map_to_tiles(inputStream, outputStream, depth=10, depthForKey=1, longpixels=512, latpixels=256, numLatitudeSections=1, splineOrder=3, useSequenceFiles=False, incomingBandRestriction=None, outgoingBandRestriction=None, modules=None, globalVariables={}):
    """Performs the mapping step of the Hadoop map-reduce job.

    Map: read L1G, possibly split by latitude, split by tile, transform pictures into tile coordinates, and output (tile coordinate and timestamp, transformed
 picture) key-value pairs.
                                            
        * inputStream: usually sys.stdin; should be a serialized L1G picture.
        * outputStream: usually sys.stdout; keys and values are separated by a tab, key-value pairs are separated by a newline.
        * depth: logarithmic scale of the tile; 10 is the limit of Hyperion's resolution
        * depthForKey: widest zoom scale for the reducer (must be a smaller number than 'depth')
        * longpixels, latpixels: number of pixels in the output tiles
        * numLatitudeSections: number of latitude stripes to cut before splitting into tiles (reduces error due to Earth's curvature)
        * splineOrder: order of the spline used to calculate the affine_transformation (see SciPy docs); must be between 0 and 5
        * useSequenceFiles: if True, read data one band at a time (possibly dropping bands) from Hadoop SequenceFiles
        * incomingBandRestriction: if not None, restrict the incoming bands to this set
        * outgoingBandRestriction: if not None, restrict the outgoing bands to this set
        * modules: a list of external Python files to evaluate as analytics
    """

    loadedModules = []
    if modules is not None:
        for module in modules:
            globalVars = {}
            exec(compile(open(module).read(), module, "exec"), globalVars)
            loadedModules.append(globalVars["newBand"])

    sys.stderr.write("Loading an image at %s\n" % time.strftime("%H:%M:%S"))

    # get the Level-1 image
    if not useSequenceFiles:
        geoPicture = input_oneLine(inputStream)
    else:
        geoPicture = input_SequenceFile(inputStream, incomingBandRestriction)

    inputBands = geoPicture.bands[:]

    sys.stderr.write("Starting modules at %s\n" % time.strftime("%H:%M:%S"))

    # run the external analytics
    for newBand in loadedModules:
        geoPicture = newBand(geoPicture)

    sys.stderr.write("Removing bands at %s\n" % time.strftime("%H:%M:%S"))

    # remove unnecessary bands
    geoPicture = removeBands(geoPicture, outgoingBandRestriction)

    sys.stderr.write("Creating tiles at %s\n" % time.strftime("%H:%M:%S"))

    # convert GeoTIFF coordinates into degrees
    tlx, weres, werot, tly, nsrot, nsres = json.loads(geoPicture.metadata["GeoTransform"])
    spatialReference = osr.SpatialReference()
    spatialReference.ImportFromWkt(geoPicture.metadata["Projection"])
    coordinateTransform = osr.CoordinateTransformation(spatialReference, spatialReference.CloneGeogCS())
    rasterXSize = geoPicture.picture.shape[1]
    rasterYSize = geoPicture.picture.shape[0]
    rasterDepth = geoPicture.picture.shape[2]

    # get the timestamp to use as part of the key
    timestamp = time.mktime(datetime.datetime.strptime(json.loads(geoPicture.metadata["L1T"])["PRODUCT_METADATA"]["START_TIME"], "%Y %j %H:%M:%S").timetuple())

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

            # do some linear algebra to convert coordinates
            L2PNG_to_geo_trans = numpy.matrix([[(latmin - latmax)/float(latpixels), 0.], [0., (longmax - longmin)/float(longpixels)]])

            L1TIFF_to_geo_trans = numpy.matrix([[(downLat - upLat)/((thetop - bottom)*rasterYSize), (rightLat - leftLat)/rasterXSize], [(downLong - upLong)/((thetop - bottom)*rasterYSize), (rightLong - leftLong)/rasterXSize]])
            geo_to_L1TIFF_trans = L1TIFF_to_geo_trans.I

            trans = geo_to_L1TIFF_trans * L2PNG_to_geo_trans

            offset_in_deg = numpy.matrix([[latmax - cornerLat], [longmin - cornerLong]], dtype=numpy.double)

            # correct for the bottom != 0. case (only if section > 0)
            truncate_correction = L1TIFF_to_geo_trans * numpy.matrix([[int(floor(bottom*rasterYSize))], [0.]], dtype=numpy.double)

            # correct for the curvature of the Earth between the top of the section and the bottom of the section (that's why we cut into latitude sections)
            curvature_correction = L1TIFF_to_geo_trans * (geo_to_L1TIFF_trans * numpy.matrix([[leftLat - cornerLat], [leftLong - cornerLong]], dtype=numpy.double) - numpy.matrix([[(middle*rasterYSize)], [0.]], dtype=numpy.double))

            offset = L1TIFF_to_geo_trans.I * (offset_in_deg - truncate_correction - curvature_correction)

            offset = offset[0,0], offset[1,0]

            # lay the GeoTIFF into the output image array
            inputPicture = geoPicture.picture[int(floor(bottom*rasterYSize)):int(ceil(thetop*rasterYSize)),:,:]
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
            outputGeoPicture.metadata = geoPicture.metadata
            outputGeoPicture.bands = geoPicture.bands + ["MASK"]

            parentDepth, parentLongIndex, parentLatIndex = ti
            while parentDepth > depthForKey:
                parentDepth, parentLongIndex, parentLatIndex = tileParent(parentDepth, parentLongIndex, parentLatIndex)

            sys.stderr.write("Writing tile %s at %s\n" % (tileName(*ti), time.strftime("%H:%M:%S")))

            globalVariables["forestallDeath"] = True
            outputStream.write("%s.%s-%010d\t" % (tileName(parentDepth, parentLongIndex, parentLatIndex), tileName(*ti), timestamp))
            try:
                outputGeoPicture.serialize(outputStream)
            except IOError:
                outputStream.write("BROKEN")
            outputStream.write("\n")
            globalVariables["forestallDeath"] = False

    sys.stderr.write("Done at %s\n" % time.strftime("%H:%M:%S"))
    globalVariables["doneWithEverything"] = True

def doEverything(globalVariables):
    try:
        config = configparser.ConfigParser()
        config.read(["../CONFIG.ini", "CONFIG.ini"])

        zoomDepthNarrowest = int(config.get("DEFAULT", "mapreduce.zoomDepthNarrowest"))
        zoomDepthWidest = int(config.get("DEFAULT", "mapreduce.zoomDepthWidest"))
        if zoomDepthWidest >= zoomDepthNarrowest:
            raise Exception("mapreduce.zoomDepthWidest must be a smaller number (lower zoom level) than mapreduce.zoomDepthNarrowest")

        longpixels = int(config.get("DEFAULT", "mapper.tileLongitudePixels"))
        latpixels = int(config.get("DEFAULT", "mapper.tileLatitudePixels"))
        numLatitudeSections = int(config.get("DEFAULT", "mapper.numberOfLatitudeSections"))
        splineOrder = int(config.get("DEFAULT", "mapper.splineOrder"))
        useSequenceFiles = (config.get("DEFAULT", "mapper.useSequenceFiles").lower() == "true")
        incomingBandRestriction = json.loads(config.get("DEFAULT", "mapper.incomingBandRestriction"))
        if incomingBandRestriction is not None:
            incomingBandRestriction = set(incomingBandRestriction)
        outgoingBandRestriction = json.loads(config.get("DEFAULT", "mapper.outgoingBandRestriction"))
        if outgoingBandRestriction is not None:
            outgoingBandRestriction = set(outgoingBandRestriction)
        modules = json.loads(config.get("DEFAULT", "mapper.modules"))
        if modules == []: modules = None

        map_to_tiles(sys.stdin, sys.stdout, depth=zoomDepthNarrowest, depthForKey=zoomDepthWidest, longpixels=longpixels, latpixels=latpixels, numLatitudeSections=numLatitudeSections, splineOrder=splineOrder, useSequenceFiles=useSequenceFiles, incomingBandRestriction=incomingBandRestriction, outgoingBandRestriction=outgoingBandRestriction, modules=modules, globalVariables=globalVariables)
    except Exception as exception:
        globalVariables["exception"] = exception
        sys.exit(0)

if __name__ == "__main__":
    osr.UseExceptions()
    globalVariables = {"forestallDeath": False, "doneWithEverything": False}

    workerThread = threading.Thread(target=doEverything, args=(globalVariables,), name="WorkerThread")
    workerThread.daemon = True
    workerThread.start()

    frequencyInSeconds = 10
    lifetime = 40*60
    birth = time.time()

    while True:
        sys.stderr.write("Heartbeat at %s\n" % time.strftime("%H:%M:%S"))
        sys.stderr.write("reporter:status:Heartbeat at %s\n" % time.strftime("%H:%M:%S"))
        time.sleep(frequencyInSeconds)
        
        now = time.time()
        if globalVariables["forestallDeath"]:
            sys.stderr.write("Narrowly avoided dying while writing at %s\n" % time.strftime("%H:%M:%S"))

        elif now - birth > lifetime:
            sys.stderr.write("Cardiac at %s after a fulfilling life of %g minutes\n" % (time.strftime("%H:%M:%S"), (now - birth)/60.))
            sys.stderr.write("reporter:status:Cardiac at %s after a fulfilling life of %g minutes\n" % (time.strftime("%H:%M:%S"), (now - birth)/60.))
            break

        if globalVariables["doneWithEverything"]:
            sys.stderr.write("Done with everything at %s\n" % time.strftime("%H:%M:%S"))
            break

        if "exception" in globalVariables:
            sys.stderr.write("Oops, exception at %s\n" % time.strftime("%H:%M:%S"))
            raise globalVariables["exception"]

    sys.stderr.write("Out of the loop at %s\n" % time.strftime("%H:%M:%S"))
