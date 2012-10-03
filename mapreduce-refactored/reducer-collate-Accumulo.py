#!/usr/bin/env python

from io import BytesIO
try:
    import ConfigParser as configparser
except ImportError:
    import configparser

import jpype

from utilities import *

################################################################################## collate

def makeParentKeys(keys):
    parentKeys = {}
    for key in keys:
        depth, longIndex, latIndex, layer, timestamp = key.split("-")

        parentKey = "%s-%s" % (tileName(*tileParent(depth, longIndex, latIndex)), timestamp)
        if parentKey not in parentKeys:
            parentKeys[parentKey] = []
        parentKeys[parentKey].append(key)

    return parentKeys

def collate(keys, AccumuloInterface, splineOrder, verbose=False):
    for parentKey, childKeys in makeParentKeys(keys).items():
        sys.stderr.write("%s Building %s from %s...\n" % (time.strftime("%H:%M:%S"), parentKey, str(childKeys)))

        childImages = []
        for key in childKeys:
            sys.stderr.write("%s     loading %s...\n" % (time.strftime("%H:%M:%S"), str(childKeys)))

            try:
                l2pngBytes = AccumuloInterface.readL2png(key)
            except jpype.JavaException as exception:
                raise RuntimeError(exception.stacktrace())

            buff = BytesIO(l2pngBytes)
            childImages.append(numpy.asarray(Image.open(buff)))

        sys.stderr.write("%s     shrinking and overlaying %d images...\n" % (time.strftime("%H:%M:%S"), len(childKeys)))

        shape = childImages[0].shape
        outputRed = numpy.zeros(shape, dtype=numpy.uint8)
        outputGreen = numpy.zeros(shape, dtype=numpy.uint8)
        outputBlue = numpy.zeros(shape, dtype=numpy.uint8)
        outputMask = numpy.zeros(shape, dtype=numpy.uint8)
            
        for image in childImages:
            rasterYSize, rasterXSize = outputRed.shape
            inputRed, inputGreen, inputBlue, inputMask = image

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

        parentImage = Image.fromarray(numpy.dstack((outputRed, outputGreen, outputBlue, outputMask)))
        buff = BytesIO()
        parentImage.save(buff, "PNG", options="optimize")

        sys.stderr.write("%s     writing to Accumulo key %s...\n" % (time.strftime("%H:%M:%S"), parentKey))

        try:
            outputAccumulo.write(parentKey, "{}", buff.getvalue())
        except jpype.JavaException as exception:
            raise RuntimeError(exception.stacktrace())

################################################################################## entry point

if __name__ == "__main__":
    sys.stderr.write("%s Enter reducer-collate-Accumulo.py...\n" % time.strftime("%H:%M:%S"))

    config = configparser.ConfigParser()
    config.read(["../CONFIG.ini", "CONFIG.ini"])

    sys.stderr.write("%s Starting the Java Virtual Machine...\n" % time.strftime("%H:%M:%S"))
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
        AccumuloInterface = AccumuloInterface.connectForReading(ACCUMULO_DB_NAME, ZOOKEEPER_LIST, ACCUMULO_USER_NAME, ACCUMULO_PASSWORD, ACCUMULO_TABLE_NAME)
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    zoomDepthNarrowest = int(config.get("DEFAULT", "mapreduce.zoomDepthNarrowest"))
    zoomDepthWidest = int(config.get("DEFAULT", "mapreduce.zoomDepthWidest"))
    if zoomDepthWidest >= zoomDepthNarrowest:
        raise Exception("mapreduce.zoomDepthWidest must be a smaller number (lower zoom level) than mapreduce.zoomDepthNarrowest")

    sys.stderr.write("%s Extracting all level %02d keys from the database...\n" % (time.strftime("%H:%M:%S"), zoomDepthNarrowest))
    keys = {}
    try:
        keys[zoomDepthNarrowest] = getKeys("T%02d-" % zoomDepthNarrowest, "T%02d-" % (zoomDepthNarrowest + 1))
    except jpype.JavaException as exception:
        raise RuntimeError(exception.stacktrace())

    sys.stderr.write("%s Collating up to level %02d...\n" % (time.strftime("%H:%M:%S"), zoomDepthWidest))
    splineOrder = int(config.get("DEFAULT", "mapper.splineOrder"))
    for depth in xrange(zoomDepthNarrowest, zoomDepthWidest, -1):
        collate(keys, AccumuloInterface, splineOrder, verbose=True)

    sys.stderr.write("%s Finished everything; shutting down...\n" % time.strftime("%H:%M:%S"))
