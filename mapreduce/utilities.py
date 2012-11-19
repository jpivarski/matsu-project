#!/usr/bin/env python

import sys
from math import floor

import numpy
from scipy.ndimage.interpolation import affine_transform

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

class Heartbeat:
    def __init__(self, stdout=True, stderr=True, reporter=True):
        self.stdout = stdout
        self.stderr = stderr
        self.reporter = reporter

    def write(self, message):
        if self.stdout:
            sys.stdout.write(message)
        if self.stderr:
            sys.stderr.write(message)
        if self.reporter:
            sys.stderr.write("reporter:status:" + message)

def makeParentChildMap(keylist):
    parentKeys = {}
    for key in keylist:
        depth, longIndex, latIndex, layer, timestamp = key.split("-")
        depth = int(depth[1:])
        longIndex = int(longIndex)
        latIndex = int(latIndex)

        parentKey = "%s-%s-%s" % (tileName(*tileParent(depth, longIndex, latIndex)), layer, timestamp)
        if parentKey not in parentKeys:
            parentKeys[parentKey] = []
        parentKeys[parentKey].append(key)

    return parentKeys

def zoomOutImage(parentKey, childKeys, childImages, splineOrder):
    rasterYSize, rasterXSize = childImages[0].shape[0:2]
    outputRed = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
    outputGreen = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
    outputBlue = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)
    outputMask = numpy.zeros((rasterYSize, rasterXSize), dtype=numpy.uint8)

    for key, image in zip(childKeys, childImages):
        inputRed = image[:,:,0]
        inputGreen = image[:,:,1]
        inputBlue = image[:,:,2]
        inputMask = image[:,:,3]

        trans = numpy.matrix([[2., 0.], [0., 2.]])
        offset = 0., 0.

        inputRed = affine_transform(inputRed, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        inputGreen = affine_transform(inputGreen, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        inputBlue = affine_transform(inputBlue, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)
        inputMask = affine_transform(inputMask, trans, offset, (rasterYSize, rasterXSize), None, splineOrder)

        depth, longIndex, latIndex, layer, timestamp = key.split("-")
        depth = int(depth[1:])
        longIndex = int(longIndex)
        latIndex = int(latIndex)

        longOffset, latOffset = tileOffset(depth, longIndex, latIndex)
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

    return numpy.dstack((outputRed, outputGreen, outputBlue, outputMask))
