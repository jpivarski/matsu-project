#!/usr/bin/env python

import math
import random
import time

import numpy
from PIL import Image

from cassius import *
import GeoPictureSerializer

picture = GeoPictureSerializer.deserialize(open("/home/pivarski/NOBACKUP/matsu_serialized/Australia_Karijini_bigger.serialized"))

def visualize2d(array2d, lowPercentile=0.0, highPercentile=99.9, fileName="/tmp/tmp.png"):
    lowValue, highValue = numpy.percentile(array2d, [lowPercentile, highPercentile])
    print "Range:", lowValue, highValue
    values = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)

    red = values[:,:]
    green = values[:,:]
    blue = values[:,:]

    output = Image.fromarray(numpy.dstack((red, green, blue)))
    output.save(open(fileName, "wb"))

def visualize3d(array3d, lowPercentile=0.0, highPercentile=99.9):
    for bandIndex, bandName in enumerate(picture.bands):
        visualize2d(array3d[:,:,bandIndex], lowPercentile=lowPercentile, highPercentile=highPercentile, fileName="/tmp/tmp_%s.png" % bandName)

def wavelength(bandNumber):
    if bandNumber < 70.5:
        return (bandNumber - 10.) * 10.213 + 446.
    else:
        return (bandNumber - 79.) * 10.110 + 930.

def incidentSolar(wavelength):
    return 2.5e-28/(math.exp(0.0143878/(wavelength*1e-9)/(5778)) - 1.)/(wavelength*1e-9)**5

################################################

mask = (picture.picture.sum(axis=2) > 0)
maskedPixels = picture.picture[mask]

normalizedPixels = numpy.empty(maskedPixels.shape, dtype=numpy.dtype(float))
for bandIndex in xrange(len(picture.bands)):
    bandName = picture.bands[bandIndex]
    normalizedPixels[:,bandIndex] = maskedPixels[:,bandIndex] / incidentSolar(wavelength(int(bandName[1:])))

pixelTotals = normalizedPixels.sum(axis=1)
for bandIndex in xrange(len(picture.bands)):
    normalizedPixels[:,bandIndex] /= pixelTotals

################################################

normalizedPicture = numpy.empty(picture.picture.shape, dtype=numpy.dtype(float))
for bandIndex in xrange(len(picture.bands)):
    bandName = picture.bands[bandIndex]
    normalizedPicture[:,:,bandIndex] = picture.picture[:,:,bandIndex] / incidentSolar(wavelength(int(bandName[1:])))

pictureTotals = normalizedPicture.sum(axis=2)
for bandIndex in xrange(len(picture.bands)):
    normalizedPicture[:,:,bandIndex] /= pictureTotals

normalizedPicture[numpy.isnan(normalizedPicture)] = 0.0

################################################

def kmeans(clusterCenters, dataset, numberOfIterations=10):
    for counter in xrange(numberOfIterations):
        print "Iteration", counter

        bestClusterIndex = None
        bestClusterDistance = None
        for index in xrange(clusterCenters.shape[0]):
            distance = numpy.absolute(dataset - clusterCenters[index,:]).sum(axis=1)

            if bestClusterIndex is None:
                bestClusterIndex = numpy.zeros(distance.shape, dtype=numpy.uint32)
                bestClusterDistance = distance

            else:
                better = (distance < bestClusterDistance)
                bestClusterIndex[better] = index
                bestClusterDistance[better] = distance[better]

        for index in xrange(clusterCenters.shape[0]):
            selection = (bestClusterIndex == index)
            denom = numpy.count_nonzero(selection)
            if denom > 0.0:
                clusterCenters[index] = dataset[selection].sum(axis=0) / denom

    return clusterCenters

numberOfClusters = 10
indexes = random.sample(xrange(normalizedPixels.shape[0]), numberOfClusters)
clusterCenters = normalizedPixels[indexes]
clusterCenters = kmeans(clusterCenters, normalizedPixels)

################################################

bestClusterIndex = None
bestClusterDistance = None
for index in xrange(clusterCenters.shape[0]):
    distance = numpy.absolute(normalizedPicture - clusterCenters[index,:])[mask].sum(axis=1)

    if bestClusterIndex is None:
        bestClusterIndex = numpy.zeros(distance.shape, dtype=numpy.uint32)
        bestClusterDistance = distance

    else:
        better = (distance < bestClusterDistance)
        bestClusterIndex[better] = index
        bestClusterDistance[better] = distance[better]

reds = numpy.zeros(mask.shape, dtype=numpy.uint8)
greens = numpy.zeros(mask.shape, dtype=numpy.uint8)
blues = numpy.zeros(mask.shape, dtype=numpy.uint8)
for index, color in enumerate(darkseries(10)):
    




    




