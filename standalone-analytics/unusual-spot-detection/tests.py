#!/usr/bin/env python

import math
import random
import time

import numpy
from PIL import Image

from cassius import *
import GeoPictureSerializer

pictureName = "Australia_Karijini_bigger"
# pictureName = "fresh_salt_water"
# pictureName = "PotassiumChloridePlant_topQuarter"
# pictureName = "NamibiaFloods-EO1A1800722011074110KF"
# pictureName = "GobiDesert01"
picture = GeoPictureSerializer.deserialize(open("/home/pivarski/NOBACKUP/matsu_serialized/%s.serialized" % pictureName))

numberOfClusters = 20
seedSigmas = 2.5

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

pixelTotals = numpy.sqrt(numpy.square(normalizedPixels).sum(axis=1))
for bandIndex in xrange(len(picture.bands)):
    normalizedPixels[:,bandIndex] /= pixelTotals

################################################

normalizedPicture = numpy.empty(picture.picture.shape, dtype=numpy.dtype(float))
for bandIndex in xrange(len(picture.bands)):
    bandName = picture.bands[bandIndex]
    normalizedPicture[:,:,bandIndex] = picture.picture[:,:,bandIndex] / incidentSolar(wavelength(int(bandName[1:])))

pictureTotals = numpy.sqrt(numpy.square(normalizedPicture).sum(axis=2))
for bandIndex in xrange(len(picture.bands)):
    normalizedPicture[:,:,bandIndex] /= pictureTotals

normalizedPicture[numpy.isnan(normalizedPicture)] = 0.0

def visualize2d(array2d, fileName, lowPercentile=0.0, highPercentile=99.9):
    lowValue, highValue = numpy.percentile(array2d, [lowPercentile, highPercentile])
    # print "Range:", lowValue, highValue
    values = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)

    red = values[:,:]
    green = values[:,:]
    blue = values[:,:]

    output = Image.fromarray(numpy.dstack((red, green, blue)))
    output.save(open(fileName, "wb"))

for bandIndex, bandName in enumerate(picture.bands):
    visualize2d(normalizedPicture[:,:,bandIndex], fileName="%s/raw_%s.png" % (pictureName, bandName))

################################################

def kmeans(clusterCenters, dataset, numberOfIterations=10, allChangeThreshold=1e-2, halfChangeThreshold=1e-3):
    for counter in xrange(numberOfIterations):
        bestClusterIndex = None
        bestClusterDistance = None
        for clusterIndex in xrange(clusterCenters.shape[0]):
            distance = numpy.sqrt(numpy.square(dataset - clusterCenters[clusterIndex,:]).sum(axis=1))

            if bestClusterIndex is None:
                bestClusterIndex = numpy.zeros(distance.shape, dtype=numpy.int32)
                bestClusterDistance = distance

            else:
                better = (distance < bestClusterDistance)
                bestClusterIndex[better] = clusterIndex
                bestClusterDistance[better] = distance[better]

        clusterPopulations = []
        for clusterIndex in xrange(clusterCenters.shape[0]):
            clusterPopulations.append((bestClusterIndex == clusterIndex).sum())

        changes = []
        for clusterIndex in xrange(clusterCenters.shape[0]):
            selection = (bestClusterIndex == clusterIndex)
            denom = numpy.count_nonzero(selection)
            if denom > 0.0:
                oldCluster = clusterCenters[clusterIndex].copy()
                clusterCenters[clusterIndex] = dataset[selection].sum(axis=0) / denom

                changes.append(numpy.sqrt(numpy.square(clusterCenters[clusterIndex] - oldCluster).sum()))

        if numberOfIterations > 10:
            print changes

        allChangeSatisfied = all(x < allChangeThreshold for x in changes)
        halfChangeSatisfied = sum(x < halfChangeThreshold for x in changes) > clusterCenters.shape[0]/2.0
        if allChangeSatisfied and halfChangeSatisfied:
            break

    return clusterPopulations

def clusterQuality(clsuterCenters, dataset):
    bestClusterDistance2 = None
    for clusterIndex in xrange(clusterCenters.shape[0]):
        distance2 = numpy.square(dataset - clusterCenters[clusterIndex,:]).sum(axis=1)

        if bestClusterDistance2 is None:
            bestClusterDistance2 = distance2

        else:
            better = (distance2 < bestClusterDistance2)
            bestClusterDistance2[better] = distance2[better]

    return numpy.sqrt(bestClusterDistance2.sum())

trials = []
for i in xrange(10):
    clusterCenters = normalizedPixels[random.sample(xrange(normalizedPixels.shape[0]), numberOfClusters)]
    kmeans(clusterCenters, normalizedPixels[random.sample(xrange(normalizedPixels.shape[0]), 1000)], numberOfIterations=10)
    trials.append((clusterQuality(clusterCenters, normalizedPixels), clusterCenters.copy()))

trials.sort()
quality, clusterCenters = trials[0]

clusterPopulations = kmeans(clusterCenters, normalizedPixels, numberOfIterations=100)

################################################

colors = darkseries(numberOfClusters, alternating=False, phase=0.0)
colors.reverse()

wavelengths = [wavelength(int(bandName[1:])) for bandName in picture.bands]

averageWavelengths = numpy.empty(numberOfClusters, dtype=numpy.dtype(float))
for clusterIndex in xrange(clusterCenters.shape[0]):
    averageWavelengths[clusterIndex] = wmean(wavelengths, clusterCenters[clusterIndex,:])[0]

order = numpy.argsort(averageWavelengths)
orderedColors = numpy.empty(len(colors), dtype=numpy.dtype(object))
maxClusterPopulations = numpy.percentile(clusterPopulations, 95.0)
for clusterIndex, color in zip(order, colors):
    # color.opacity = min(clusterPopulations[clusterIndex]/maxClusterPopulations/2.0 + 0.5, 1.0)
    orderedColors[clusterIndex] = color

plots = []
for clusterIndex, color in enumerate(orderedColors):
    plots.append(Scatter(x=wavelengths, y=clusterCenters[clusterIndex,:], marker=None, connector="unsorted", linecolor=color))

draw(Overlay(*plots, ymin=0.0, xlabel="wavelength [nm]", ylabel="weighted, normalized radiance"), fileName="%s/clusters.svg" % pictureName)

################################################

bestClusterIndex = None
bestClusterDistance = None
for index in xrange(clusterCenters.shape[0]):
    distance = numpy.sqrt(numpy.square(normalizedPicture - clusterCenters[index,:])[mask].sum(axis=1))

    if bestClusterIndex is None:
        bestClusterIndex = numpy.zeros(distance.shape, dtype=numpy.int32)
        bestClusterDistance = distance

    else:
        better = (distance < bestClusterDistance)
        bestClusterIndex[better] = index
        bestClusterDistance[better] = distance[better]

reds = numpy.zeros(mask.shape, dtype=numpy.uint8)
greens = numpy.zeros(mask.shape, dtype=numpy.uint8)
blues = numpy.zeros(mask.shape, dtype=numpy.uint8)
for clusterIndex, color in enumerate(orderedColors):
    lighterColor = lighten(color)

    selection = mask.copy()
    selection[mask] = (bestClusterIndex == clusterIndex)

    reds[selection] += lighterColor.r * 255
    greens[selection] += lighterColor.g * 255
    blues[selection] += lighterColor.b * 255

output = Image.fromarray(numpy.dstack((reds, greens, blues)))
output.save(open("%s/clusterIdentity.png" % pictureName, "wb"))

################################################

commonness = numpy.empty(bestClusterIndex.shape, dtype=numpy.dtype(float))
for clusterIndex in xrange(numberOfClusters):
    commonness[bestClusterIndex == clusterIndex] = clusterPopulations[clusterIndex]
commonness /= bestClusterDistance

clusteredPicture = numpy.zeros(mask.shape, dtype=numpy.uint8)
clusteredPicture[mask] = numpy.minimum((commonness - commonness.min())/(commonness.max() - commonness.min()) * 256, 255)
output = Image.fromarray(clusteredPicture)
output.save(open("%s/commonness.png" % pictureName, "wb"))

clusteredPicture = numpy.zeros(mask.shape, dtype=numpy.uint8)
clusteredPicture[mask] = numpy.minimum((commonness.max() - commonness)/(commonness.max() - commonness.min()) * 256, 255)
output = Image.fromarray(clusteredPicture)
output.save(open("%s/rareness.png" % pictureName, "wb"))

################################################

reducedClusterIndex = numpy.empty(bestClusterIndex.shape, dtype=numpy.int32)
for clusterIndex in xrange(numberOfClusters):
    selection = bestClusterIndex == clusterIndex
    ownedPoints = normalizedPixels[selection] - clusterCenters[clusterIndex,:]
    covarianceMatrix = numpy.cov(ownedPoints.T)

    normalizations = numpy.sqrt(numpy.sum(numpy.square(ownedPoints), axis=1))
    normalizedDisplacements = numpy.empty(ownedPoints.shape, dtype=numpy.dtype(float))
    for clusterDimension in xrange(ownedPoints.shape[1]):
        normalizedDisplacements[:,clusterDimension] = ownedPoints[:,clusterDimension] / normalizations

    lengthOfSigma = numpy.sqrt(numpy.sum(normalizedDisplacements.dot(covarianceMatrix) * normalizedDisplacements, axis=1))
    significances = normalizations / lengthOfSigma

    reducedClusterIndex[selection] = numpy.where(significances < seedSigmas, numpy.int32(clusterIndex), numpy.int32(-1))

try:
    array2d = picture.picture[:,:,picture.bands.index("B016")]
except ValueError:
    array2d = picture.picture[:,:,picture.bands.index("B03")]
lowValue, highValue = numpy.percentile(array2d, [0.0, 100.0])
values = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)

reds = values.copy()
greens = values.copy()
blues = values.copy()
for clusterIndex, color in enumerate(orderedColors):
    lighterColor = lighten(color)

    selection = mask.copy()
    selection[mask] = (reducedClusterIndex == clusterIndex)

    reds[selection] = lighterColor.r * 255
    greens[selection] = lighterColor.g * 255
    blues[selection] = lighterColor.b * 255

output = Image.fromarray(numpy.dstack((reds, greens, blues)))
output.save(open("%s/clusterIdentity_reduced.png" % pictureName, "wb"))

################################################

hierarchicalClusters = -numpy.ones(mask.shape, dtype=numpy.int32)
hierarchicalClusters[mask] = reducedClusterIndex

def visualize(N):
    try:
        array2d = picture.picture[:,:,picture.bands.index("B016")]
    except ValueError:
        array2d = picture.picture[:,:,picture.bands.index("B03")]
    lowValue, highValue = numpy.percentile(array2d, [0.0, 100.0])
    values = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)
    
    reds = values.copy()
    greens = values.copy()
    blues = values.copy()
    for clusterIndex, color in enumerate(orderedColors):
        lighterColor = lighten(color)

        reds[hierarchicalClusters == clusterIndex] = lighterColor.r * 255
        greens[hierarchicalClusters == clusterIndex] = lighterColor.g * 255
        blues[hierarchicalClusters == clusterIndex] = lighterColor.b * 255
        
    output = Image.fromarray(numpy.dstack((reds, greens, blues)))
    output.save(open("%s/clusterIdentity_reduced_%02d.png" % (pictureName, N), "wb"))

left = numpy.reciprocal(numpy.sqrt(numpy.sum(numpy.square(normalizedPicture - numpy.roll(normalizedPicture, 1, axis=0)), axis=2)))
right = numpy.reciprocal(numpy.sqrt(numpy.sum(numpy.square(normalizedPicture - numpy.roll(normalizedPicture, -1, axis=0)), axis=2)))
up = numpy.reciprocal(numpy.sqrt(numpy.sum(numpy.square(normalizedPicture - numpy.roll(normalizedPicture, 1, axis=1)), axis=2)))
down = numpy.reciprocal(numpy.sqrt(numpy.sum(numpy.square(normalizedPicture - numpy.roll(normalizedPicture, -1, axis=1)), axis=2)))

def grow():
    unassigned = (hierarchicalClusters == -1)

    bestChoice = None
    bestAntidiff = None
    newSelection = numpy.zeros(hierarchicalClusters.shape, dtype=numpy.dtype(bool))
    for clusterIndex in xrange(numberOfClusters):
        selection = (hierarchicalClusters == clusterIndex)
        selectionLeft = numpy.roll(selection, 1, axis=0)
        selectionRight = numpy.roll(selection, -1, axis=0)
        selectionUp = numpy.roll(selection, 1, axis=1)
        selectionDown = numpy.roll(selection, -1, axis=1)

        numpy.logical_and(selectionLeft, unassigned, selectionLeft)
        numpy.logical_and(selectionRight, unassigned, selectionRight)
        numpy.logical_and(selectionUp, unassigned, selectionUp)
        numpy.logical_and(selectionDown, unassigned, selectionDown)

        numpy.logical_or(newSelection, selectionLeft, newSelection)
        numpy.logical_or(newSelection, selectionRight, newSelection)
        numpy.logical_or(newSelection, selectionUp, newSelection)
        numpy.logical_or(newSelection, selectionDown, newSelection)

        antidiffLeft = numpy.zeros(selection.shape, dtype=numpy.dtype(float))
        antidiffRight = numpy.zeros(selection.shape, dtype=numpy.dtype(float))
        antidiffUp = numpy.zeros(selection.shape, dtype=numpy.dtype(float))
        antidiffDown = numpy.zeros(selection.shape, dtype=numpy.dtype(float))

        antidiffLeft[selectionLeft] = left[selectionLeft]
        antidiffRight[selectionRight] = right[selectionRight]
        antidiffUp[selectionUp] = up[selectionUp]
        antidiffDown[selectionDown] = down[selectionDown]

        antidiff = numpy.maximum(numpy.maximum(numpy.maximum(antidiffLeft, antidiffRight), antidiffUp), antidiffDown)

        if bestChoice is None:
            bestChoice = numpy.zeros(unassigned.shape, dtype=numpy.int32)
            bestAntidiff = antidiff

        else:
            better = (antidiff > bestAntidiff)
            bestChoice[better] = clusterIndex
            bestAntidiff[better] = antidiff[better]

    hierarchicalClusters[newSelection] = bestChoice[newSelection]

visualize(0)
for i in xrange(1, 20):
    print "Growth iteration", i
    grow()
    visualize(i)

################################################

expandedClusterIndex = numpy.empty(bestClusterIndex.shape, dtype=numpy.int32)
for clusterIndex in xrange(numberOfClusters):
    selection = bestClusterIndex == clusterIndex
    ownedPoints = normalizedPixels[selection] - clusterCenters[clusterIndex,:]
    covarianceMatrix = numpy.cov(ownedPoints.T)

    normalizations = numpy.sqrt(numpy.sum(numpy.square(ownedPoints), axis=1))
    normalizedDisplacements = numpy.empty(ownedPoints.shape, dtype=numpy.dtype(float))
    for clusterDimension in xrange(ownedPoints.shape[1]):
        normalizedDisplacements[:,clusterDimension] = ownedPoints[:,clusterDimension] / normalizations

    lengthOfSigma = numpy.sqrt(numpy.sum(normalizedDisplacements.dot(covarianceMatrix) * normalizedDisplacements, axis=1))
    significances = normalizations / lengthOfSigma

    expandedClusterIndex[selection] = numpy.where(significances > 5.0, numpy.int32(clusterIndex), numpy.int32(-1))

try:
    array2d = picture.picture[:,:,picture.bands.index("B016")]
except ValueError:
    array2d = picture.picture[:,:,picture.bands.index("B03")]
lowValue, highValue = numpy.percentile(array2d, [0.0, 100.0])
values = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)

reds = values.copy()
greens = values.copy()
blues = values.copy()
for clusterIndex, color in enumerate(orderedColors):
    lighterColor = lighten(color)

    if float(clusterPopulations[clusterIndex])/sum(clusterPopulations) < 0.02:
        selection = mask.copy()
        selection[mask] = (bestClusterIndex == clusterIndex)

        reds[selection] = lighterColor.r * 255
        greens[selection] = lighterColor.g * 255
        blues[selection] = lighterColor.b * 255

output = Image.fromarray(numpy.dstack((reds, greens, blues)))
output.save(open("%s/clusterIdentity_anomalousClusters.png" % pictureName, "wb"))

reds = values.copy()
greens = values.copy()
blues = values.copy()
for clusterIndex, color in enumerate(orderedColors):
    lighterColor = lighten(color)

    selection = mask.copy()
    selection[mask] = (expandedClusterIndex == clusterIndex)

    reds[selection] = lighterColor.r * 255
    greens[selection] = lighterColor.g * 255
    blues[selection] = lighterColor.b * 255

output = Image.fromarray(numpy.dstack((reds, greens, blues)))
output.save(open("%s/clusterIdentity_farFromClusters.png" % pictureName, "wb"))
    
################################################

significancePicture = numpy.empty(bestClusterIndex.shape, dtype=numpy.dtype(float))
for clusterIndex in xrange(numberOfClusters):
    selection = bestClusterIndex == clusterIndex
    ownedPoints = normalizedPixels[selection] - clusterCenters[clusterIndex,:]
    covarianceMatrix = numpy.cov(ownedPoints.T)

    normalizations = numpy.sqrt(numpy.sum(numpy.square(ownedPoints), axis=1))
    normalizedDisplacements = numpy.empty(ownedPoints.shape, dtype=numpy.dtype(float))
    for clusterDimension in xrange(ownedPoints.shape[1]):
        normalizedDisplacements[:,clusterDimension] = ownedPoints[:,clusterDimension] / normalizations

    lengthOfSigma = numpy.sqrt(numpy.sum(normalizedDisplacements.dot(covarianceMatrix) * normalizedDisplacements, axis=1))
    significances = normalizations / lengthOfSigma

    significancePicture[selection] = significances

lowValue, highValue = numpy.percentile(significancePicture, [0.0, 100.0])
values = numpy.array(numpy.maximum(numpy.minimum(((significancePicture - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)
shapedValues = numpy.zeros(mask.shape, dtype=numpy.uint8)
shapedValues[mask] = values

output = Image.fromarray(shapedValues)
output.save(open("%s/clusterIdentity_significance.png" % pictureName, "wb"))

try:
    array2d = picture.picture[:,:,picture.bands.index("B016")]
except ValueError:
    array2d = picture.picture[:,:,picture.bands.index("B03")]
lowValue, highValue = numpy.percentile(array2d, [0.0, 100.0])
values2 = numpy.array(numpy.maximum(numpy.minimum(((array2d - lowValue) / (highValue - lowValue)) * 256, 255), 0), dtype=numpy.uint8)

reds = values2.copy()
greens = values2.copy()
blues = values2.copy()
for clusterIndex, color in enumerate(orderedColors):
    lighterColor = lighten(color)

    selection = mask.copy()
    selection[mask] = (significancePicture > 5.0)

    reds[selection] = 255
    greens[selection] = 0
    blues[selection] = 0

output = Image.fromarray(numpy.dstack((reds, greens, blues)))
output.save(open("%s/clusterIdentity_significantlyFarFromClusters.png" % pictureName, "wb"))
