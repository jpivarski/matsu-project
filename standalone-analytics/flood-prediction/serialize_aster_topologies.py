#!/usr/bin/env python
# Called as follows, creates 4 files, /tmp/test.datq0, /tmp/test.datq1, /tmp/test.datq2, /tmp/test.datq3
# Each of the outputs is a serialization of a numpy 1800 x 1800 x 3 array with bands 'raw', 'curvature_lat', and
# 'curvature_lng'. 'raw' is always indexed to be the first band.
# python serialize_aster_topologies.py ASTER_data /tmp/test.dat --toLocalFile 
import sys
import os
import xml.etree.ElementTree as ET
import glob
import json
import argparse
import subprocess
import re

import numpy
import aster_topology
from PIL import Image
from osgeo import gdal
from osgeo import osr
from osgeo import gdalconst
from aster_api import asterPicture

HADOOP = "/opt/hadoop/bin/hadoop"

gdal.UseExceptions()
osr.UseExceptions()

parser = argparse.ArgumentParser(description="Glue together a set of image bands with their metadata, serialize, and put the result in HDFS.")
parser.add_argument("inputDirectory", help="local filesystem directory containing TIF files")
parser.add_argument("outputFilename", help="HDFS filename for the output (make sure the directory exists)")
parser.add_argument("--bands", nargs="+", default=["raw","curvature"], help="list of bands to retrieve, like \"raw grad grad+ curvature\" ")
parser.add_argument("--requireAllBands", action="store_true", help="if any bands are missing, skip this image; default is to simply ignore the missing bands and include the others")
parser.add_argument("--toLocalFile", action="store_true", help="save the serialized result to a local file instead of HDFS")
args = parser.parse_args()


def parseElement(inelement):
  outdict = {}  
  containers = []
  for element in inelement:
    if len(element.getchildren()) > 0:
      containers.append(element)
    else:
      outdict[element.tag] = element.text
    for container in containers:
      outdict[container.tag] = parseElement(container)
  return outdict

import GeoPictureSerializer
geoPicture = GeoPictureSerializer.GeoPicture()

# convert the granule MetaData file into a JSON-formatted string
granulemetadata = {}
try:
    rawdataFileName = glob.glob(args.inputDirectory + '/*_dem.tif')[0]
except IndexError:
    raise Exception("%s doesn't have rawdata file" % args.inputDirectory)

metadataFileName = rawdataFileName.replace('_dem.tif', '.zip.xml')
if not os.path.exists(rawdataFileName):
    raise Exception("%s doesn't have the metadata file " % metadataFileName)

metadata = ET.parse(metadataFileName)
last = granulemetadata
stack = []
granulemetadata = parseElement(metadata.getroot())
geoPicture.metadata["granuleMetaData"] = json.dumps(granulemetadata)
asterpicture = asterPicture(rawdataFileName)
# Note that the 'tiffs' dictionary maps a feature name to a numpy array with the exception
# of the 'raw' feature which maps onto a gdal dataset with attendent functionality like getting raster x size.
tiffs = {}
tiffs.update(aster_topology.gen_topological_bands(asterpicture.dataset.ReadAsArray(), requestedFeatures = args.bands))
tiffs['raw'] = asterpicture.dataset
for key in ['curvature','gradient']:
  try:
    tiffs.pop(key)
    args.bands.append(key + '_lng')
    args.bands.append(key + '_lat')
    print args.bands
  except:
    print 'not dropping %s\n'%key

print 'Features Present: ',repr(tiffs.keys()).strip('[').strip(']')

sampletiff = tiffs['raw']
geoPicture.metadata["GeoTransform"] = json.dumps(sampletiff.GetGeoTransform())
geoPicture.metadata["Projection"] = sampletiff.GetProjection()
geoPicture.bands = list(set(args.bands).intersection(tiffs.keys()))
geoPicture.bands.sort()
# raw is always the 0th index.
geoPicture.bands.insert(0, geoPicture.bands.pop(geoPicture.bands.index('raw')))

print dir(sampletiff)
print 'Xsize, Ysize:',sampletiff.RasterXSize, sampletiff.RasterYSize
print 'Bands to include in serial object: ',repr(geoPicture.bands).strip('[').strip(']')

if args.requireAllBands and len(geoPicture.bands) != len(args.bands):
    sys.exit(0)   # exit quietly if we require all bands but don't have them
if len(geoPicture.bands) == 0:
    sys.exit(0)   # also exit quietly if there are no bands

quadrantxsize = sampletiff.RasterXSize/2
quadrantysize = sampletiff.RasterXSize/2
quadrants = [(0,0), (quadrantysize,0), (0, quadrantxsize), (quadrantysize, quadrantxsize)]

for quadrant in quadrants:
  array = numpy.empty((quadrantysize, quadrantxsize, len(geoPicture.bands)), dtype=numpy.float)

  for index, key in enumerate(geoPicture.bands):
      sys.stdout.write("Adding feature %s\n"%key)
      scaleOffset = 0.
      scaleFactor = 1.  
      # raw has to be treated differently because it is a gdal data object while the features are numpy arrays.
      if key == 'raw':
        print 'Raw raster count: ', tiffs[key].RasterCount
        band = tiffs[key].GetRasterBand(1).ReadAsArray()[quadrant[0]:quadrant[0] + quadrantysize, quadrant[1]:quadrant[1] + quadrantxsize]
      else:
        band = tiffs[key][quadrant[0]:quadrant[0] + quadrantysize, quadrant[1]:quadrant[1] + quadrantxsize]
      array[:,:,index] = (band * scaleFactor) + scaleOffset

  geoPicture.picture = array

  if args.toLocalFile:
      output = open(args.outputFilename + 'q' + str(quadrants.index(quadrant)), "w")
      geoPicture.serialize(output)
      output.write("\n")
  else:
      hadoop = subprocess.Popen([HADOOP, "dfs", "-put", "-", args.outputFilename + str(quadrants.index(quadrant))], stdin=subprocess.PIPE)
      geoPicture.serialize(hadoop.stdin)
      hadoop.stdin.write("\n")
