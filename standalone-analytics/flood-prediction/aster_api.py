import datetime
import time
import json
import os

import numpy
from PIL import Image
from osgeo import osr

import GeoPictureSerializer
from osgeo import gdal
from gdalconst import *

osr.UseExceptions()
#granule = 'ASTGTM2_S19E026'
#dataset = gdal.Open(os.path.join('ASTER_data', granule + '_dem.tif'), GA_ReadOnly)
#print('Dataset description: %s\n'%dataset.GetDescription())
#print('Dataset metadata:%s\n'%dataset.GetMetadata())
#print('Raster Xsize %i Ysize %i \n'%(dataset.RasterXSize, dataset.RasterYSize))
#a=dataset.ReadAsArray()

class asterPicture:
  def __init__(self, tiffFile):
    self.dataset = gdal.Open(tiffFile, GA_ReadOnly)
    self.namestem = (os.path.split(self.dataset.GetDescription())[-1]).split('.')[0]

  def png(self, filename=None, label=None, dirname='/tmp'):
    if filename is not None:
      if label is not None:
        filename = os.path.join(dirname, self.namestem + label + '.png')
      else:
        filename = os.path.join(dirname, self.namestem + '.png')
    else:
      if label is not None:
        filename = os.path.join(dirname, self.namestem + label + '.png')
      else:
        filename = os.path.join(dirname, self.namestem + '.png')
    png(self.dataset.ReadAsArray(), fileName=filename)

def png(a, minRadiance=None, maxRadiance=None, fileName="tmp.png", alpha=False, trimmed=True, debug=False, line=None, lines=None):
  if trimmed:
    mincut = 5.
    maxcut = 95.
    if minRadiance is None:
      if debug:
        pass
      minRadiance = numpy.percentile(a, mincut).min()
    if maxRadiance is None:
      maxRadiance = numpy.percentile(a, maxcut).max()
  else:
    if minRadiance is None:
        minRadiance = min(a.min(), a.min(), a.min())
    if maxRadiance is None:
        maxRadiance = max(a.max(), a.max(), a.max())
  if debug:
    print minRadiance, maxRadiance
  try:
    picture = (a - minRadiance) * 255./(maxRadiance - minRadiance)
  except:
    print minRadiance
    print maxRadiance
    print trimmed
    x=input()
  if debug:
    print 'set picture above 0'
    print numpy.maximum(picture, 0., picture)
    print 'set picture below 255'
    print numpy.minimum(picture, 255., picture)
    for p in range(1,100):
      print p, numpy.percentile(picture, p)
  # ensure that values of picture are between 0 and 255.
  numpy.maximum(picture, 0., picture)
  numpy.minimum(picture, 255., picture)
  if line is not None:
    lines = [line]
  if lines is not None:
    for line in lines:
      picture[line[0],:] = 255.
      picture[:,line[1]] = 255.
  picture = numpy.array(picture, dtype=numpy.uint8)
  if alpha:
      pass
  else:
      if len(picture.shape)==3:
        image = Image.fromarray(picture[:,:,0])
      else:
        image = Image.fromarray(picture)
      image.save(fileName, "PNG", option="optimize")

def png2(redBand, greenBand, blueBand, minRadiance=None, maxRadiance=None, fileName="tmp.png", alpha=False, trimmed=True, debug=False, line=None, lines=None):
    mincut = 5.
    maxcut = 95.
    inpic = numpy.dstack([redBand, greenBand, blueBand])
    if minRadiance is None:
      minRadiance = min(numpy.percentile(inpic[:,:,0], mincut).min(),
                        numpy.percentile(inpic[:,:,1], mincut).min(),
                        numpy.percentile(inpic[:,:,2], mincut).min())
    if maxRadiance is None:
      maxRadiance = max(numpy.percentile(inpic[:,:,0], maxcut).max(),
                        numpy.percentile(inpic[:,:,1], maxcut).max(),
                        numpy.percentile(inpic[:,:,2],maxcut).max())

    picture = (inpic[:,:,:] - minRadiance) * 255./(maxRadiance - minRadiance)
    numpy.maximum(picture, 0., picture)
    numpy.minimum(picture, 255., picture)
    if line is not None:
      lines = [line]
    if lines is not None:
      for line in lines:
        picture[line[0],:,:] = 255.
        picture[:,line[1],:] = 255.
    picture = numpy.array(picture, dtype=numpy.uint8)
    if alpha:
      image = Image.fromarray(numpy.dstack((picture[:,:,0], picture[:,:,1], picture[:,:,2], alphaBand)))
      image.save(fileName, "PNG", option="optimize")
    else:
      image = Image.fromarray(picture)
      image.save(fileName, "PNG", option="optimize")
