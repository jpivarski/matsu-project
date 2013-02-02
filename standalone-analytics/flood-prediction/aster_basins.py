#!/usr/bin/env python
# non map-reduce running looks like this:
# (using ZOOM = 0.1)
#python aster_basins.py -m 200 -d 100 ASTGTM2_S19E026_dem.tif
from aster_api import *
from features import report
from spacetimeIdentity import *
from optparse import OptionParser, make_option
from scipy.ndimage.interpolation import zoom
import scipy.signal
import math
import numpy
import sys
import os

ZOOM = 0.1
def stretch(row, vetorow, grad_minus, grad_plus, TAGVALUE, ORIGTAGVALUE):
  # find the left and right ends of the subtended pixels.
  emptyrighthand = numpy.logical_and( numpy.logical_and( numpy.roll(row, -1) != TAGVALUE, row == TAGVALUE), grad_minus <= 0)
  emptylefthand = numpy.logical_and( numpy.logical_and( numpy.roll(row, 1) != TAGVALUE, row == TAGVALUE), grad_plus >= 0)  
  stopIndex = None
  for taggedpixel in numpy.where(numpy.logical_or(emptyrighthand, emptylefthand))[0]:
    row_min_ind = taggedpixel
    # going to the right, is there any pixel that is either already part of the region or starts going downhill?
    downhill = numpy.logical_or(grad_plus[ row_min_ind + 1 : ] > 0, row[ row_min_ind + 1 : ] >= ORIGTAGVALUE)
    if any(downhill):
      stretchright = taggedpixel + min(numpy.where(downhill)[0]) + 1
    else:
      stretchright = len(row)
    # here, downhill is the set of pixels to the left that are going downhill relative to this cell or have been previously tagged.
    downhill = numpy.logical_or(grad_minus[ 1 : row_min_ind ] < 0, row[ 1 : row_min_ind ] >= ORIGTAGVALUE)
    # stretchleft is the nearest pixel to the left that is going downhill or has been previously tagged.
    if any(downhill):
      stretchleft = max(numpy.where(downhill)[0]) + 1 # nb: need to add 1 because downhill excludes first column.
    else:
      stretchleft = 0
    row[ stretchleft + 1 : stretchright ] = TAGVALUE
    vetorow[ stretchleft + 1 : stretchright ] = TAGVALUE
    stopIndex = stretchright
  return stopIndex

def degToPixel(latitude, longitude, longmin = 26., latmin = 18., granuleXsize = 1.0, granuleYsize = 1.0, NPIX = (3600, 3600), latminute = 0., longminute = 0.):
  # longmin, latmin are 'upper left' corner of a square granule within which the pixel is to be
  # located. granuleXsize and granuleYsize is the dimensions of the granule in degrees. For
  # ASTER data this is 1.0 deg each.
  # NPIX is a tuple consisting of the number of pixels in X,Y dimension.
  # for ASTER this is (3600, 3600)
  latitude = latitude + latminute/60.
  longitude = longitude + longminute/60.
  longmax = longmin + granuleYsize
  latmax = latmin + granuleXsize
  xpixel = (latitude - latmin/(latmax - latmin)) * NPIX[0]
  ypixel = (longitude - longmin/(longmax - longmin)) * NPIX[1]
  return int(ypixel), int(xpixel)

def gridloop(key, picture, decimation, maxregions, granularity, timestamp, outfile = None):

  if outfile is not None:
    of = file(outfile, 'w')
  inpixelarray = picture.dataset.ReadAsArray()
  pixelarray = numpy.ma.masked_array(inpixelarray, numpy.zeros(inpixelarray.shape))
  origarray = pixelarray
  pixelarray = zoom(pixelarray, ZOOM, cval = numpy.mean(pixelarray))
  pixelarray = (pixelarray.astype(numpy.int)/decimation)*float(decimation)
  png(origarray, fileName = key + '.png')
  png(pixelarray, fileName = key + 'decimated.png') 
  sys.stderr.write('Number of Elevation Values %i\n'%sum(numpy.bincount(pixelarray.flatten().astype('int')) != 0 ))
  if True:
    roll_plus_one = numpy.roll(pixelarray, 1, axis=0), numpy.roll(pixelarray, 1, axis= 1)
    roll_minus_one = numpy.roll(pixelarray, -1, axis=0), numpy.roll(pixelarray, -1, axis= 1)
    avgHeight = (pixelarray + roll_plus_one[0] + roll_minus_one[0] + roll_plus_one[1] + roll_minus_one[1])/5.
    avg10x10 = scipy.signal.convolve2d(pixelarray, numpy.ones((10,10))/100, 'valid')
    try:
      avg40x40 = scipy.signal.convolve2d(pixelarray, numpy.ones((40,40))/1600, 'valid')
    except:
      avg40x40 = numpy.arange(25).reshape(5,5)
    grad_plus = numpy.array([roll_plus_one[0] - pixelarray, roll_plus_one[1] - pixelarray], dtype = numpy.int64)
    grad_minus = numpy.array([pixelarray - roll_minus_one[0], pixelarray - roll_minus_one[1]], dtype=numpy.int64)
    grad_plus_long = numpy.array([numpy.roll(avgHeight, granularity, axis=0) - avgHeight, numpy.roll(avgHeight, granularity, axis=1) - avgHeight], dtype = numpy.int64)
    grad_minus_long = numpy.array([avgHeight - numpy.roll(avgHeight, -granularity, axis=0), avgHeight - numpy.roll(avgHeight, -granularity, axis=1)], dtype=numpy.int64)
    avgGradient = (grad_plus + grad_minus)/2.
    curvature = (grad_plus_long - grad_minus_long)/2.
    avgGradientMagnitude = avgGradient[0]**2 + avgGradient[1]**2
    curvatureMagnitude = curvature[0]**2 + curvature[1]**2
    FLATGRADCUT = numpy.percentile(avgGradientMagnitude, 60.) 
    NEGMAXCURV =  0
    POSMINCURV = 0
    # classify pixel as concave up/down and whether it is roughly flat or not.
    poscurv = numpy.min([curvature[0], curvature[1]], 0) >= POSMINCURV
    negcurv = numpy.max([curvature[0], curvature[1]], 0) <= NEGMAXCURV
    mincurv = numpy.min([curvature[0], curvature[1]], 0)
    maxcurv = numpy.min([curvature[0], curvature[1]], 0)
    flat = (avgGradientMagnitude < FLATGRADCUT)
    regions = pixelarray.copy()
    vetoregions = pixelarray.copy()
  for ipass in range(maxregions):
    global_min_ind = numpy.unravel_index(numpy.argmin(pixelarray), pixelarray.shape)
    global_min = numpy.min(pixelarray[:,:])
    global_max = numpy.max(pixelarray)
    TAGVALUE = math.ceil(global_max + 1)
    if ipass==0:
      ORIGTAGVALUE = TAGVALUE
      report(sys.stderr, key, avgGradientMagnitude, avgGradient, curvature, poscurv, negcurv, flat, FLATGRADCUT, curvatureMagnitude)
    else:
      regions = numpy.maximum(regions, zoom(zoom(regions, 0.8), 1.25))
    global_min_ind = numpy.unravel_index(numpy.argmin(regions), regions.shape)
    global_min = numpy.min(regions[:,:])
    covered = {}
    lastrow = False
    startrow = 0    
    for iIteration in range(10):
      if iIteration % 2:
        # going up
        iterations = xrange(irow - 2, -1, -1)
      else:
        # going down
        iterations = xrange(startrow, regions.shape[0])
      for irow in iterations:
        if len(covered) > 0: # proxy for having found a starting/seed set of pixels to grow region from.
          if iIteration % 2 :
            # going up!
            prior = regions[irow + 1,:]
            gradrow = grad_plus[0][irow + 1, : ]
            subtended = numpy.logical_and(gradrow >= 0, prior == TAGVALUE)
          else:
            # going down.
            prior = regions[irow - 1,:]
            gradrow = grad_minus[0][irow - 1, :]
            subtended = numpy.logical_and(gradrow <= 0, prior == TAGVALUE)
          row = regions[irow,:]
          subtended = numpy.logical_and(subtended, row < ORIGTAGVALUE)
          vetorow = vetoregions[irow,:]           
          if lastrow and any(subtended):
            lastfound = maxfound = irow
            lastrow = True
            # extend to flat/rising cells subtended by ones found in prior row.            
            row[subtended] = TAGVALUE
            vetorow[subtended] = 0
            # stretch new pixels left and right if they have room.
            # I think this would be faster if there is a separate loop for the rightward stretches
            # and one for the leftward stretches.
            stopIndex = stretch(row, vetorow, grad_minus [1][irow,:], grad_plus [1][irow,:], TAGVALUE, ORIGTAGVALUE)
            covered[irow] = row_min_ind, stopIndex
          else:
            startrow = irow
            break
        else:
          row = regions[irow,:]
          if any(row==global_min):
            lastfound = maxfound = irow
            lastrow = True
            row_min_ind = numpy.unravel_index(numpy.argmin(row), row.shape)[0] # first pixel in this row with value=global_min
            if True:
              downhill = numpy.logical_or(grad_plus [1][irow, row_min_ind + 1 : ] > 0, row[ row_min_ind + 1 : ] >= ORIGTAGVALUE)
              if any(downhill):
                stretchright = row_min_ind + min(numpy.where(downhill)[0]) + 1
              else:
                stretchright = len(row)
              # here, downhill is the set of pixels to the left that are going downhill relative to this cell or have been previously tagged.
              downhill = numpy.logical_or(grad_minus [1][irow,  1 : row_min_ind ] < 0, row[ 1 : row_min_ind ]>=ORIGTAGVALUE)
              # stretchleft is the nearest pixel to the left that is going downhill or has been previously tagged.
              if any(downhill):
                stretchleft = max(numpy.where(downhill)[0]) + 1 # nb: need to add 1 because downhill excludes first column.
              else:
                stretchleft = 0
              covered[irow] = row_min_ind, stretchright
              regions[irow, stretchleft + 1 : stretchright ] = TAGVALUE        
              vetoregions[irow, stretchleft + 1 : stretchright ] = TAGVALUE        
            else:
              stopIndex = row_min_ind + numpy.argmax(grad_minus[1][irow, row_min_ind + 1:] > 0) + 1
              covered[irow] = row_min_ind, stopIndex
              regions[irow, row_min_ind:stopIndex + 1] = TAGVALUE        
              vetoregions[irow, row_min_ind:stopIndex + 1] = TAGVALUE        
      emptyrighthand = numpy.logical_and(numpy.roll(regions, -1, 1) == TAGVALUE, regions < ORIGTAGVALUE)
      emptylefthand = numpy.logical_and(numpy.roll(regions,  1, 1) == TAGVALUE, regions < ORIGTAGVALUE)
      emptyabove = numpy.logical_and(numpy.roll(regions, -1, 0) == TAGVALUE, regions < ORIGTAGVALUE)
      emptybelow = numpy.logical_and(numpy.roll(regions,  1, 0) == TAGVALUE, regions < ORIGTAGVALUE)
      solopixel = numpy.logical_and(numpy.logical_and(emptyrighthand, emptylefthand), numpy.logical_and(emptyabove, emptybelow))
      regions[solopixel] = TAGVALUE
      pixelarray[regions == TAGVALUE] = TAGVALUE
      if iIteration == 1 and len(covered) < 3:
        break
    regionsize = (regions==TAGVALUE).astype('int').sum()
    if regionsize >= int(.01*regions.shape[0]*regions.shape[1]):
      png(pixelarray, fileName = key + 'regions'+ str(ipass) + '.png', minRadiance=numpy.min(pixelarray), maxRadiance=TAGVALUE)
      png(pixelarray, fileName = key + 'classifiedregions'+ str(ipass) + '.png', minRadiance=ORIGTAGVALUE, maxRadiance=TAGVALUE)
      png2(vetoregions, vetoregions, vetoregions,  fileName = key + 'coloredregions' + str(ipass) + '.png') 
  pixelarray[pixelarray < ORIGTAGVALUE] = ORIGTAGVALUE -1
  png(pixelarray, fileName = key + 'classifiedregions.png', minRadiance=ORIGTAGVALUE - 2, maxRadiance=TAGVALUE+1)
  totpix = pixelarray.shape[0] * pixelarray.shape[1]
  for v in range(int(ORIGTAGVALUE), int(TAGVALUE) + 1):
    iregions = regions.astype('int')
    basin = pixelarray[pixelarray==v]
    try:
      basinmin = numpy.where(pixelarray==numpy.min(basin))
      fraccoverage = float((pixelarray==v).astype('int').sum())/totpix
      if fraccoverage > .01:
        sys.stderr.write('Region %i , Coverage %5.2f %% , Northernmost Min %i %i\n'%(v, fraccoverage*100, basinmin[0][0], basinmin[1][0]))
    except:
      print v, (regions==v).astype('int').sum(), (pixelarray==v).astype('int').sum()

def main(inputStream, decimation, maxregions, granularity, datadir='ASTER_data'):
  if not isinstance(inputStream, str):
    # this is left over from handling serialized hyperion/ali images. Left for subsequent parallel evolution.
    while True:
      line = inputStream.readline()
      if not line: break
      # get the Level-1 image
      try:
        picture = L1GPicture(line)
      except IOError:
        continue
  else:    
    picture = asterPicture(os.path.join(datadir, inputStream))
    # granule = 'ASTGTM2_S19E026'
    # dataset = gdal.Open(os.path.join('ASTER_data', inputStream), GA_ReadOnly).ReadAsArray()
    # picture = asterPicture(gdal.Open(os.path.join('ASTER_data', inputStream), GA_ReadOnly))
    # png(picture, fileName=granule+'.png')
    key = inputStream.split('_dem.tif')[0]
    timestamp = None
    gridloop(key, picture, decimation, maxregions, granularity, timestamp)

if __name__=="__main__":
  usage = 'usage %prog [options] file'
  version = "%prog 0.1 alpha"
  options_list = [
    make_option('-d','--decimation', default = 1, help = 'Coursen height per pixel by factor 1/decimation. Default is 1 (no decimation'),
    make_option('-m','--maxregions', default = 2, help = 'Maximum number of flood basins to look for (default 2)'),
    make_option('-g','--granularity', default = 5, help = 'Granularity: Number of pixels over which each lever arm of curvature is calculated. Default is 5.'),
    ]
  parser = OptionParser(usage = usage, version = version, option_list = options_list)
  (opt, args) = parser.parse_args()
  try:
    decimation = int(opt.decimation)
  except:
    sys.stderr.write('Invalid value for decimation option\n')
    sys.exit(1)
  try:
    granularity = int(opt.granularity)
  except:
    sys.stderr.write('Invalid value for granularity option\n')
    sys.exit(1)
  try:
    maxregions = int(opt.maxregions)
  except:
    sys.stderr.write('Invalid value for maxregins option\n')
    sys.exit(1)
  if len(args)==1:
    main(args[0], decimation, maxregions, granularity)
  else:
    main(sys.stdin, decimation, maxregions, granularity)
