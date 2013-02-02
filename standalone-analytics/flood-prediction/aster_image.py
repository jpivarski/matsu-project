#!/usr/bin/env python
#hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -mapper /home/svejcik/mapper.py -file ~/reducer.r  -reducer ~/reducer.r  -input /user/cbennett/EO1H0230322012040110P0_HYP_L1G.serialized   -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -cmdenv PYTHONPATH=/opt/lib/python -file ~/mapper.py -file ~/modified_api.py -file ~/shipspectra.csv -file ~/spacetimeIdentity.py  -jobconf mapred.reduce.tasks=7 -output output-20120919-newmapper-exit
#hadoop jar /opt/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar -mapper /home/svejcik/mapper.py -file ~/reducer.py  -reducer ~/reducer.py  -input /user/cbennett/EO1H0230322012040110P0_HYP_L1G.serialized   -cmdenv LD_LIBRARY_PATH=/opt/avrocpp-1.7.1/lib -cmdenv PYTHONPATH=/opt/lib/python -file ~/mapper.py -file ~/tagships.py -file ~/modified_api.py -file ~/shipspectra.csv -file ~/spacetimeIdentity.py  -jobconf mapred.reduce.tasks=7 -output output-20120928-newmapper-exit

from aster_api import *
from spacetimeIdentity import *
from optparse import OptionParser, make_option
import scipy.signal
import math
import numpy
import sys
import os

TAGVALUE = 50

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
  #pixelarray.mask[:500,-500:] = True
  focus = (690,699) # locate a particular pixel with a pair of perpendicular lines
  foci = [(2800,2330), (2838,2400),(2850,2470)]
  origarray = pixelarray
  pixelarray = pixelarray.astype(numpy.int)/decimation
  #
  # calculate gradient and curvature vectors and their magnitudes squared.
  # grad_plus:
  # as calculated here, the first coordinate is h[row -1, column] - h[row, column]
  # grad_minus
  # the first coordinate is h[row, column] - h[row + 1, column].
  # For both:
  # a slope that increases with increasing row (going 'down' the image) will have a *negative* gradient.
  # a slope that increases with increasing column (going 'right' in the image) will have a negative gradient.
  # Images produced:
  # (n.b. curvature is calculated from 'long' lever arm)
  # raw
  # decimated
  # averaged
  # |avgGrad|
  # avgGrad[lat], avgGrad[lng]
  # avgGrad[lat]/|avgGrad|, avgGrad[lng]/|avgGrad|
  # |c|
  # c[lat], c[lng], max(c[lat], c[lng]), min(c[lat], c[lng])
  # c[lat]/|c|, c[lng]/|c|
  # classification images (generally binary-valued, 0 or 1):
  # Flat 
  # Flat & not negative curvature
  # Flat & negative curvature
  # Flat & postiive curvature
  png(origarray, fileName = key + '.png') #, lines=foci)
  png(pixelarray, fileName = key + 'decimated.png') 
  for ipass in range(maxregions):
    roll_plus_one = numpy.roll(pixelarray, 1, axis=0), numpy.roll(pixelarray, 1, axis= 1)
    roll_minus_one = numpy.roll(pixelarray, -1, axis=0), numpy.roll(pixelarray, -1, axis= 1)
    avgHeight = (pixelarray + roll_plus_one[0] + roll_minus_one[0] + roll_plus_one[1] + roll_minus_one[1])/5.
    avg10x10 = scipy.signal.convolve2d(pixelarray, numpy.ones((10,10))/100, 'valid')
    avg40x40 = scipy.signal.convolve2d(pixelarray, numpy.ones((40,40))/1600, 'valid')
    png(avg40x40, fileName = key + 'averaged40x40.png')
    png(avg10x10, fileName = key + 'averaged10x10.png')
    png(avgHeight, fileName = key + 'averaged.png')
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
    nflat = flat.astype('int').sum()
    nNegcurv = negcurv.astype('int').sum()
    nPoscurv = poscurv.astype('int').sum()
    nNonNegativeFlat = (~negcurv & flat).astype('int').sum()
    nNegativeFlat = (negcurv & flat).astype('int').sum()
    nPositiveFlat = (poscurv & flat).astype('int').sum()
    totPixels = flat.size
    if ipass==0:
      # One could re-calculate this metadata after each basin is identified, but here we only do it once for the entire granule.
      # dump some summary info on the avg gradient magnitude
      sys.stderr.write('.01 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,.01))
      sys.stderr.write('.1 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,.1))
      sys.stderr.write('1 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,1.))
      sys.stderr.write('10 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,10.))
      sys.stderr.write('90 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,90.))
      sys.stderr.write('99 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,99.))
      sys.stderr.write('100 Percentile Avg Gradient Magnitude: %f\n'% numpy.percentile(avgGradientMagnitude,100.))
      sys.stderr.write('Minimum Avg Gradient Magnitude %f\n'%numpy.min(avgGradientMagnitude))
      sys.stderr.write('Maximum Avg Gradient Magnitude %f\n'%numpy.max(avgGradientMagnitude))
      sys.stderr.write('Median Avg Gradient Magnitude %f\n'% numpy.median(avgGradientMagnitude))
      sys.stderr.write('Cut on Avg Gradient Defining Flat: %f \n'%FLATGRADCUT)
      sys.stderr.write('Total Pixels %i\n'%totPixels)
      sys.stderr.write('Note that Flat, Negative, and Bowl classifications are not neccesarily mutually exclusive!\n')
      sys.stderr.write('Number of Flat Pixels: %i ( %f %% ) \n'%(nflat, ( 100. * nflat )/totPixels))
      sys.stderr.write('Hill-like pixels : %i ( %f %% ) \n'%(nNegcurv, ( 100. * nNegcurv )/totPixels))
      sys.stderr.write('Bowl-like pixels : %i ( %f %% ) \n'%(nPoscurv, ( 100. * nPoscurv )/totPixels))
      sys.stderr.write('Flat and not Hill-like pixels: %i ( %f %% ) \n'%(nNonNegativeFlat, ( 100. * nNonNegativeFlat )/totPixels))
      sys.stderr.write('Flat and Hill-like pixels: %i ( %f %% ) \n'%(nNegativeFlat, ( 100. * nNegativeFlat )/totPixels))
      sys.stderr.write('Flat and Bowl-like pixels: %i ( %f %% ) \n'%(nPositiveFlat, ( 100. * nPositiveFlat )/totPixels))
      sys.stderr.write('Minimum Curvature %i \n'%numpy.where(numpy.bincount(curvatureMagnitude.flatten().astype('int')) != 0)[0][0])
      sys.stderr.write('Maximum Curvature %i \n'%(len(numpy.bincount(curvatureMagnitude.flatten().astype('int'))) -1 ))
      sys.stderr.write('Number of Curvature Values %i\n'%sum(numpy.bincount(curvatureMagnitude.flatten().astype('int')) != 0 ))
      sys.stderr.write('Minimum Gradient %i\n'%numpy.where(numpy.bincount(avgGradientMagnitude.flatten().astype('int')) != 0)[0][0] )
      sys.stderr.write('Maximum Gradient %i\n'%(len(numpy.bincount(avgGradientMagnitude.flatten().astype('int'))) -1 ))
      sys.stderr.write('Number of Gradient Values %i\n'%sum(numpy.bincount(avgGradientMagnitude.flatten().astype('int')) !=0 ))
      # gradient:
      png(avgGradientMagnitude, fileName = key + 'gradient.png')
      png(avgGradient[0], fileName = key + 'AverageGradientLatitude.png')
      png(avgGradient[1], fileName = key + 'AverageGradientLongitude.png')
      png(avgGradient[0]/avgGradientMagnitude, fileName = key + 'AverageGradientDegSouth.png')
      png(avgGradient[1]/avgGradientMagnitude, fileName = key + 'AverageGradientDegEast.png')
      # curvature
      png(curvatureMagnitude, fileName = key + 'curvature.png', line=focus)
      png(numpy.max([curvature[0], curvature[1]], 0), fileName = key + 'MaxCurvature.png') 
      png(numpy.min([curvature[0], curvature[1]], 0), fileName = key + 'MinCurvature.png') 
      png(curvature[0], fileName = key + 'LatitudeCurvature.png')
      png(curvature[1], fileName = key + 'LongitudeCurvature.png')
      png(curvature[0]/numpy.sqrt(curvatureMagnitude), fileName = key + 'CurvatureDegSouth.png')
      png(curvature[1]/numpy.sqrt(curvatureMagnitude), fileName = key + 'CurvatureDegEast.png')
      # classifying regions
      png(flat.astype('float'), minRadiance=0, maxRadiance=1, fileName= key + 'flat.png')
      png((~negcurv & flat).astype('float'), fileName = key + 'FlatNotHill.png')
      png((negcurv & flat).astype('float'), fileName = key + 'FlatHill.png')
      png((poscurv & flat).astype('float'), fileName = key + 'FlatBowl.png')
    global_min_ind = numpy.unravel_index(numpy.argmin(pixelarray), pixelarray.shape)
    global_min = numpy.min(pixelarray)
    global_max = numpy.max(pixelarray)
    TAGVALUE = global_max + 1
    regions = pixelarray.copy()
    vetoregions = pixelarray.copy()
    # first pass through
    covered = {}
    lastrow = False
    for iIteration in range(5):
      if iIteration % 2:
        # going up
        iterations = xrange(irow - 2, -1, -1)
      else:
        # going down
        iterations = xrange(1, regions.shape[0])
      for irow in iterations:
        print irow
        if len(covered) > 0: # proxy for having found a starting/seed set of pixels to grow region from.
          if iIteration % 2 :
            # going up!
            print 'GOING UP FROM %i\n'%irow
            prior = regions[irow + 1,:]
            gradrow = grad_plus[0][irow + 1, : ]
            subtended = numpy.logical_and(gradrow >=0, prior==TAGVALUE)
          else:
            # going down.
            prior = regions[irow - 1,:]
            gradrow = grad_minus[0][irow - 1, :]
            subtended = numpy.logical_and(gradrow <=0, prior==TAGVALUE)
          row = regions[irow,:]
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
            emptyrighthand = numpy.logical_and(numpy.logical_and(numpy.roll(row, -1)!=TAGVALUE, row==TAGVALUE), grad_minus[1][irow,:] <= 0)
            emptylefthand = numpy.logical_and(numpy.logical_and(numpy.roll(row, 1)!=TAGVALUE, row==TAGVALUE), grad_plus[1][irow,:] >= 0)
            for taggedpixel in numpy.where(numpy.logical_or(emptyrighthand, emptylefthand))[0]:
              row_min_ind = taggedpixel
              # going to the right, is there any pixel that is either already part of the region or starts going downhill?
              downhill = numpy.logical_or(grad_plus[1][irow, row_min_ind + 1:] > 0, row[row_min_ind + 1:]==TAGVALUE)
              if any(downhill):
                stretchright = taggedpixel + min(numpy.where(downhill)[0]) + 1
              else:
                stretchright = taggedpixel + 1
              # here, downhill is the set of pixels to the left that are going downhill relative to this cell or have been previously tagged.
              downhill = numpy.logical_or(grad_minus[1][irow, 1 :row_min_ind] < 0, row[1 :row_min_ind]==TAGVALUE)
              # stretchleft is the nearest pixel to the left that is going downhill or has been previously tagged.
              if any(downhill):
                stretchleft = max(numpy.where(downhill)[0]) + 1 # nb: need to add 1 because downhill excludes first column.
              else:
                stretchleft = taggedpixel - 1
              row[stretchleft + 1 : stretchright] = TAGVALUE
              vetorow[stretchleft + 1 : stretchright] = TAGVALUE
              stopIndex = stretchright
            covered[irow] = row_min_ind, stopIndex
          else:
            break
        else:
          row = regions[irow,:]
          if any(row==global_min):
            print 'looking for new minimum of ',global_min
            # THIS IS A TEMPORARY KLUGE TO TEST WIDER REGION. NEED TO PICK UP INITIALLY NON-CONTIGUOUS MINIMA BY ITERATION BACK UP
            # SINCE ONES THAT ARE 'GLOBALLY' NON-CONTIGUOUS NEED TO BE TAGGED DIFFERENTLY!
            #regions[irow, row==global_min] = TAGVALUE        
            lastfound = maxfound = irow
            lastrow = True
            row_min_ind = numpy.unravel_index(numpy.argmin(row), row.shape)[0] # first pixel in this row with value=global_min
            print 'First row, column of minimum:', irow, row_min_ind, row[row_min_ind - 3: row_min_ind + 2]
            stopIndex = row_min_ind + numpy.argmax(grad_minus[1][irow, row_min_ind + 1:] > 0) + 1
            print 'minimum stop at:',stopIndex, row[stopIndex - 3:stopIndex + 4]
            covered[irow] = row_min_ind, stopIndex
            regions[irow, row_min_ind:stopIndex + 1] = TAGVALUE        
            vetoregions[irow, row_min_ind:stopIndex + 1] = TAGVALUE        
      emptyrighthand = numpy.logical_and(numpy.roll(regions, -1, 1)==TAGVALUE, regions!=TAGVALUE)
      emptylefthand = numpy.logical_and(numpy.roll(regions,  1, 1)==TAGVALUE, regions!=TAGVALUE)
      emptyabove = numpy.logical_and(numpy.roll(regions, -1, 0)==TAGVALUE, regions!=TAGVALUE)
      emptybelow = numpy.logical_and(numpy.roll(regions,  1, 0)==TAGVALUE, regions!=TAGVALUE)
      solopixel = numpy.logical_and(numpy.logical_and(emptyrighthand, emptylefthand), numpy.logical_and(emptyabove, emptybelow))
      regions[solopixel] = TAGVALUE
      pixelarray[regions==TAGVALUE]=TAGVALUE
      #pixelarray.mask[regions==TAGVALUE] = True
      if iIteration == 0 and len(covered) < 100:
        print 'region %i too small to be worthwhile.( %i )\n'%(ipass, len(covered))
        break
      #print 'masking!', pixelarray.mask.sum()
    if len(covered) >= 100:
      png(regions, fileName = key + 'regions'+ str(ipass) + '.png', minRadiance=0, maxRadiance=TAGVALUE)
      png2(vetoregions, vetoregions, vetoregions,  fileName = key + 'coloredregions' + str(ipass) + '.png') 

def main(inputStream, decimation, maxregions, granularity):
  if not isinstance(inputStream, str):
    # this is left over from handling serialized hyperion/ali images. Left for subsequent parallel evolution.
    while True:
      line = inputStream.readline()
      if not line: break
      # get the Level-1 image
      try:
        #picture = GeoPictureSerializer.deserialize(line)
        picture = L1GPicture(line)
      except IOError:
        continue
  else:    
    picture = asterPicture(os.path.join('ASTER_data', inputStream))
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
    decimation = float(opt.decimation)
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
