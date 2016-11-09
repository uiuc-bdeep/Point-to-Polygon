#     ------------------------------------------------------------------------
#   |                                                                         |
#   |  find Service Line Connection Type                                      |
#   |                                                                         |
#   |  By:                                                                    |
#   |  Yifang Zhang                                                           |                            
#   |  University of Illinois at Urbana Chamapaign                            |
#   |                                                                         |
#     ------------------------------------------------------------------------


##################################################################
## Preliminaries
rm(list=ls())

## This function will check if a package is installed, and if not, install it
pkgTest <- function(x) {
  if (!require(x, character.only = TRUE))
  {
    install.packages(x, dep = TRUE)
    if(!require(x, character.only = TRUE)) stop("Package not found")
  }
}

## These lines load the required packages
packages <- c("readxl", "data.table", "rgdal", "sp", "rgeos", "parallel", "doParallel")
lapply(packages, pkgTest)


##################################################################
## Points to Polygon Function,
## inputs: 
##      point(projected): points
##      poly(projected):  polygons
##      int:              start
##      int:              end
## outputs:
##      data.frame:       result
##                        the result will contains every columne from points.csv and c("polyDist")

p2p_parallel <- function(points, polygons, start, end) 
{
  
  df <- data.frame(points$X.1[start:end])
  df$polyDist <- 0
  
  for(i in start:(end)){
  
    dist <- gDistance(points[i,], polygons) # hausdorff=TRUE will not make things correct
    min_dist <- min(dist)
    df$polyDist[i-start] <- min_dist
    
    ## having progress output on the line ##
    if(i%%100 == 0){
      print(i)
    }
    
  } ## end of for loop ##
  
  return(df)
  
} ## TODO: automatically assign the whole list on four cores!!! due Oct 31st before 3pm

 
##################################################################
## the projection of function
points_raw <- read.csv("~/share/projects/Flint/production/outputs/Flint_Hedonics_1022.csv")
points <- points_raw[which(!is.na(points_raw$PropertyAddressLatitude)),]

shpName <- "~/share/projects/Flint/stores/2015ParcelswConnType/"
shpDir <- path.expand(shpName)
layerName <- "2015ParcelswConnType"

ogrInfo(dsn = shpDir, layer = layerName)
shp_poly <- readOGR(shpDir, layerName)


coordinates(points) <- ~ PropertyAddressLongitude + PropertyAddressLatitude
proj4string(points) <- CRS("+proj=longlat")
points <- spTransform(points, proj4string(shp_poly))
proj4string(points) <- proj4string(shp_poly)

#########################################################################
####                      Single Core Operation                      ####
#########################################################################

ptm <- proc.time()
result1 = p2p_parallel(points=points, polygons=shp_poly, start=(1000), end=(1000+800))
runningTime1 <- proc.time()-ptm

#########################################################################
####                   Multiple Core Operation                       ####
#########################################################################

ptm <- proc.time()
np <- detectCores()
cl <- makeForkCluster(np/2)
clusterExport(cl, c("points", "shp_poly", "p2p_parallel"))
result2 <- parLapply(cl, 1:(np/2), function(i){
  
  packages <- c("readxl", "data.table", "rgdal", "sp", "rgeos", "parallel", "doParallel")
  lapply(packages, pkgTest)
  
  res <- p2p_parallel(points=points, polygons=shp_poly, start=(i*1000), end=(i*1000+200))
  
  return(res)
  })
stopCluster(cl)

## clusterexport(cl, c("funciton", "para"...))
runningTime2 <- proc.time()-ptm



#######################
## for running on Roger
#It works for me if I use a mirror that has the package for our version 
#(I use OH 1, #135 for me, but that can change for other users). 
#You also need to:
#	module load R
#	module load gdal-stack
#	install.packages("rgdal", configure.args=c(rgdal = "proj_include_path=/sw/geosoft/proj4/include 
#					proj_lib_path=/sw/geosoft/proj4/lib --with-proj-share=/sw/geosoft/proj4/share/proj"))
