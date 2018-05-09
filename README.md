# yelp.api-comprehensive-location-query

## In a nutshell
Yelp API (v2) was designed for Apps who just want to query a few nearby business listings. To perform some aggregate statistics, I wanted to pull all of the business listings within a city. The API was not designed for this (It has a result limit of 500.) 

To query all business listings within a given geography, I had to partition the geography into smaller and smaller chunks until I didn't hit the result limit. I partitioned the geography by creating bounding boxes using latitude and longitude. 

This script uses a recursive function to partition the geography when the result limit is hit. Accordingly, this script returns many duplicate business listings, which can be de-dupped once you're sure you've pulled the entire set.

## To use:
- You will need a large volume of [Yelp API queries](https://www.yelp.com/developers), which we got by signing an agreement with Yelp. 
- You will need the 4 lat/long coordinates that bound the city (or geography) that you are querying. I calculated these for every city in the U.S. using the [shape files provided by the Census.](https://www.census.gov/geo/maps-data/data/tiger-line.html)  
