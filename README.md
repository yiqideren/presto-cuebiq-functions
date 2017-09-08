# presto-cuebiq-functions
custom UDFs for PrestoDB.

currently, there are two main sets of functions: 

Hashing functions, created because as of .147 presto doesn't have functions that hash strings and also have strings as return value. among these:
  sha256
  md5
  pseudorandom shuffle

Geometry functions
  -geohash
  -point in polygon
  -haversine distance

to install the plugin, just build the jar with maven and load it in <prestoDir>/plugin/cuebiq.

=======
Functions

 * sha_256 (VARCHAR) -> VARCHAR
 * md_5 (VARCHAR) -> VARCHAR
 * shuffle_string (VARCHAR) -> VARCHAR

 * geohash_encode (DOUBLE,DOUBLE,INTEGER) -> VARCHAR
 * geohash_decode (VARCHAR) -> ARRAY(DOUBLE)
  example: select geohash_decode('u0n2hb185') -> [45.00002145767212, 9.000012874603271]
 * poly_contains(ARRAY(DOUBLE) poly,DOUBLE lng,DOUBLE lat) -> BOOLEAN
  example: select poly_contains(ARRAY[45, 9.5, 45.5,9.5, 45.5,9, 46, 9, 46, 10, 45, 10],45.7,9.7);
  it also works if coordinates' order are swapped (lng first) 
 * haversine(DOUBLE lat1,DOUBLE lng1,DOUBLE lat2,DOUBLE lng2) -> DOUBLE
