# pig is high level version of mapreduce
# can be faster than mr when it runs on tez (DAG)
# extensible through UDFs

# grunt the cli repl interpreter, other wise scripts or ambari/hue

# load data into a new relation (bag of tuples), give it a schema (schema on read)
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

# a pig relation is a bag of tuples
# a pig relation is similar to a table in a RDB, where the tuples in the bag correspond to the rows in a table
# unlike a relational table, however, pig relations don't require that every tuple contain the same number of fields in the same positin have the same type

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

DUMP metadata;

# creating a relation from another relation; foreach/generate

metadata = LOAD '' USING PigStorage('|') AS (movidID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MM-yyyy')) AS releaseTime;


# creates a bag that contains tuples of all the rows (like a reduce operation)
ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

DUMP avgRatings;

# describe the schema of the relation
DESCRIBE ratings;
DESCRIBE ratingsByMovie;
DESCRIBE avgRaings;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

fiveStarswithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;
DESCRIBE fiveStarsWithData;
DUMP fiveStarsWithData;

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;
DUMP oldestFiveStarMovies;


