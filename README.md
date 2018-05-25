# Parallel-Implementation-of-exactStorm-algorithm-with-Spark

In the paper "Detecting distance-based outliers in streams of data" of Fabrizio Angiulli and Fabio Fassetti from the Università della Calabria in Italy, an algorithm for distance-based outlier detection in stream of data named exactStorm, is being introduced.

In a few words, this algorithm works with time windows and it aims to spot outliers over each window. It achieves this by keeping in a list the preceding neighbours of a point and by counting the number of neighbours after this point. If for all the time that the point is on the active window the sum of the number of points in the "before" list and the number of "after" points is greater than a given limit, then the point is considered inlier.

A very efficient optimization is that if the count of after neighbours is even or greater than the given limit, then this point is considered a safe inlier and there is no need to check its status on every window change.

In this repo, there is this algorithm implemented in Scala and Spark, but with two major changes:
1. We consider the points coming in batches and not one by one.
2. We parallelize the whole process in order to make the algorithm scalable.  

We use a random generation and the Spark Streaming to create a dummy stream and we do the outlier detection over this stream.

Link of the paper: https://dl.acm.org/citation.cfm?id=1321552
