luso
====
Experiments with TopDoc extraction in Lucene for large result sets.

This is an extremely rough approximation of how Lucene extracts top X documents, sorted by score.
X documents (ints) are collected with their scores (floats) and extracted in sorted order.
This is done with a stated method of collection & extraction in parallel in T threads.

The method 'pq' is a PriorityQueue, which is what Lucene uses if we dig deep enough in the code.
'ip' collects all docs & scores in arrays and sorts with Lucene's InPlace sorter when collection has finished.
'as' collects all docs & scores in arrays of ScoreDocs and sorts with Arrays.sort when collection has finished.

The purpose of this small test is to experiment with the performance characteristics of the different methods
when the amount of hits and the number of threads changes (spoiler: 'pq' is bad when the two values goes up).

Note: The 'ip' and 'as' methods cannot be used as a direct replacement for 'pq' in Lucene as they require
      all hits & scores to be collected, whereas 'pq' is a sliding window collector.


Usage:
    mvn -q exec:java

Examples:
    MAVEN_OPTS=-Xmx1024m mvn -q exec:java -Dexec.args="pq 1 1000 1000000"
    MAVEN_OPTS=-Xmx1024m mvn -q exec:java -Dexec.args="pq 4 1000000"
    MAVEN_OPTS=-Xmx1024m mvn -q exec:java -Dexec.args="ip 4 1000000"

- Toke Eskildsen, te@ekot.dk, 2014
