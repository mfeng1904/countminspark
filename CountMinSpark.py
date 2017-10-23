import sys
import time
import pyspark
import pyspark.streaming
from CountMinSketch import CountMin

N = 10000000
W = 6
P = 18

DELTA = 1e-3
EPSILON = 1e-3


def make_sketch():
    return CountMin(DELTA,EPSILON)
    
def make_sketch_partition(items):
    sketch = make_sketch()
    for i in items:
        sketch.increment(i)
    return [sketch]

def merge_sketches(a,b):
    base = make_sketch()
    base.merge(a)
    base.merge(b)
    return base

def main():
    def merge_and_estimate(rdd):
        sketches = rdd.collect()
        print "accumulating %d sketches from RDD" % len(sketches)
        for sketch in sketches:
            totals.merge(sketch)
        print totals.estimate("#cats")

    """
    Generate multiple streams of data using W workers and P partitions.
    Create a bloomfilter on each of the P partitions.
    """
    totals = make_sketch()
    
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = pyspark.SparkContext("local[2]", "Spark Bloom Filter")
    ssc = pyspark.streaming.StreamingContext(sc, 10)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    socket_stream = ssc.socketTextStream("127.0.0.1", 5555)
    lines = socket_stream.window(10)
    counts = lines.flatMap(lambda text: text.split(" "))\
                  .map(lambda text: text.encode('utf-8'))\
                  .filter(lambda word: word.lower().startswith("#"))\
                  .mapPartitions(make_sketch_partition)\
                  .reduce(merge_sketches)\
                  .foreachRDD(merge_and_estimate)
    ssc.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ssc.stop()

if __name__ == "__main__":
    main()
