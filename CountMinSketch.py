import math
import mmh3

class CountMin:
    
    def __init__(self,d,e):
        self.delta = d
        self.epsilon = e
        wd = self.compute_width_depth()
        self.depth = wd[0]
        self.width = wd[1]
        self.sketch = []
        for i in range(self.depth):
            self.sketch.append([0] * self.width)
    
    def to_string(self):
        print "Delta: %f\nEpsilon: %f\nDepth: %d\nWidth: %d"%(self.delta,self.epsilon,self.depth,self.width)
        for row in self.sketch:
            print str(row)     

    def compute_width_depth(self):
        """
        Takes two parameters (delta and epsilon), which define the accuracy and error probability of the sketch, and it should return two values (width and depth).
        """
        return [int(round(2/self.delta)),int(round(math.log(1/self.epsilon)))]
    
    def increment(self,item):
        """
        increment the corresponding counter for this item. The hash functions used is up to you, and they can be automatically generated.
        """
        for i in range(len(self.sketch)):
            if type(item) == int:
                index = item % self.width % (i+1)
            else:
                index = mmh3.hash(item) % self.width % (i+1)
            self.sketch[i][index] += 1
    
    def estimate(self,item):
        """
        estimate the count for the given value.
        """
        counts = []
        for i in range(len(self.sketch)):
            if type(item) == int:
                index = item % self.width % (i+1)
            else:
                index = mmh3.hash(item) % self.width % (i+1)
            counts.append(self.sketch[i][index])
        return min(counts)
    
    def merge(self,CountMin):
        """
        make sure l and w are same, otherwise return nothing
        merges a CountMin sketch with the current sketch (inorder to merge sketches across partitions).
        """
        if CountMin.width == self.width and CountMin.depth == self.depth:
            for i in range(len(self.sketch)):
                for j in range(self.width):
                    self.sketch[i][j] += CountMin.sketch[i][j]
        return self
