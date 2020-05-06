from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRatings(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_movies, reducer=self.reducer_count_ratings)]
    
    #set key as MovieID
    def mapper_get_movies(self, _,line):
        (userID,MovieID,rating,timestamp) = line.split('\t')
        yield MovieID, 1
        #where 1 is the amount of time iterated over input
    
    def reducer_count_ratings(self,key,rating):
        yield key,sum(rating)
  
if __name__ == '__main__':
    MovieRatings.run()