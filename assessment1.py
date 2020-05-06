from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRatings(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_movies, combiner=self.combine_count_rating
                ,reducer=self.reducer_count_ratings
            ),
            MRStep(
                reducer=self.reduce_sort_ratings
            )
        ]
        
    #get movies
    def mapper_get_movies(self, _, line):
        (userID,MovieID,rating,timestamp) = line.split('\t')
        yield MovieID, 1
        #where 1 is the amount of time iterated over input
    
    #combining the key(movie) with value(sum of rating)
    def combine_count_rating(self, movie, rating):
        yield movie, sum(rating)
    
    #makes iterable int + no extra output
    def reducer_count_ratings(self, movie, rating):
        yield None, (sum(rating), movie)

    #sorting from most rated to least rated
    def reduce_sort_ratings(self, _, total_ratings):
        for rating, movie in sorted(total_ratings, reverse=True):
            yield (int(rating), movie)

if __name__ == '__main__':
    MovieRatings.run()