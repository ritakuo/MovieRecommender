# MovieRecommender
The Netflix Prize was an open competition for the best collaborative filtering algorithm to predict user ratings for films, based on previous ratings without any other information about the users or films

In this project, we use Item collaborative filterting on the similarity between items calculated using people’s rating of those items.


The steps are follows:

Step 1: Data Pre-processing

code: DataDividedByUser.java

taking the input file provided by Netflix, divide data by user_id, and merge data for the same user_id

input:
user_id movid_id rating
101     1         9
101     2         8

output:
Key: user_id    Value: movie_id:rating
      101                1:9, 2:8 

Step 2: Build Co-occurance matrix 

code: CoOccurrenceMatrixGenerator.java

count how many times 2 movie have appeared together 

input:
Key: user_id    Value: movie_id:rating
      101                1:9, 2:8 

output:
key: movie1:movie2  value:# of co-occurance
          1:2               1
         
Step 3: Based on user's rating for the movie he saw, predict his score on movies he hasn't seem.
code: Multiplication.java

3.1 Normalize co-occurance matrix

3.2 predict the score

mapper : rating_for_movie2_based_on_movie1=  (rating_for_movie1 * co-occurance[m1][m2])/totoa_occurance_movie2

reducer: combine all score of movie2 



