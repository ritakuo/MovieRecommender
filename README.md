# Netflix Movie Recommender
- The Netflix Prize was an open competition for the best collaborative filtering algorithm to predict user ratings for films, based on previous ratings without any other information about the users or films

## tl&dr
- In this project, we calculate one person's rating for one movie he has not seem and make recommendation for top N movies for him. 
- We use collaborative filtering based on the similarity between items calculated using people’s rating of those items.

## example output
- based on the user rating files for 100 movie, we generate a recommendation list for the user about top 5 movies for them to watch next.
* we predict user id 2351607 is recommend with the following movie in ascending order: 
  * Lilo and Stitch  
  * 7 Seconds  
  * Never Die Alone
  * Mostly Martha
  * What the #$*! Do We Know!?
 
![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-10+at+8.55.08+AM.png)

## how the code works
### 4 mapreduce jobs:
1. Pre-processing of Netflix raw data 
2. Data processing
3. Build co-occurrence matrix
4. Build rating matrix
5. Generate top N recommend list for user based on his view history and rating

### Pre-Processing
[Netflix Prize data | Kaggle](https://www.kaggle.com/netflix-inc/netflix-prize-data/version/1#README) raw data has the following format:
```
MovieID:
UserID, Rating, Date
```
- Movie ID range from 1-17770
- Customer ID range from 1 to 2649429
- Rating from 1-5
- in this project, I don't take account of the dates. 

For prove of concept, I first process the data so I only have user rating data from 100 movie, so my input is 100 files that has the following format
```
UserID, MovieID, Rating
```
![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-08+at+7.09.42+AM.png)

### Job 1: Data Processing 
DataDividerByUser.java
#### input args:
- arg0: input path from pre-processed data (user rating data from 100 movies)
- arg1: output path job1
#### job 1 output format 
```
UserID movieID: rating, movieID: rating, movieID: rating
```
![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-08+at+7.10.26+AM.png)

### Job 2: Build co-occurrence matrix
CoOccurrenceMatrixGenerator.java.    
calculate number of time two movies are seem together.
#### input args:
- arg0: output path from job1 
- arg1: output path for job2

#### job2 output format 
```
movieID: movieID  num_time_seem_together
```

![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-08+at+7.44.21+AM.png)

ex: movieID_100 and movie_ID_1 has been seem together 8 times. 

### Job 3: Build rating matrix
Multiplication.java    
* From user’s rating for movie he has seen, predict his score on movies he hasn’t seem based on the relationship between the 2 movies. 
* normalize the original input by multiply use rating matrix (original input) with co-occurence matrix (job2_output)
    * For example:   
        * user rate movie_1 with rating “3”, movie_1 and movie_2 has co-occurance of 10 times. 
        * movie_2 has total occurance of 121. 
        * so for user1, the likelihood of watching movie_2 is  3*10 / 121

#### input args 
- arg0: output from job2
- arg1: input for job1 (user rating file for 100 movies)
- arg2: output path for job3

#### how job3 works 
1. Build movieRelation map from ouput of job2 
```
input: 
movie_A:movie_B \t num_time_seem_together

output:
{ movie_1 : {movie_1, movie_2, num_time_seem_together}, {movie_1, movie_2, num_time_seem_together}}
```
2. use movieRelationMap to build denominatorMap
```
{ movie_1, totalOccurance}
```
3. mapper job to transform above output to
```
user_id:movie_id dividedScore
```
formula for divided score:
```
divided_score for user on movie_2= (user rating on movie_1) *(num of time movie_1 movie_2 co-ocuurence)/ (number of time movie 2 occure)
```
4. reducer job to get sum
```
user_id movie_id:sum(dividedScore)
```

![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-10+at+8.46.53+AM.png)

### Job4: Generate top n recommend list based on user view history and rating

get recommendation user about movie them have not seem

#### input argeument
- args0: userRating.txt 
```
userId, movieId, rating
```
- args1: movie_titles.txt
```
movieId, movieName
```
- args2: number of recommendation for this user
- args3:user movie relation (input from job3)
```
user_id movie_id:sum(dividedScore)
```
- args4:output path for this job
- args5:name of finalRecommendationResult
#### how job4 works 
1. Build a map for user id and list of movie he/she has seem (alreadyWatchedList)
2. Filter out the alreadyWatchedList from output of job3
3.  Replace movieid with movieName
```
user_name, movieName:Rating
```
![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-10+at+8.51.56+AM.png)
4. Cumston comparator to return top N movie recommendation for user and their predicted rating for those recommended movie 
 
![](https://s3-us-west-2.amazonaws.com/donot-delete-github-image/Screen+Shot+2019-02-10+at+8.55.08+AM.png)
   
