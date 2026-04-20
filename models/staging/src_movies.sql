WITH raw_movies AS (
    Select * From dev.raw.movies
)

Select 
    movieId AS movie_id, 
    title, 
    genres
From raw_movies