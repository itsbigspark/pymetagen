SELECT title, release_year, imdb_score
FROM films
WHERE release_year > 1990
ORDER BY imdb_score DESC
