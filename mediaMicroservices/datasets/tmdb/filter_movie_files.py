import requests
import json
import optparse
import time
import copy

all_num = 0


# 2230000

def worker(casts, movies):
  language="en-US"
  filtered_movies = []
  loaded_casts = {}
  added_movie_num = 0

  for i in range(2230000):
    cast = casts[i]
    loaded_casts[cast["id"]] = True
  for movie in movies:
    new_movie = copy.deepcopy(movie)
    new_movie["cast"] = []
    for cast in movie["cast"]:
      if cast["id"] in loaded_casts:
        new_movie["cast"].append(cast)
    if len(new_movie["cast"]) > 0:
      filtered_movies.append(new_movie)
      added_movie_num += 1
    if added_movie_num >= 300000:
      break
  return filtered_movies

def main():
  parser = optparse.OptionParser()
  parser.add_option("--rfile", type="string", dest="rfile")
  parser.add_option("--cfile", type="string", dest="cfile")
  parser.add_option("--wfile", type="string", dest="wfile")
  (options, args) = parser.parse_args()
  with open(options.rfile, "r") as movie_file:
    with open(options.cfile, "r") as cast_file:
      movies = json.load(movie_file)
      casts = json.load(cast_file)
      filtered = worker(casts, movies)
      with open(options.wfile, "w") as filtered_file:
        json.dump(filtered, filtered_file, indent=2)

if __name__ == '__main__':
  main()



