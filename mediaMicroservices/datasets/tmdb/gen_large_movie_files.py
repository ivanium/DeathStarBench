import requests
import json
import optparse
import time
import copy

all_num = 0


# 2230000

def worker(movies):
  language="en-US"
  names = {}
  extended_movies = []
  for movie in movies:
    if (movie["title"] not in names):
        names[movie["title"]] = True
        for i in range(100):
            new_movie = copy.deepcopy(movie)
            new_movie["id"] += i * 10000000
            new_movie["title"] = new_movie["title"]+"_"+str(i)
            for cast in new_movie["cast"]:
                cast["id"] += i * 10000000
            extended_movies.append(new_movie)
  return extended_movies

def main():
  parser = optparse.OptionParser()
  parser.add_option("--rfile", type="string", dest="rfile")
  parser.add_option("--wfile", type="string", dest="wfile")
  (options, args) = parser.parse_args()
  with open(options.rfile, "r") as movie_file:
    movies = json.load(movie_file)
    names = worker(movies)
    with open(options.wfile, "w") as name_file:
      json.dump(names, name_file, indent=2)

if __name__ == '__main__':
  main()



