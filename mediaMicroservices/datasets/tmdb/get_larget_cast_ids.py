import requests
import json
import optparse
import time

def worker(movies):
  language="en-US"
  names = []
  max_id = 0
  max_movie_id = 0
  for movie in movies:
    max_movie_id = max(max_movie_id, movie["id"])
    for cast in movie['cast']:
        if cast["id"] == "":
            print("Empty!")
        else:
            max_id = max(max_id, cast["id"])
  print(max_id)
  print(max_movie_id)

def main():
  parser = optparse.OptionParser()
  parser.add_option("--rfile", type="string", dest="rfile")
#   parser.add_option("--wfile", type="string", dest="wfile")
  (options, args) = parser.parse_args()
  with open(options.rfile, "r") as movie_file:
    movies = json.load(movie_file)
    worker(movies)
    # with open(options.wfile, "w") as name_file:
    #   json.dump(names, name_file, indent=2)

if __name__ == '__main__':
  main()