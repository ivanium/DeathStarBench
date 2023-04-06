import requests
import json
import optparse
import time

def worker(movies):
  language="en-US"
  names = []
  for movie in movies:
    names.append(str(movie["id"]))
  return names

def main():
  parser = optparse.OptionParser()
  parser.add_option("--rfile", type="string", dest="rfile")
  parser.add_option("--wfile", type="string", dest="wfile")
  (options, args) = parser.parse_args()
  with open(options.rfile, "r") as movie_file:
    movies = json.load(movie_file)
    ids = worker(movies)
    with open(options.wfile, "w") as name_file:
      json.dump(ids, name_file, indent=2)

if __name__ == '__main__':
  main()