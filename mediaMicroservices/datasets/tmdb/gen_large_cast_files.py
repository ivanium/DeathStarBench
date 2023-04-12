import requests
import json
import optparse
import time
import copy

all_num = 0


def worker(casts):
  language="en-US"
  extended_casts = []
  for cast in casts:
    for i in range(100):
        new_cast = copy.deepcopy(cast)
        new_cast["id"] += i * 10000000
        extended_casts.append(new_cast)
  return extended_casts

def main():
  parser = optparse.OptionParser()
  parser.add_option("--rfile", type="string", dest="rfile")
  parser.add_option("--wfile", type="string", dest="wfile")
  (options, args) = parser.parse_args()
  with open(options.rfile, "r") as cast_file:
    casts = json.load(cast_file)
    out = worker(casts)
    with open(options.wfile, "w") as out_file:
      json.dump(out, out_file, indent=2)

if __name__ == '__main__':
  main()



