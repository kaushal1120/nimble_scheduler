import os
import re
from contextlib import contextmanager
from contextlib import ExitStack

class Map():

  #Have no of reducers here
  no_of_reducers = 3

  def getFileNames(self, file_name: str):
    file_names = [(file_name,'r')]
    for i in range(Map.no_of_reducers):
      file_names.append(('reduce_output_'+str(i+1),'a'))
    return file_names


  @staticmethod
  def mapData(file_name: str):
    # First read data from file, then map and then write based on consistent hashing.
    # For conistent hashing we need to cautios as we want to write data to predeined no of files
    # If the file is not there then open the file and append it.

    # Get filenames based on no of reducers and the input file name.
    m = Map()
    file_names = m.getFileNames(file_name)
    print(file_names)
    with ExitStack() as stack:
      files = [stack.enter_context(open(f_name, f_mode)) for f_name,f_mode in file_names]
      r_file = files[0]
      print(r_file)
      for lines in r_file:
        words = re.findall('\w+',lines)
        for word in words:
          # Now consistent hash the word so that it always goes to one file
          o_file = files[(hash(word) % Map.no_of_reducers) + 1]
          o_file.write(word+' '+str(1)+os.linesep)
