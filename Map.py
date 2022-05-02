import os
import re
import sys
from contextlib import contextmanager
from contextlib import ExitStack

class Map():

  #Have no of reducers here
  no_of_reducers = 5

  def getFileNames(self, i_file_name: str):
    file_names = [(i_file_name,'r')]
    for i in range(Map.no_of_reducers):
      file_names.append(('reduce_output'+'_'+str(i+1),'a'))
    return file_names


  '''
    Usage: {'input_file':'plain_text_1', 'output_file':'reduce_output'}
  '''

  @staticmethod
  def mapData(files: dict()):
    # First read data from file, then map and then write based on consistent hashing.
    # For conistent hashing we need to cautios as we want to write data to predeined no of files
    # If the file is not there then open the file and append it.
    # Get filenames based on no of reducers and the input file name.

    input_file_size = 0
    output_file_size = 0

    m = Map()
    file_names = m.getFileNames(files['input_file'])

    with ExitStack() as stack:
      files = [stack.enter_context(open(f_name, f_mode)) for f_name,f_mode in file_names]
      for f in files[1:]:
        f.write('#####\n')
        f.flush()
      r_file = files[0]
      for line in r_file:
        input_file_size += len(line)
        words = re.findall('\w+',line)
        for word in words:
          # Now consistent hash the word so that it always goes to one file
          o_file = files[(hash(word) % Map.no_of_reducers) + 1]
          o_line = word+' '+str(1)+os.linesep
          output_file_size += len(o_line)
          o_file.write(o_line)
      for f in files[1:]:
        f.write('*****')
        f.flush()

    '''
      We need to return the input fize size and output file size for this map
      For, output file size since we do not have a single file we are going to
      sum up the bytes of line and keep a running sum, and return it in MB.
    '''
    input_file_size = (input_file_size/(1024*1024))
    output_file_size = (output_file_size/(1024*1024))
    return [input_file_size, output_file_size]
