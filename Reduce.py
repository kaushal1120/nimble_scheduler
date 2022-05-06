from collections import defaultdict
from time import sleep

class Reduce():

  ''' 
    task_id is important because we need to know which file to pick for reading.
    Remember start_of_file: #####, end_of_file: *****
    So, we keep incrementing for start_of_file and decrement for end_of_file, and we are sure that the first
    word in the file is start of the file, we keep the variable till is reaches 0.
  '''
  @staticmethod
  def reduceData(files: dict()):
    input_file_size = 0
    
    #We might need this or not clarify
    output_file_size = 0
    file_name = files['input_file']
    start_count = 0
    end_count = 0
    words_dict = defaultdict(int)
    with open(file_name, encoding='utf-8') as r_file:
      while True:
        # Remove this sleep if you are running reduce very isolated, currently in eager it does busy waiting 
        # until we are finished writing to the files, busy waiting consumes CPU cycles and hamper the map 
        # process if they are running on the same CPU/core. So be wary carefule. We allow the reduce to sleep
        # for 500ms so that if it doesnt have anything to read just sleep for sometimes and let map make 
        # progress. 
        sleep(0.5)
        for line in r_file:
          if line.strip():
            input_file_size += len(line)
            if line.strip() == "#####":
              start_count += 1
            elif line.strip() == "*****":
              end_count += 1
            else:
              d = line.split(' ')
              if len(d) == 2:
                words_dict[d[0].strip()] += int(d[1].strip())
          
        if start_count - end_count == 0:
            break
    
    # This is the end. We might want to save this data or not, decide later. 
    with open('final', 'a', encoding='utf-8') as file:
      for k in words_dict.keys():
        l = str(k)+' '+str(words_dict[k])+'\n'
        output_file_size += len(l)
        file.write(l)

    input_file_size = (input_file_size/(1024*1024))
    output_file_size = (output_file_size/(1024*1024))

    return [input_file_size,[output_file_size]]
