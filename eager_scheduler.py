#!/usr/bin/python

from multiprocessing import Pool
import functools, time, json, sys

class EagerScheduler:
    #global step dependency model
    step_dependency_model = {}

    #Stores total cost of running all tasks individually
    total_cost=0.0
    #To compute JCT
    job_start_time=sys.float_info.max
    job_end_time=0.0

    #Code to execute given task for given analytics job
    def exec_task(self,stg_idx,task_idx,exec_file):
        task = self.step_dependency_model['stages'][stg_idx]['tasks'][task_idx]
        #Executes each step of the task in that order
        for step in task['steps']:
            #Preparing to execute function from different python class
            obj = getattr(__import__(exec_file),exec_file)
            #Preparing arguments for execution
            arg_dict = {}
            for i in range(len(step['step_func_arg_keys'])):
                arg_dict[step['step_func_arg_keys'][i]] = step['step_func_arg_vals'][i]
            print('Scheduling task_id: ', task['task_id'], 'of', exec_file, 'at', str(time.time()))
            #Computing step execution time
            start = time.time()
            C,P = getattr(obj, step['step_func_name'])(arg_dict)
            end = time.time()
            #Computing cost and star and end times for JCT calculation
            step['P'] = P
            step['C'] = C
            print(P,C)
            self.total_cost += end-start
            self.job_end_time = max(self.job_end_time,end)
            self.job_start_time = min(self.job_start_time,start)
        return end

    def smap(self,f):
        return f()

    def schedule(self):
        with open('2_stage_map_reduce.json', 'r') as f:
            self.step_dependency_model = json.load(f)
        print('Read step dependency model successfully')
        print(self.step_dependency_model)

        #Eager scheduler
        scheduled = []
        for i in range(0,len(self.step_dependency_model['stages'])):
            stage = self.step_dependency_model['stages'][i]
            for j in range(0,len(stage['tasks'])):
                scheduled.append(functools.partial(self.exec_task,i,j,stage['exec_file']))

        print(scheduled)

        #schedule all stages at once
        with Pool() as pool:
            res = pool.map(self.smap, scheduled)
            print(res)

        print('Total cost is:', self.total_cost)
        print('JCT given start time 0.0', self.job_end_time-self.job_start_time)

        # Serializing json and writing to file
        json_object = json.dumps(self.step_dependency_model, indent = 4)
        with open("2_stage_map_reduce_eager.json", "w") as outfile:
            outfile.write(json_object)

if __name__ == '__main__':
    x = EagerScheduler()
    x.schedule()