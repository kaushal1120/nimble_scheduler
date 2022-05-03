#!/usr/bin/python

from multiprocessing import Pool
from itertools import chain
import heapq
import functools
import time
import json
import json
import sys

class NimbleScheduler:

    #global step dependency model
    step_dependency_model = {}

    #Priority queue ueue to dispath processes from
    scheduled = []

    #Used to terminate dispatcher process
    no_of_tasks = 0

    stage_map = {}


    def exec_task(self,i,j,exec_file):
        #Stores total cost of running all tasks individually
        total_cost=0.0
        #To compute JCT
        task_start_time=sys.float_info.max
        task_end_time=0.0
        #Stage map stage_id -> stage object

        optimal_task_duration = 0.0
        task = step_dependency_model['stages'][i]['tasks'][j]
        
        for step in task['steps']:
            #steps_launched[step['step_id']] = step
            obj = getattr(__import__(exec_file),exec_file)

            #Prepare arguments to task exec
            arg_dict = {}
            for i in range(len(step['step_func_arg_keys'])):
                arg_dict[step['step_func_arg_keys'][i]] = step['step_func_arg_vals'][i]
            print('Scheduling task_id: ', task['task_id'], 'of', exec_file, 'at', str(time.time()))
            
            #Fork new process for task
            start = time.time()
            C,P = getattr(obj, step['step_func_name'])(arg_dict)
            end = time.time()

            #Update P, C, rc, rp, total job cost, optimal step duration, job finish time
            #Update P, C, rc, rp, total job cost, optimal step duration, job finish time
            self.step_dependency_model['stages'][self.stg_idx]['tasks'][self.task_idx]['steps'][0]['P'] = P
            self.step_dependency_model['stages'][self.stg_idx]['tasks'][self.task_idx]['steps'][0]['C'] = C
            #step_discrete_timestamps[step['step_id']] = [start,end]
            #Averaging for better estimates
            step['rc'] = (step['rc'] + C/(end-start))/(task['no_of_runs']+1)
            step['rp'] = (step['rp'] + P/(end-start))/(task['no_of_runs']+1)
            total_cost += end-start
            optimal_task_duration += end-start
            step['d*'] = (step['d*'] + end - start)/(task['no_of_runs'] + 1)
            task_end_time = max(task_end_time,end)
            task_start_time = min(task_start_time,start)

        #Update optimal task duration, no_of_runs
        task['D*'] = (task['D*'] + end - start)/(task['no_of_runs'] + 1)
        task['no_of_runs'] += 1
        return [total_cost, task_end_time, task_start_time]


    def smap(self,f):
        return f()

    def get_optimal_duration(self,stage_id, task_id):
        global step_dependency_model
        task = step_dependency_model['stages'][stage_id]['tasks'][task_id]
        return task['D*']

    def interval_overlaps(self,interval_1,interval_2):
        if interval_1[0] < interval_2[0]:
            if interval_1[1] > interval_2[0]:
                return True
            else:
                return False
        else:
            if interval_2[1] > interval_1[0]:
                return True
            else:
                return False

    #Assuming parent started at 0.0
    def get_step_finish_time(self,step):
        parent_stage = self.stage_map[step['parent'][0:1]]
        step_discrete_intervals = []
        rps = []
        for task in parent_stage['tasks']:
            #Assumption: No two steps of a stage are parents to different steps
            produce_step = task['steps'][len(task['steps'])-1]
            interval = [step_discrete_intervals.append(task['D*']-produce_step['d*']),step_discrete_intervals.append(task['D*'])]
            step_discrete_intervals.append(interval)
            rps.append(produce_step['rp'])
        step_discrete_intervals, rps = (list(t) for t in zip(*sorted(zip(step_discrete_intervals, rps))))
        
        #Compute aggregated rp(t) for each discretized time interval
        step_intervals = []
        new_rps = []
        step_discrete_timestamps = sorted(list(chain.from_iterable(step_discrete_intervals)))
        for i in range(1,step_discrete_timestamps):
            step_intervals.append([step_discrete_timestamps[i-1],step_discrete_timestamps[i]])
            new_rps.append(0)
        for i in range(0,len(step_intervals)):
            for j in range(0,len(step_discrete_intervals)):
                if self.interval_overlaps(step_intervals[i],step_discrete_intervals[j]):
                    new_rps[i] += rps[j]

        #Calculate rac for each discretized time interval
        produced_till_here = 0
        consumed_till_here = 0
        actually_consumed_till_here = 0
        time_passed = 0
        racs = []
        for i in range(0,len(step_intervals)):
            time_passed += step_intervals[i][1] - step_intervals[i][0]
            produced_till_here += (step_intervals[i][1] - step_intervals[i][0])*new_rps[i]
            consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*step['rc']
            if produced_till_here < consumed_till_here:
                racs.append(min(new_rps[i],step['rc']))
            else:
                racs.append(step['rc'])
            actually_consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*racs[i]

        #Calculate optimal step finish time (which only considers its parent step)
        te = step_intervals[len(step_intervals)][1]
        if actually_consumed_till_here < produced_till_here:
            te += (produced_till_here - actually_consumed_till_here)/step['rc']
        
        return 

    #
    def get_optimal_finish_time(self,stage_id,task_id):
        global step_dependency_model
        task = step_dependency_model['stages'][stage_id]['tasks'][task_id]
        t_opt = 0.0
        for step in task['steps']:
            if step['has_parent']:
                t = max(self.get_step_finish_time(task), t_opt + step['d*'])
            else:
                t = t_opt + step['d*']
            t_opt = t
        return t_opt    

    #Computes optimal launch time to schedule task
    def get_optimal_launch_time(self,stage_id,task_id):
        global step_dependency_model
        task = step_dependency_model['stages'][stage_id]['tasks'][task_id]
        D = self.get_optimal_duration(stage_id,task_id)
        Te = self.get_optimal_finish_time(stage_id,task_id)
        task['Te'] = Te
        task['Ts'] = Te - D
        return task['Ts']

    #Dispatches tasks according to their start times
    def dispatcher(self,no_of_tasks):
        print('Dispatcher tasks now')
        start_time = time.time()
        results = []
        while len(results) < no_of_tasks:
            if len(self.scheduled) == 0:
                continue
            while len(self.scheduled) > 0 and self.scheduled[0][0] >= time.time()-start_time:
                processes_to_dispatch = []
                processes_to_dispatch.append(self.scheduled[0][1])
                self.scheduled.pop(0)
            with Pool() as pool:
                results.append(pool.map_async(self.smap, processes_to_dispatch))
            no_of_tasks -= len(processes_to_dispatch)
        print('Scheduling done')
        return results

    def schedule(self):
        global step_dependency_model
        with open('2_stage_map_reduce.json', 'r') as f:
            step_dependency_model = json.load(f)
        print('Read step dependency model successfully')

        #Nimble scheduler
        current_parents = []
        new_parents = []
        no_of_tasks = 0
        results = []

        #Scheduling initial stages with no parents
        for i in range(0,len(step_dependency_model['stages'])):
            stage = step_dependency_model['stages'][i]
            no_of_tasks += len(stage['tasks'])
            self.stage_map[stage['stage_id']] = stage
            if stage['parent'] is None:
                new_parents.append(stage['stage_id'])
                for j in range(0,len(stage['tasks'])):
                    heapq.heappush(self.scheduled, (stage['tasks'][j]['Ts*'], (stage['tasks'][j]['Ts*'],functools.partial(self.exec_task,i,j,stage['exec_file']))))

        total_cost = 0.0
        job_end_time = 0.0
        job_start_time = sys.float_info.max

        #Starting the dispatcher
        #There could be some contention for the queue of processes 'scheduled'
        with Pool() as pool:
            res = pool.map(self.smap, self.scheduled)
            print(res)

        total_cost += sum([res[x][0] for x in range(len(res))])
        job_start_time = min([res[x][2] for x in range(len(res))])

        #Scheduling remaining stages in bfs
        while no_of_tasks > 0:
            for i in range(0,len(step_dependency_model['stages'])):
                stage = step_dependency_model['stages'][i]
                if stage['parent'] in current_parents:
                    new_parents.append(stage['stage_id'])
                    for j in range(0,len(stage['tasks'])):
                        self.get_optimal_launch_time(i,j)                    
                        self.scheduled.append((functools.partial(self.exec_task,i,j,stage['exec_file'],i,j)))
            no_of_tasks -= len(self.scheduled)
            current_parents = new_parents
            new_parents = []
        
        total_cost += sum([res[x][0] for x in range(len(res))])
        job_end_time = max([res[x][1] for x in range(len(res))])

        #Wait on all processes to complete
        results.get()
        print('Total cost is:', self.total_cost)
        print('JCT given start time 0.0', self.task_end_time-self.task_start_time)

        # Serializing json and writing to file
        json_object = json.dumps(step_dependency_model, indent = 4)
        with open("2_stage_map_reduce_lazy.json", "w") as outfile:
            outfile.write(json_object)

if __name__ == '__main__':
    nimble = NimbleScheduler()
    nimble.schedule()