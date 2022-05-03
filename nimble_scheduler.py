#!/usr/bin/python

from multiprocessing import Pool, Manager
from multiprocessing.managers import SyncManager
from queue import PriorityQueue
from itertools import chain
import heapq, functools, time, json, sys, threading

class MyManager(SyncManager):
    pass
MyManager.register("PriorityQueue", PriorityQueue)  # Register a shared PriorityQueue

def Manager():
    m = MyManager()
    m.start()
    return m

class NimbleScheduler:
    #global step dependency model
    step_dependency_model = {}

    #Stage map stage_id -> stage object
    stage_map = {}

    def exec_task(self,stg_idx,task_idx,exec_file):
        #Stores cost of running current task
        cost=0.0
        #To compute JCT
        task_start_time=sys.float_info.max
        task_end_time=0.0

        optimal_task_duration = 0.0
        task = self.step_dependency_model['stages'][stg_idx]['tasks'][task_idx]
        
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
            step['P'] = P
            step['C'] = C
            #step_discrete_timestamps[step['step_id']] = [start,end]
            #Averaging for better estimates
            step['rc'] = (step['rc']*task['no_of_runs'] + C/(end-start))/(task['no_of_runs']+1)
            for i in range(0,len(P)):
                step['rp'][i] = (step['rp'][i]*task['no_of_runs'] + P[i]/(end-start))/(task['no_of_runs']+1)
            cost += end-start
            optimal_task_duration += end-start
            step['d*'] = (step['d*']*task['no_of_runs'] + end - start)/(task['no_of_runs'] + 1)
            task_end_time = max(task_end_time,end)
            task_start_time = min(task_start_time,start)

        #Update optimal task duration, no_of_runs
        task['D*'] = (task['D*']*task['no_of_runs'] + end - start)/(task['no_of_runs'] + 1)
        task['no_of_runs'] += 1
        return [cost, task_end_time, task_start_time, [stg_idx, task_idx, task]]

    def smap(self,f):
        return f()

    def get_optimal_duration(self,stg_id, task_id):
        task = self.step_dependency_model['stages'][stg_id]['tasks'][task_id]
        return task['D*']

    def interval_overlaps(self,interval_1,interval_2):
        if interval_1[0] <= interval_2[0]:
            if interval_1[1] > interval_2[0]:
                return True
            else:
                return False
        else:
            if interval_2[1] > interval_1[0]:
                return True
            else:
                return False

    #Assuming parent started at 0.0, calculates optimal finish time of step
    def get_step_finish_time(self,step):
        parent_step_start_time = sys.float_info.max
        parent_stage = self.stage_map[step['parent'][0:1]]
        #print('Parent stage', parent_stage['stage_id'])
        step_discrete_intervals = []
        rps = []
        for task in parent_stage['tasks']:
            #Assumption: No two steps of a stage are parents to different steps which implies that production from a task
            #is done only from its last step
            produce_step = task['steps'][len(task['steps'])-1]
            parent_step_start_time = min(parent_step_start_time,task['Ts*']+task['D*']-produce_step['d*'])
            interval = [task['D*']-produce_step['d*'],task['D*']]
            step_discrete_intervals.append(interval)
            print(step['step_id'])
            print(step['step_id'][2])
            print(ord(step['step_id'][2]))
            rps.append(produce_step['rp'][ord(step['step_id'][2])-ord('0')])

        #print('Parent_step_start_time: ', parent_step_start_time)   
        #print('step_discrete_intervals', step_discrete_intervals, 'rps', rps)         
        #step_discrete_intervals, rps = (list(t) for t in zip(*sorted(zip(step_discrete_intervals, rps))))
        #Compute aggregated rp(t) for each discretized time interval
        step_intervals = []
        new_rps = []
        step_discrete_timestamps = sorted(list(set(chain.from_iterable(step_discrete_intervals))))
        for i in range(1,len(step_discrete_timestamps)):
            step_intervals.append([step_discrete_timestamps[i-1],step_discrete_timestamps[i]])
            new_rps.append(0)
        for i in range(0,len(step_intervals)):
            for j in range(0,len(step_discrete_intervals)):
                if self.interval_overlaps(step_intervals[i],step_discrete_intervals[j]):
                    new_rps[i] += rps[j]  
        #print('step_discrete_intervals', step_intervals, 'rps', new_rps)         
        #Calculate rac for each discretized time interval
        produced_till_here = 0
        consumed_till_here = 0
        actually_consumed_till_here = 0
        time_passed = 0
        racs = []
        #print('rc',step['rc'])
        for i in range(0,len(step_intervals)):
            time_passed += step_intervals[i][1] - step_intervals[i][0]
            produced_till_here += (step_intervals[i][1] - step_intervals[i][0])*new_rps[i]
            consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*step['rc']
            if produced_till_here < consumed_till_here:
                racs.append(min(new_rps[i],step['rc']))
            else:
                racs.append(step['rc'])
            actually_consumed_till_here += (step_intervals[i][1] - step_intervals[i][0])*racs[i]
        #print('racs',racs)
        #Calculate optimal step finish time (which only considers its parent step)
        te = step_intervals[len(step_intervals)-1][1]
        #print('act cons', actually_consumed_till_here, 'prod', produced_till_here, 'te', te)
        if actually_consumed_till_here < produced_till_here:
            te += (produced_till_here - actually_consumed_till_here)/step['rc']
        #print('te', te)
        return parent_step_start_time + te

    #Computes optimal finish time of task
    def get_optimal_finish_time(self,stg_id,task_id):
        task = self.step_dependency_model['stages'][stg_id]['tasks'][task_id]
        t_opt = 0.0
        for step in task['steps']:
            if step['has_parent']:
                t = max(self.get_step_finish_time(step), t_opt + step['d*'])
            else:
                t = t_opt + step['d*']
            t_opt = t
        #print('topt',t_opt)
        return t_opt    

    #Computes optimal launch time to schedule task
    def get_optimal_launch_time(self,stg_id,task_id):
        task = self.step_dependency_model['stages'][stg_id]['tasks'][task_id]
        D = self.get_optimal_duration(stg_id,task_id)
        Te = self.get_optimal_finish_time(stg_id,task_id)
        task['Te*'] = Te
        task['Ts*'] = Te - D
        #print('D*', D, 'Te', Te, 'Ts', Te-D)
        return task['Ts*']

    #Dispatches tasks according to their start times
    def dispatcher(self,no_of_tasks,results,scheduled):
        print('Dispatching tasks now')
        pool = Pool()
        start_time = time.time()
        while len(results) < no_of_tasks:
            if scheduled.empty():
                continue
            #print(scheduled.queue[0][0], time.time()-start_time)
            while not scheduled.empty() and scheduled.queue[0][0] <= time.time()-start_time:
                results.append(pool.apply_async(self.exec_task,scheduled.get()[1]))
        print('Scheduling done')
        return results

    def schedule(self):
        with open('2_stage_map_reduce_lazy.json', 'r') as f:
            self.step_dependency_model = json.load(f)
        print('Read step dependency model successfully')

        #Nimble scheduler
        current_parents = []
        new_parents = []
        no_of_tasks = 0
        no_of_tasks_for_dispatcher = 0
        results = []
        total_cost = 0.0
        job_end_time = 0.0
        job_start_time = sys.float_info.max
        #Priority queue to dispatch tasks from
        scheduled = PriorityQueue()

        #Scheduling initial stages with no parents
        for i in range(0,len(self.step_dependency_model['stages'])):
            stage = self.step_dependency_model['stages'][i]
            no_of_tasks += len(stage['tasks'])
            no_of_tasks_for_dispatcher += len(stage['tasks'])
            self.stage_map[stage['stage_id']] = stage
            if stage['parent'] is None:
                new_parents.append(stage['stage_id'])
                for j in range(0,len(stage['tasks'])):
                    scheduled.put((stage['tasks'][j]['Ts*'],(i,j,stage['exec_file'],)))
                    no_of_tasks -= 1
    
        #Starting the dispatcher
        #There could be contention for the queue of processes 'scheduled'        
        dispatcher_thread = threading.Thread(target=self.dispatcher, args=[no_of_tasks_for_dispatcher,results,scheduled])
        dispatcher_thread.start()

        #Scheduling remaining stages in bfs
        while no_of_tasks > 0:
            for i in range(0,len(self.step_dependency_model['stages'])):
                stage = self.step_dependency_model['stages'][i]
                if stage['parent'] in current_parents:
                    new_parents.append(stage['stage_id'])
                    for j in range(0,len(stage['tasks'])):
                        self.step_dependency_model['stages'][i]['tasks'][j]['Ts*'] = self.get_optimal_launch_time(i,j) 
                        print(i,j,stage['tasks'][j]['Ts*'])                   
                        scheduled.put((stage['tasks'][j]['Ts*'],(i,j,stage['exec_file'],)))
                        no_of_tasks -= 1
            current_parents = new_parents
            new_parents = []

        #Wait on all processes to complete
        dispatcher_thread.join()
        res = [x.get() for x in results]
        #print(res)
        total_cost += sum([res[x][0] for x in range(len(res))])
        job_start_time = min([res[x][2] for x in range(len(res))])
        job_end_time = max([res[x][1] for x in range(len(res))])
        for obj in res:
            self.step_dependency_model["stages"][obj[3][0]]['tasks'][obj[3][1]] = obj[3][2]
        print('Total cost is:', total_cost)
        print('JCT given start time 0.0', job_end_time-job_start_time)

        # Serializing json and writing to file
        json_object = json.dumps(self.step_dependency_model, indent = 4)
        with open("2_stage_map_reduce_nimble.json", "w") as outfile:
            outfile.write(json_object)

if __name__ == '__main__':
    nimble = NimbleScheduler()
    nimble.schedule()