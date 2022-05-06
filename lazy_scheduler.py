#!/usr/bin/python

from multiprocessing import Pool
import functools, time, json, sys

class LazyScheduler:
    #global step dependency model
    step_dependency_model = {}

    #Code to execute given task for given analytics job
    def exec_task(self, stg_idx, task_idx, exec_file):
        #Stores cost of running current task
        cost=0.0

        #To compute JCT
        task_start_time=sys.float_info.max
        task_end_time=0.0

        task = self.step_dependency_model['stages'][stg_idx]['tasks'][task_idx]
        optimal_task_duration = 0.0

        #Executes each step of the task in that order
        for step in task['steps']:
            #Preparing to execute function from different python class
            obj = getattr(__import__(exec_file),exec_file)
            #Prepare arguments to task exec
            arg_dict = {}
            for i in range(len(step['step_func_arg_keys'])):
                arg_dict[step['step_func_arg_keys'][i]] = step['step_func_arg_vals'][i]
            print('Scheduling task_id: ', task['task_id'], 'of', exec_file, 'at', str(time.time()))

            #Computing step execution time
            start = time.time()
            C,P = getattr(obj, step['step_func_name'])(arg_dict)
            end = time.time()

            #Update P, C, rc, rp, total job cost, optimal step duration, job finish time
            step['P'] = P
            step['C'] = C
            #Averaging for better estimates
            step['rc'] = (step['rc']*task['no_of_runs'] + C/(end-start))/(task['no_of_runs']+1)
            for i in range(0,len(P)):
                step['rp'][i] = (step['rp'][i]*task['no_of_runs'] + P[i]/(end-start))/(task['no_of_runs']+1)
            optimal_task_duration += end-start
            step['d*'] = (step['d*']*task['no_of_runs'] + end - start)/(task['no_of_runs'] + 1)
            #Computing cost and start and end times for JCT calculation
            cost += end-start
            task_end_time = max(task_end_time,end)
            task_start_time = min(task_start_time,start)

        #Update optimal task duration, no_of_runs
        task['D*'] = (task['D*']*task['no_of_runs'] + optimal_task_duration)/(task['no_of_runs'] + 1)
        task['no_of_runs'] += 1

        return [cost, task_end_time, task_start_time, [stg_idx, task_idx, task]]

    def smap(self, f):
        return f()

    def schedule(self):
        with open('2_stage_map_reduce_3.json', 'r') as f:
            self.step_dependency_model = json.load(f)
        print('Read step dependency model successfully')

        #Lazy scheduler
        current_parents = []
        new_parents = []
        scheduled = []
        total_cost = 0.0
        job_end_time = 0.0
        job_start_time = sys.float_info.max
        no_of_tasks = 0

        #Scheduling initial stages with no parents
        for i in range(0,len(self.step_dependency_model['stages'])):
            stage = self.step_dependency_model['stages'][i]
            no_of_tasks += len(stage['tasks'])
            if stage['parent'] is None:
                new_parents.append(stage['stage_id'])
                for j in range(0,len(stage['tasks'])):
                    scheduled.append(functools.partial(self.exec_task,i,j,stage['exec_file']))

        with Pool() as pool:
            res = pool.map(self.smap, scheduled)

        #Computing total cost, jct for complete job. Updating changes to step dependency model
        total_cost += sum([res[x][0] for x in range(len(res))])
        job_start_time = min(job_start_time, min([res[x][2] for x in range(len(res))]))
        job_end_time = max(job_end_time, max([res[x][1] for x in range(len(res))]))
        for obj in res:
            self.step_dependency_model["stages"][obj[3][0]]['tasks'][obj[3][1]] = obj[3][2]

        no_of_tasks -= len(scheduled)
        scheduled = []
        current_parents = new_parents
        new_parents = []

        #Scheduling remaining stages in bfs
        while no_of_tasks > 0:
            for i in range(0,len(self.step_dependency_model['stages'])):
                stage = self.step_dependency_model['stages'][i]
                if stage['parent'] in current_parents:
                    new_parents.append(stage['stage_id'])
                    for j in range(0,len(stage['tasks'])):
                        scheduled.append(functools.partial(self.exec_task,i,j,stage['exec_file']))
            with Pool() as pool:
                res = pool.map(self.smap, scheduled)

            #Computing total cost, jct for complete job. Updating changes to step dependency model
            total_cost += sum([res[x][0] for x in range(len(res))])
            job_start_time = min(job_start_time, min([res[x][2] for x in range(len(res))]))
            job_end_time = max(job_end_time, max([res[x][1] for x in range(len(res))]))
            for obj in res:
                self.step_dependency_model["stages"][obj[3][0]]['tasks'][obj[3][1]] = obj[3][2]

            no_of_tasks -= len(scheduled)
            scheduled = []
            current_parents = new_parents
            new_parents = []

        print('Total cost is:', total_cost)
        print('JCT given start time 0.0', job_end_time-job_start_time)

        #Writing updated step dependency model back to json file
        json_object = json.dumps(self.step_dependency_model, indent = 4)
        with open("2_stage_map_reduce_lazy.json", "w") as outfile:
            outfile.write(json_object)

if __name__ == '__main__':
    lazy = LazyScheduler()
    lazy.schedule()