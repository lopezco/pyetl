import os
import subprocess as sp
import time


#processFile is the function used to simulate the processing of a single file
#Here, processFile does not actually process the input file, it simply runs a Linux sleep command
#whose length in proportional to the file's size
#Technically, processFile relies on the subprocess Python package which allows to run system commands
#in async or sync mode (the latter requiring an additional call to the wait() function that waits for
#the command's completion)
#We will use processFile in sync mode for the serial case and in async mode for the parallel case
#since this will allow us to start several concurrent processing tasks without any additional
#parallel processing framework
#Inputs:
#filename: name of the file to process
#sleepFactor: factor to apply to the file size for determining the sleep command length
def processFile(filename, sleepFactor, async=False):
    p = sp.Popen(["sleep", str(os.path.getsize(filename) * sleepFactor)])
    if not async:
        p.wait()
        p.communicate()
        if p.returncode != 0:
            raise Exception("Processing task errored")
    return p


class ParallelProcessor(object):
    pass


def start_task(function, parameters, worker, task, verbose=False, async=False):
    """
    Starts a new processing task identified by a task index on a worker identified by a worker index
    :param fileTable: table of files to process
    :param worker: worker index, belongs to the [0, (#workers-1)] range
    :param task: task index, belongs to the [0, (#files-1)] range
    :param verbose:
    :return:
    """
    p = sp.Popen(["sleep", str(os.path.getsize(filename) * sleepFactor)])
    if not async:
        p.wait()
        p.communicate()
        if p.returncode != 0:
            raise Exception("Processing task errored")

    if verbose:
        print("Worker " + str(worker) + " starting task " + str(task))
    return p

#getNextAssignedTask returns the index of the next task a worker has to run
#If there are no tasks left to run for the worker, the function returns None
#Inputs:
#currentTask: index of the current task
#tasksAssigned: list of all tasks assigned to the worker
def getNextAssignedTask(currentTask, tasksAssigned):
    #Find the index of currentTask in tasksAssigned
    idx = tasksAssigned.index(currentTask)
    if idx == len(tasksAssigned)-1:
        #If currentTask was the last element, return None
        return None
    else:
        #Otherwise return the next value
        return tasksAssigned[idx+1]

#parallelProcessing is the function that distributes and handles processing tasks on the workers
#It mainly relies on the input taskAssignment variable which has to be computed beforehand and
#and describes the distribution schemes (it contains for each worker the list of assigned tasks)
#Inputs:
#numFiles: number of files to process
#numWorkers: number of workers to use
#fileTable: table of files to process
#sleepFactor: factor to apply to the file size for determining the sleep command length
#taskAssignment: list of tasks assigned to each worker (the i-th element of taskAssignment is the
#list of tasks assigned to the i-th worker)
def parallelProcessing(numFiles, numWorkers, fileTable, sleepFactor, taskAssignment, verbose=False):
    #Start a timer
    start = time.time()
    #Keep track of completed tasks, globally and for each worker
    numTasksCompleted = 0
    tasksCompleted = [[] for _ in range(numWorkers)]
    #Keep track of the execution time on each worker
    execTimeWorkers = [None] * numWorkers
    #The following lists store the task that is currently executing on each worker,
    #more precisely the task's index in currentTask and the associated process in currentProcess
    #Initialize currentTask with the first element found in taskAssignment
    currentTask = [elem[0] for elem in taskAssignment]
    currentProcess = [None] * numWorkers
    #Start processing and run as long as not all files have been processed
    while numTasksCompleted < numFiles:
        #Loop through workers
        for idx in range(0, numWorkers):
            #The following test checks if the worker has already completed all its tasks,
            #in which case its currentTask would have a None value
            if currentTask[idx] is not None:
                if not currentProcess[idx]:
                    #If the current worker does not have any task assigned yet, start a new one
                    if verbose:
                        print("Worker " + str(idx) + ": no task assigned yet")
                    currentProcess[idx] = start_task(fileTable, sleepFactor, idx, currentTask[idx])
                else:
                    #Otherwise investigate the running process
                    p = currentProcess[idx]
                    #The following tests checks if the task has terminated
                    if not(p.poll() == None):
                        #If the task has not terminated, there is nothing to do, just move on to the next worker
                        #If it has terminated, we will increase the completed tasks counter and start a new task
                        #Before that, we check the return code to make sure the process terminated successfully
                        p.communicate()
                        if p.returncode != 0:
                            raise Exception("Processing task errored")
                        #Increase the completed tasks counter and add the index of the completed task to the
                        #worker's list of completed tasks
                        numTasksCompleted += 1
                        tasksCompleted[idx].append(currentTask[idx])
                        if verbose:
                            print("Worker " + str(idx) + ": task " + str(currentTask[idx]) + " completed!")
                        #Get the next task the worker has to run
                        currentTask[idx] = getNextAssignedTask(currentTask[idx], taskAssignment[idx])
                        if currentTask[idx] is not None:
                            #If there is a task left to run, start it
                            currentProcess[idx] = start_task(fileTable, sleepFactor, idx, currentTask[idx])
                        else:
                            #Otherwise it means that the worker has completed all its tasks
                            #Save the execution time
                            execTimeWorkers[idx] = time.time() - start
    #Before exiting, we check that execution went well by comparing the taskAssignment and tasksCompleted variables
    #which are supposed to be equal
    for idx in range(0, numWorkers):
        t1 = taskAssignment[idx]
        t2 = tasksCompleted[idx]
        if len(t1) != len(t2) or not all([t1[elem] == t2[elem] for elem in range(0, len(t1))]):
            raise Exception("Something went wrong on worker " + str(idx))
    #End the timer and display the execution time
    end = time.time()
    execTimeOverall = end - start
    print("Execution time in PARALLEL mode: %.2f seconds" % execTimeOverall)
    return execTimeOverall, execTimeWorkers
