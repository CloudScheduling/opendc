package org.opendc.workflow.service.scheduler.task

import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.api.Task
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import java.util.*

/**
 * One of the task scheduling policies in a DC environment.
 *
 * This class implements HEFT algorithm based ordering on the tasks read from a trace.
 * Extends TaskOrderPolicy class for allowing its execution.
 */

public class HEFTPolicy(private val hosts : Set<HostSpec>) : HolisticTaskOrderPolicy{

    /**
     * A set of tasks is transformed into a queue by applying HEFT algorithm.
     * @param tasks eligible tasks for scheduling
     */
    public override fun orderTasks(tasks: Set<TaskState>) : Queue<TaskState>{
        val pendingTasks = tasks.toMutableSet()
        val finalScheduledTasks = LinkedList<TaskState>()
            for (task in pendingTasks) {
                // pending tasks would be passed to the calculateUpwardRank() function
                if (null == task.task.metadata["upward-rank"]) {
                    task.task.metadata["upward-rank"] = calculateUpwardRank(task.task)
                }
                // otherwise, it means the rank was already calculated as part of child task's computation cost calculation
            }
        val unsortedtasksList = tasks.toList()
        var tasksList = unsortedtasksList.sortedByDescending { it.task.metadata["upward-rank"] as Double }
        // now we do sorting of tasks above as per decreasing order of the Upward ran

        // select the first task from sorted list
        // for-each processor(read 'host'), do:
        // compute the "Earliest Finish Time" (EFT) using the insertion-based scheduling policy
        // assign task to the host that minimises EFT of this selected task
        val tasksScheduledOnCPUMap : MutableMap<ProcessingUnit, MutableList<TaskState>> = mutableMapOf()
        for (task in tasksList){
            val requiredCores: Int = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
            var earliestFinishTime = Long.MAX_VALUE
            var currentStartTime: Long = 0
            var prevFinishTime : Long = 0
            var currentFinishTime = Long.MAX_VALUE
            var nextStartTime : Long = 0
            var assignedHost: HostSpec? = null
            var assignedCore: ProcessingUnit? = null
            var gapFound = false
            var firstTaskOnHostCPUSupplied = false
            val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
            for (host in potentialHosts){
                gapFound = false
                var executionTime = calculateExecutionTime(task, requiredCores, host)
                for (CPU in host.model.cpus){
                    // check if there are no tasks scheduled on this CPU
                    if (!tasksScheduledOnCPUMap.containsKey(CPU)) {
                        // if so (no task scheduled on this CPU), schedule this task on this very CPU!
                        tasksScheduledOnCPUMap[CPU] = mutableListOf(task)
                        currentStartTime = 0
                        currentFinishTime = currentStartTime + executionTime
                        earliestFinishTime = currentFinishTime
                        assignedCore = CPU
                        assignedHost = host
                        gapFound = true
                        firstTaskOnHostCPUSupplied = true
                        break
                    }
                    else
                    { // else if there are tasks already scheduled on this CPU
                        val tasksScheduledOnThisCPU = tasksScheduledOnCPUMap[CPU]!!
                        prevFinishTime = 0
                        nextStartTime = 0
                        currentFinishTime = Long.MAX_VALUE

                        // if there is only one task on this CPU, then set the values of finish time and start time
                        if(tasksScheduledOnThisCPU.size == 1){
                            prevFinishTime = tasksScheduledOnThisCPU[0].task.metadata["finish-time"] as Long
                            nextStartTime = prevFinishTime
                        }

                        // start checking for the best fit of the execution time slot of the current task on each CPUs of current host
                        for (i in 1 until tasksScheduledOnThisCPU.size){
                            prevFinishTime = tasksScheduledOnThisCPU[i-1].task.metadata["finish-time"] as Long
                            nextStartTime = tasksScheduledOnThisCPU[i].task.metadata["start-time"] as Long
                            if(nextStartTime - prevFinishTime >= executionTime){ // first suitable CPU task-scheduling gap found
                                currentFinishTime = prevFinishTime + executionTime
                                gapFound = true
                                break
                            }
                        }
                        // if no suitable gap was found, calculate and store the finish time to compare with other hosts
                        if(!gapFound){
                            prevFinishTime = tasksScheduledOnThisCPU.last().task.metadata["finish-time"] as Long
                            currentFinishTime = prevFinishTime + executionTime
                        }

                        if(currentFinishTime < earliestFinishTime){
                            earliestFinishTime = currentFinishTime
                            currentStartTime = prevFinishTime
                            assignedHost = host
                            assignedCore = CPU
                        }
                    }
                }
                if(firstTaskOnHostCPUSupplied)
                    break // first task was supplied which can be adjusted on the very first CPU
            }
            task.task.metadata["assigned-host"] = assignedHost!!
            task.task.metadata["finish-time"] = earliestFinishTime
            task.task.metadata["start-time"] = currentStartTime
            // for every CPU, ordered list of a task assigned in the sorted order of task scheduled
            val CPUTasks = tasksScheduledOnCPUMap[assignedCore]!!
            for (i in 0 until CPUTasks.size){
                val tempFinishTime = CPUTasks[i].task.metadata["finish-time"]
                if(tempFinishTime == currentStartTime && !firstTaskOnHostCPUSupplied){
                    CPUTasks.add(i+1, task)
                    break
                }
            }
            pendingTasks.remove(task)
            finalScheduledTasks.add(task)
        }
        return finalScheduledTasks
    }

    private fun calculateExecutionTime(task: TaskState, requiredCores: Int, host: HostSpec): Long {
        val cpuCycles = task.task.metadata["cpu-cycles"] as Long
        val frequency = host.model.cpus.first().frequency
        return (cpuCycles / (frequency * 0.8 * requiredCores)).toLong()
    }

//    private fun removeForNow(){
//        val scheduledTasks : MutableMap<ProcessingUnit, MutableList<TaskState>> = mutableMapOf()
//
//        for (host in potentialHosts){
//            val executionTime = calculateExecutionTime(task, requiredCores, host)
//            for (CPU in host.model.cpus){
//                // look for best-fitting of a task on each processor - by checking the start time and finish time of each CPU.
//
//                if (!scheduledTasks.containsKey(CPU))
//                    scheduledTasks[CPU] = mutableListOf()
//
//                val tasks = scheduledTasks[CPU]!!
//                var prevFinishTime : Long = -1
//                var currentFinishTime = Long.MAX_VALUE
//                var gapFound = false
//
//                for (i in 1 until tasks.size) {
//                    prevFinishTime = tasks[i - 1].task.metadata["finish-time"] as Long
//                    val nextStartTime = tasks[i].task.metadata["start-time"] as Long
//
//                    if (nextStartTime - prevFinishTime >= executionTime) {
//                        currentFinishTime = (prevFinishTime + executionTime).toLong()
//                        gapFound = true
//                        break
//                    }
//                }
//
//                if (!gapFound) {
//                    prevFinishTime = tasks.last().task.metadata["finish-time"] as Long
//                    // currentStartTime = prevFinishTime
//                    currentFinishTime = prevFinishTime + executionTime
//                }
//
//
//                if (currentFinishTime < earliestFinishTime){
//                    earliestFinishTime = currentFinishTime
//                    currentStartTime = prevFinishTime
//                    assignedHost = host
//                    assignedCore = CPU
//                }
//            }
//        }
//
//        task.task.metadata["assigned-host"] = assignedHost!!
//        task.task.metadata["finish-time"] = earliestFinishTime
//        task.task.metadata["start-time"] = currentStartTime
//
//        val cpuTasks = scheduledTasks[assignedCore]!!
//        for (i in 0 until cpuTasks.size) {
//            val finishTime = cpuTasks[i].task.metadata["finish-time"]
//            if (finishTime == currentStartTime) {
//                cpuTasks.add(i + 1, task)
//                break
//            }
//        }
//
//    }

    /**
     * Upward rank for each task would be calculated here
     * @param task task for which upward rank needs to be calculated
     */

    private fun calculateUpwardRank(task: Task): Double{
//        val upwardRank: Double = getMeanComputationCostOfTask(task) + getUpwardRankRecursively(task, Double.MIN_VALUE)
//        task.task.metadata["upward-rank"] = upwardRank // +getUpwardRankRecursively(task, Double.MIN_VALUE)
//        return upwardRank
        val meanCommunicationCost = 0.0
        var dependentTaskRank: Double
        var tempMaxChildRank: Double = 0.0
        if(null == task.dependents || task.dependents.isEmpty()){
            task.metadata["upward-rank"] = getMeanComputationCostOfTask(task)
            return task.metadata["upward-rank"] as Double
        }
        else{
        for (childTask in task.dependents){
            dependentTaskRank = calculateUpwardRank(childTask)
            if (dependentTaskRank + meanCommunicationCost > tempMaxChildRank){
                tempMaxChildRank = dependentTaskRank + meanCommunicationCost
            }
        }
        return tempMaxChildRank + getMeanComputationCostOfTask(task)
        }
    }

//    /**
//     * Upward rank for each task would be calculated here -- NOT USING THIS ANYMORE
//     * @param task task passed for which upward rank needs to be calculated
//     * @param maxChildRank the maximum child rank to keep a track for parent's upward rank
//     */
//
//    private fun getUpwardRankRecursively(task: TaskState, maxChildRank: Double) : Double{
//        // val upwardRank = 0
//
//        // var maxChildRank = Double.MIN_VALUE
//        // assumed above that data transfer cost is common in the simulation environment
//
//         if (null == task.dependents || task.dependents.isEmpty()) {
//            // task.task.metadata["upward-rank"] = getMeanComputationCostOfTask(task)
//             // return task.task.metadata["upward-rank"] as Double
//             return 0.toDouble()
//         }
//         else {
//            for (childTask in task.dependents) {
//                // set maxChildRank to 0, and then only keep for-loop for recursive call and getting upward rank.
//                return if (maxChildRank < getMeanComputationCostOfTask(childTask)
//                    + getUpwardRankRecursively(childTask, Double.MIN_VALUE)){
//                    getMeanComputationCostOfTask(childTask) +
//                        getUpwardRankRecursively(childTask, Double.MIN_VALUE)
//                    // return getUpwardRankRecursively(childTask)
//                } else
//                    maxChildRank
//            }
//             return Double.MIN_VALUE // never be executed!
//        }
//    }

    private fun getMeanComputationCostOfTask(task: Task) : Double {
        var frequency: Double
        var totalComputationCostOfTask = 0.0
        var avgComputationCost = 0.0

        // _|_, above assumed the communication cost between child and parent task as constant
        // to reduce the complexity of the code
        val cpuCycles = task.metadata["cpu-cycles"] as Long
        val requiredCores: Int = task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
        val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
        // calculate computation cost of this task on each host based on their CPU frequency
        for (host in potentialHosts){
            frequency = host.model.cpus.first().frequency
            totalComputationCostOfTask += cpuCycles / (frequency * 0.8 * requiredCores)
        }
        avgComputationCost = totalComputationCostOfTask / potentialHosts.size
        task.metadata["mean-computation-cost"] = avgComputationCost
        return avgComputationCost
    }

    /**
     * A set of tasks is transformed into a final queue under which their final
     * execution on the specified hosts would be fixed by performing a best-fit match.
     * @param tasks eligible tasks for scheduling
     */
    private fun taskInsertionPhase(tasks: Set<TaskState>){

    }

    /**
    * Implementation/definition required for inheriting TaskOrderPolicy class
    */
    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("Would not be implemented!")
    }

}
