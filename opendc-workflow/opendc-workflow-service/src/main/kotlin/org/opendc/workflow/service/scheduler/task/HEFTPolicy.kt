package org.opendc.workflow.service.scheduler.task

import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import org.opendc.workflow.service.internal.TaskState
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
                    calculateUpwardRank(task)
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
        for (task in tasksList){
            val requiredCores: Int = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
            var earliestFinishTime = Long.MAX_VALUE
            var currentStartTime: Long = 0
            var assignedHost: HostSpec? = null
            var assignedCore: ProcessingUnit? = null
            val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
            // val cpuPerHost : MutableMap<SimHost, ProcessingUnit>
            val scheduledTasks : MutableMap<ProcessingUnit, MutableList<TaskState>> = mutableMapOf()

            for (host in potentialHosts){
                //val minTotalCoreEFT = hostsToCPUAndTimePairMapping[host]!!.toList().sortedBy { it.second }.take(requiredCores)
                val executionTime = calculateExecutionTime(task, requiredCores, host)
                for (CPU in host.model.cpus){
                // look for best-fitting of a task on each processor - by checking the start time and finish time of each CPU.

                    if (!scheduledTasks.containsKey(CPU))
                        scheduledTasks[CPU] = mutableListOf()

                    val tasks = scheduledTasks[CPU]!!
                    var prevFinishTime : Long = -1
                    var currFinishTime = Long.MAX_VALUE
                    var gapFound = false

                    for (i in 1 until tasks.size) {
                        prevFinishTime = tasks[i - 1].task.metadata["finish-time"] as Long
                        val nextStartTime = tasks[i].task.metadata["start-time"] as Long

                        if (nextStartTime - prevFinishTime >= executionTime) {
                            currFinishTime = (prevFinishTime + executionTime).toLong()
                            gapFound = true
                            break
                        }
                    }

                    if (!gapFound) {
                        prevFinishTime = tasks.last().task.metadata["finish-time"] as Long
                        // currentStartTime = prevFinishTime
                        currFinishTime = prevFinishTime + executionTime
                    }


                    if (currFinishTime < earliestFinishTime){
                        earliestFinishTime = currFinishTime
                        currentStartTime = prevFinishTime
                        assignedHost = host
                        assignedCore = CPU
                    }
                }
            }

            task.task.metadata["assigned-host"] = assignedHost!!
            task.task.metadata["finish-time"] = earliestFinishTime
            task.task.metadata["start-time"] = currentStartTime

            val cpuTasks = scheduledTasks[assignedCore]!!
            for (i in 0 until cpuTasks.size) {
                val finishTime = cpuTasks[i].task.metadata["finish-time"]
                if (finishTime == currentStartTime) {
                    cpuTasks.add(i + 1, task)
                    break
                }
            }

            // for every CPU, ordered list of a task assigned in the sorted order of task scheduled
            pendingTasks.remove(task)
            finalScheduledTasks.add(task)
        }
        return finalScheduledTasks
    }

    private fun calculateExecutionTime(task: TaskState, requiredCores: Int, host: HostSpec): Double {
        val cpuCycles = task.task.metadata["cpu-cycles"] as Long
        val frequency = host.model.cpus.first().frequency
        return cpuCycles / (frequency * 0.8 * requiredCores)
    }

    /**
     * Upward rank for each task would be calculated here
     * @param task task passed for which upward rank needs to be calculated
     */

    private fun calculateUpwardRank(task: TaskState): Double{
        val upwardRank: Double = getMeanComputationCostOfTask(task) + getUpwardRankRecursively(task, Double.MIN_VALUE)
        task.task.metadata["upward-rank"] = upwardRank // +getUpwardRankRecursively(task, Double.MIN_VALUE)
        return upwardRank
    }


    private fun getUpwardRankRecursively(task: TaskState, maxChildRank: Double) : Double{
        // val upwardRank = 0
        val meanCommunicationCost = 0.0
        // var maxChildRank = Double.MIN_VALUE
        // assumed above that data transfer cost is common in the simulation environment

         if (null == task.dependents || task.dependents.isEmpty()) {
            // task.task.metadata["upward-rank"] = getMeanComputationCostOfTask(task)
             // return task.task.metadata["upward-rank"] as Double
             return 0.toDouble()
         }
         else {
            for (childTask in task.dependents) {
                // set maxChildRank to 0, and then only keep for-loop for recursive call and getting upward rank.
                return if (maxChildRank < getMeanComputationCostOfTask(childTask)
                    + getUpwardRankRecursively(childTask, Double.MIN_VALUE)){
                    getMeanComputationCostOfTask(childTask) +
                        getUpwardRankRecursively(childTask, Double.MIN_VALUE)
                    // return getUpwardRankRecursively(childTask)
                } else
                    maxChildRank
            }
             return Double.MIN_VALUE // never be executed!
        }
    }

    private fun getMeanComputationCostOfTask(task: TaskState) : Double {
        var frequency: Double
        var totalComputationCostOfTask = 0.0
        var avgComputationCost = 0.0

        // _|_, above assumed the communication cost between child and parent task as constant
        // to reduce the complexity of the code
        val cpuCycles = task.task.metadata["cpu-cycles"] as Long
        val requiredCores: Int = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
        val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
        // calculate computation cost of this task on each host based on their CPU frequency
        for (host in potentialHosts){
            frequency = host.model.cpus.first().frequency
            totalComputationCostOfTask += cpuCycles / (frequency * 0.8 * requiredCores)
        }
        avgComputationCost = totalComputationCostOfTask / potentialHosts.size
        task.task.metadata["mean-computation-cost"] = avgComputationCost
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
