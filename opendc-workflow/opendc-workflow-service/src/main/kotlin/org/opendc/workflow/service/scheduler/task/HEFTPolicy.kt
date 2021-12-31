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

public class HEFTPolicy(private val hosts : Set<HostSpec>) : TaskOrderPolicy{

    private val hostsToCPUAndTimePairMapping : Map<HostSpec, Map<ProcessingUnit, Double>> = hosts.associateBy(
        {host : HostSpec -> host},
        {host : HostSpec -> host.model.cpus.associateBy({ core ->
            core
        }, { 0.0 }).toMutableMap() }
    )
    /**
     * A set of tasks is transformed into a queue by applying HEFT algorithm.
     * @param tasks eligible tasks for scheduling
     */
    private fun orderTasks(tasks: Set<TaskState>) : Queue<TaskState>{
        val pendingTasks = tasks.toMutableSet()
        val scheduledTasks = LinkedList<TaskState>()
            for (task in pendingTasks) {
                // pending tasks would be passed to the calculateUpwardRank() function
                if (null == task.task.metadata["upward-rank"]) {
                    calculateUpwardRank(task)
                }
                // otherwise, it means the rank was already calculated as part of child task's computation cost calculation
            }
        val tasksList = tasks.toList()
        tasksList.sortedByDescending { it.task.metadata["upward-rank"] as Double }
        // now we do sorting of tasks above as per decreasing order of the Upward ran

        // select the first task from sorted list
        // for-each processor(read 'host'), do:
        // compute the "Earliest Finish Time" (EFT) using the insertion-based scheduling policy
        // assign task to the host that minimises EFT of this selected task
        for (task in tasksList){
            val requiredCores: Int = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
            var selectedHost : HostSpec? = null
            var selectedCores: List<ProcessingUnit>? = null
            var min = Double.MAX_VALUE
            val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
            for (host in potentialHosts){
                val minTotalCoreEFT = hostsToCPUAndTimePairMapping[host]!!.toList().sortedBy { it.second }.take(requiredCores)
                val startTime = minTotalCoreEFT.maxOf { it.second }
                val executionTime = calculateExecutionTime(task, requiredCores, host)
                val minEFT = startTime + executionTime
                if(minEFT < min){
                    min = minEFT
                    selectedHost = host
                    selectedCores = minTotalCoreEFT.map { it.first }
                }
            }
                if (selectedCores != null) {
                    for (core in selectedCores) {
                        hostsToCPUAndTimePairMapping[selectedHost]!![core] = min
                    }
                }
            // PENDING LOGIC VERIFICATION above due to minimum EFT determination for each task required...
            task.task.metadata["assigned-host"] = Pair(selectedHost?.uid, selectedHost?.name)
            pendingTasks.remove(task)
            scheduledTasks.add(task)
        }
        return scheduledTasks
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
        var upwardRank: Double

        upwardRank = getMeanComputationCostOfTask(task) + getUpwardRankRecursively(task)
        task.task.metadata["upward-rank"] = upwardRank +getUpwardRankRecursively(task)
        return upwardRank
    }


    private fun getUpwardRankRecursively(task: TaskState) : Double{
        // val upwardRank = 0
        val meanCommunicationCost = 0.0
        var maxChildRank = Double.MIN_VALUE
        // assumed above that data transfer cost is common in the simulation environment

        return if (null == task.dependents) {
            task.task.metadata["upward-rank"] = getMeanComputationCostOfTask(task)
            task.task.metadata["upward-rank"] as Double
        } else {
            for (childTask in task.dependents) {
                if (maxChildRank < getMeanComputationCostOfTask(childTask)){
                    maxChildRank = getMeanComputationCostOfTask(childTask)
                }
            }
            maxChildRank
        }
    }

    private fun getMeanComputationCostOfTask(task: TaskState) : Double {
        var frequency: Double
        var totalComputationCostOfTask = 0.0
        var avgComputationCost = 0.0

        // _|_, above assumed the communication cost between child and parent task as consistent
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
