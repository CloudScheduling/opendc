package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import java.util.*

private data class ExecutionSpec(val task: TaskState, val host: HostSpec, val selectedCpus: List<ProcessingUnit>, val completionTime: Double)

public class MinMinPolicy(public val hosts : Set<HostSpec>) : HolisticTaskOrderPolicy {

    /**
     * A set of tasks is transformed into a queue by applying Min-Min.
     * @param tasks eligible tasks for scheduling
     */
    override fun orderTasks(tasks: Set<TaskState>): Queue<TaskState> {
        val startTimes = hosts.associateBy(
            { host -> host },
            { host -> host.model.cpus.associateBy({ cpu -> cpu }, { 0.0 }).toMutableMap() })

        val unmappedTasks = tasks.toMutableSet()
        val orderedTasks = LinkedList<TaskState>()

        while (unmappedTasks.isNotEmpty()) {
            val (nextTask, selectedHost, selectedCpus, completionTime) = unmappedTasks
                .map { getMinExecutionSpec(it, startTimes) }
                .minByOrNull { it.completionTime }!!

            for (cpu in selectedCpus) {
                startTimes[selectedHost]!![cpu] = completionTime
            }

            nextTask.task.metadata["assigned-host"] = selectedHost.name
            unmappedTasks.remove(nextTask)

            orderedTasks.addLast(nextTask)
        }

        return orderedTasks
    }

    /**
     * For a given task the minimal execution time is found.
     * The host associated with this time is stored in the [startTimes] map.
     *
     * We assume that a machine only has the same kind of CPUs.
     * @param startTimes lists for each host the processing
     */
    private fun getMinExecutionSpec(taskState: TaskState, startTimes: Map<HostSpec, Map<ProcessingUnit, Double>>): ExecutionSpec {
        var min = Double.MAX_VALUE
        var selectedHost:HostSpec? = null
        var selectedCpus: List<ProcessingUnit>? = null

        // TODO Check metadata key
        val requiredCpus: Int = taskState.task.metadata["required-cpus"] as Int
        val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCpus }

        for (host in potentialHosts) {
            // Select the n CPUs that complete their previous task first.
            val minCpuCompletionTimes = startTimes[host]!!
                .toList()
                .sortedBy { it.second } // it.second = completion time of CPU
                .take(requiredCpus)

            val startTime = minCpuCompletionTimes.maxOf { it.second }

            val execTime = calculateExecutionTime(taskState, requiredCpus, host)
            val taskCompletionTime = startTime + execTime

            if (taskCompletionTime < min) {
                min = taskCompletionTime
                selectedHost = host
                selectedCpus = minCpuCompletionTimes.map { it.first } // get CPUs
            }
        }

        if (selectedHost == null || selectedCpus == null) {
            throw Exception("No host or CPUs could be found")
        }

        return ExecutionSpec(taskState, selectedHost, selectedCpus, min)
    }

    /**
     * Calculate the execution time for a task on a host.
     * The execution time is the time needed to execute the job on the machine
     * once it is running on it (if a task T waits until point in time A to run
     * on host H and then runs until time B, the execution time is B-A).
     */
    private fun calculateExecutionTime(task: TaskState, requiredCpus: Int, host: HostSpec): Double {
        val cpuCycles = task.task.metadata["cpu-cycles"] as Int
        // Assumption: all CPUs have same frequency
        val frequency = host.model.cpus.first().frequency
        return cpuCycles / (frequency * requiredCpus)
    }

    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
    }

}
