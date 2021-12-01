package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import java.util.*

public class MinMinPolicy(public val hosts : Set<HostSpec>) : HolisticTaskOrderPolicy {
    /**
     * A set of tasks is transformed into a queue by applying Min-Min.
     * @param tasks eligible tasks for scheduling
     */
    override fun orderTasks(tasks: MutableSet<TaskState>): Queue<TaskState> {
        val startTimes = hosts.associateBy({ host -> host },
            { host -> host.model.cpus.associateBy({ cpu -> cpu }, { _ -> 0.0 }).toMutableMap() })

        while (tasks.isNotEmpty()) {
            val nextTask: TaskState



            tasks.remove(nextTask)
        }
    }

    /**
     * For a given task the minimal execution time is found.
     * The host associated with this time is stored in the [startTimes] map.
     *
     * We assume that a machine only has the same kind of CPUs.
     * @param startTimes lists for each host the processing
     */
    private fun findMinExecutionTimeTask(taskState: TaskState, startTimes: Map<HostSpec, MutableMap<ProcessingUnit, Double>>) {
        var min = Int.MAX_VALUE
        var selectedHost:HostSpec? = null

        // TODO Check metadata key
        val requiredCpus: Int = taskState.task.metadata["cpu-count"] as Int
        val possibleHosts = hosts.filter { it.model.cpus.count() >= requiredCpus }

        for (host in possibleHosts) {
            val execTime = calculateExecutionTime(taskState, host)
            val selectedCpus = startTimes[host]
                .toList()
                .sortedBy { kvp -> kvp.second }
                .take(requiredCpus)

            val startTime = selectedCpus.maxOf { it.second }
            val completionTime = startTime + execTime

            if (completionTime < min) {
                min = completionTime
                selectedHost = host
            }
        }

        if (selectedHost == null) {
            throw Exception("No host could be found")
        }

        val selectedCpus = startTimes[host]
            .toList()
            .sortedBy { kvp -> kvp.second }
            .take(requiredCpus)
            .map { it.first }

        for (cpu in selectedCpus) {
            startTimes[selectedHost][cpu] = min
        }
    }

    /**
     * Calculate the execution time for a task on a host.
     * The execution time is the time needed to execute the job on the machine
     * once it is running on it (if a task T waits until point in time A to run
     * on host H and then runs until time B, the execution time is B-A).
     */
    private fun calculateExecutionTime(task : TaskState, host: HostSpec): Double {
        val cpuCycles = task.task.metadata["cpu-cycle"]
        // we ignore number of CPUs for now
        //val requiredCpus: Int = task.task.metadata["cpu-count"] as Int
        val totalFrequency = host.model.cpus.sumOf { it.frequency }
    }

    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
    }

}
