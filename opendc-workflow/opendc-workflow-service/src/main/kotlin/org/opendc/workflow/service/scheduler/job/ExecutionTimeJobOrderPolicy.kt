package org.opendc.workflow.service.scheduler.job

import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import org.opendc.workflow.api.WORKFLOW_TASK_DEADLINE
import org.opendc.workflow.api.WORKFLOW_TASK_EXECUTION
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.WorkflowSchedulerListener
import org.opendc.workflow.service.internal.WorkflowServiceImpl

/**
 * A [JobOrderPolicy] that orders jobs based on its critical path length.
 */
public data class ExecutionTimeJobOrderPolicy(val ascending: Boolean = true) : JobOrderPolicy {
    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<JobState> =
        object :
            Comparator<JobState>,
            WorkflowSchedulerListener {
            private val results = HashMap<Job, Long>()

            init {
                scheduler.addListener(this)
            }

            private val Job.duration: Long
                get() = results[this]!!

            override fun jobSubmitted(job: JobState) {
                results[job.job] = job.job.toposort().sumOf { task ->
                    val estimable = task.metadata[WORKFLOW_TASK_EXECUTION] as? Long?
                    estimable ?: Long.MAX_VALUE
                }
            }

            override fun jobFinished(job: JobState) {
                results.remove(job.job)
            }

            override fun compare(o1: JobState, o2: JobState): Int {
                return compareValuesBy(o1, o2) { it.job.duration }.let { if (ascending) it else -it }
            }
        }

    override fun toString(): String {
        return "Job-Duration(${if (ascending) "asc" else "desc"})"
    }
}

/**
 * Create a topological sorting of the tasks in a job.
 *
 * @return The list of tasks within the job topologically sorted.
 */
public fun Job.toposort(): List<Task> {
    val res = mutableListOf<Task>()
    val visited = mutableSetOf<Task>()
    val adjacent = mutableMapOf<Task, MutableList<Task>>()

    for (task in tasks) {
        for (dependency in task.dependencies) {
            adjacent.getOrPut(dependency) { mutableListOf() }.add(task)
        }
    }

    fun visit(task: Task) {
        visited.add(task)

        adjacent[task] ?: emptyList<Task>()
            .asSequence()
            .filter { it !in visited }
            .forEach { visit(it) }

        res.add(task)
    }

    tasks
        .asSequence()
        .filter { it !in visited }
        .forEach { visit(it) }
    return res
}
