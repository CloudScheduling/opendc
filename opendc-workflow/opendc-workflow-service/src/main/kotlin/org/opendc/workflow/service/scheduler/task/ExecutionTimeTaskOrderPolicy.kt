package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.api.WORKFLOW_TASK_EXECUTION
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowSchedulerListener
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.getValue
import kotlin.collections.set

/**
 * A [TaskOrderPolicy] orders tasks based on the pre-specified (approximate) duration of the task.
 */
public data class ExecutionTimeTaskOderPolicy(public val ascending: Boolean = true) : TaskOrderPolicy {

    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> =
        object : Comparator<TaskState>, WorkflowSchedulerListener {
            private val results = HashMap<UUID, Long>()

            init {
                scheduler.addListener(this)
            }

            override fun taskReady(task: TaskState) {
                val deadline = task.task.metadata[WORKFLOW_TASK_EXECUTION] as? Long?
                results[task.task.uid] = deadline ?: Long.MAX_VALUE
            }

            override fun taskFinished(task: TaskState) {
                results -= task.task.uid
            }

            private val TaskState.duration: Long
                get() = results.getValue(task.uid)

            override fun compare(o1: TaskState, o2: TaskState): Int {
                return compareValuesBy(o1, o2) { state -> state.duration }.let {
                    if (!ascending) it else -it
                }
            }
        }

    override fun toString(): String {
        return "Task-Duration(${if (ascending) "asc" else "desc"})"
    }
}
