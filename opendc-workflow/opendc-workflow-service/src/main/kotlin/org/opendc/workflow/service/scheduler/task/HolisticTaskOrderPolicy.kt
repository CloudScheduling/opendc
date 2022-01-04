package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import java.util.*

public interface HolisticTaskOrderPolicy : TaskOrderPolicy {
    public fun orderTasks(tasks: List<TaskState>): Queue<TaskState>
}
