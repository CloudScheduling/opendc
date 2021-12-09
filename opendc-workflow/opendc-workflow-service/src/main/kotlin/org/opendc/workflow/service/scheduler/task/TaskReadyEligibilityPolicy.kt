package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.TaskStatus
import org.opendc.workflow.service.internal.WorkflowServiceImpl

public class TaskReadyEligibilityPolicy : TaskEligibilityPolicy {
    override fun invoke(scheduler: WorkflowServiceImpl): TaskEligibilityPolicy.Logic = object : TaskEligibilityPolicy.Logic {
        override fun invoke(task: TaskState): TaskEligibilityPolicy.Advice =
            if (task.state == TaskStatus.READY)
                TaskEligibilityPolicy.Advice.ADMIT
            else if (task.state == TaskStatus.CREATED)
                TaskEligibilityPolicy.Advice.DENY
            else
                throw Exception("This branch should never be executed.")
    }

}
