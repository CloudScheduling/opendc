package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.TaskStatus
import org.opendc.workflow.service.internal.WorkflowServiceImpl

public class DependenciesFinishedTaskEligibilityPolicy : TaskEligibilityPolicy {
    override fun invoke(scheduler: WorkflowServiceImpl): TaskEligibilityPolicy.Logic = object : TaskEligibilityPolicy.Logic {
        override fun invoke(task: TaskState): TaskEligibilityPolicy.Advice =
            if (task.dependencies.all { it.state == TaskStatus.FINISHED })
                TaskEligibilityPolicy.Advice.ADMIT
            else
                TaskEligibilityPolicy.Advice.DENY
    }

}
