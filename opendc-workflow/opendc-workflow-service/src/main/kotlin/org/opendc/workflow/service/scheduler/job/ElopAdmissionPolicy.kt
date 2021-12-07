package org.opendc.workflow.service.scheduler.job

import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.WorkflowServiceImpl

public class ElopAdmissionPolicy : JobAdmissionPolicy {
    override fun invoke(scheduler: WorkflowServiceImpl): JobAdmissionPolicy.Logic = object : JobAdmissionPolicy.Logic {
        override fun invoke(
            job: JobState
        ): JobAdmissionPolicy.Advice =
            if (job.metadata["sufficientHostsLeft"] as Boolean) {
                JobAdmissionPolicy.Advice.ADMIT
            }
            else JobAdmissionPolicy.Advice.STOP
    }

    override fun toString(): String = "Elop Admission Policy"
}
