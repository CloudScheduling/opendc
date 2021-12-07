package org.opendc.compute.service.scheduler.filters

import org.opendc.compute.api.Server
import org.opendc.compute.service.driver.Host
import org.opendc.compute.service.internal.HostView
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState

public class ElopFilter(private val jobHostAssignment : HashMap<JobState, Set<Host>>) : HostFilter {
    override fun test(host: HostView, server: Server): Boolean {
        val taskState = server.meta["task"] as TaskState
        val jobState = jobHostAssignment[taskState.job]
        return jobState?.contains(host.host) ?: throw Exception("JobState not in dictionary")
    }
}
