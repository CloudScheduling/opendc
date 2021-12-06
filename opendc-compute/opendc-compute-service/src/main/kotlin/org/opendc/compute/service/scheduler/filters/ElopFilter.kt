package org.opendc.compute.service.scheduler.filters

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState

public class ElopFilter : HostFilter {
    public val mapping: HashMap<JobState, HashSet<HostView>> = HashMap<JobState, HashSet<HostView>>()
    public val reserverCpus: java.util.HashMap<HostView, Int> = HashMap<HostView, Int>()

    override fun test(host: HostView, server: Server): Boolean {
        val hosts = mapping.get(server.taskState.job) ?: throw Exception("No hosts for job stored!")
        return hosts.contains(host)
    }
}
