package org.opendc.compute.service.scheduler

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import java.util.*

/**
 * Assigns the host to the server that was previously determined by the HolisticTaskOrderPolicy.
 */
public class AssignmentExecutionScheduler : ComputeScheduler {
    private val hosts = mutableListOf<HostView>()

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(server: Server): HostView {
        val (selectedHostId, selectedHostName) = server.meta["assigned-host"] as Pair<UUID, String>
        val selectedHost = hosts.find { h -> h.host.uid == selectedHostId && h.host.name == selectedHostName }
        return selectedHost ?: throw Exception("No host with name $selectedHostName")
    }
}
