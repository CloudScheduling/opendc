package org.opendc.compute.service.scheduler

import org.opendc.compute.api.Server
import org.opendc.compute.service.internal.HostView
import kotlin.random.Random

public class RandomScheduler : ComputeScheduler {
    private val random = Random(123)
    private val hosts = mutableSetOf<HostView>()

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(server: Server): HostView? {
        return hosts.random(random)
    }
}
