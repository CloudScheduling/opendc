package org.opendc.workflow.service

import org.junit.jupiter.api.Test
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.kernel.SimSpaceSharedHypervisorProvider
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.ConstantPowerModel
import org.opendc.simulator.compute.power.SimplePowerDriver
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class MinMinTest {
    @Test
    fun testMinMin() = runBlockingSimulation {
        val workflow = createWorkflow()
        val host = createHostSpec(1)
    }

    private fun createWorkflow() : Job {
        val tasks = HashSet<Task>()
        val workflow = Job(UUID.randomUUID(), "<unnamed>", tasks, HashMap())

        for (id in 0..9) {
            val task = Task(UUID(0L, id.toLong()), "task$id", HashSet(), mapOf(
                "cpu-cycles" to 5000 * (id + 1)
            ))
            tasks.add(task)
        }
        return workflow
    }

    private fun createHostSpec(uid: Int): HostSpec {
        // Machine model based on: https://www.spec.org/power_ssj2008/results/res2020q1/power_ssj2008-20191125-01012.html
        val node = ProcessingNode("AMD", "am64", "EPYC 7742", 32)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 3400.0) }
        val memory = List(8) { MemoryUnit("Samsung", "Unknown", 2933.0, 16_000) }

        val machineModel = MachineModel(cpus, memory)

        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimplePowerDriver(ConstantPowerModel(0.0)),
            SimSpaceSharedHypervisorProvider()
        )
    }
}
