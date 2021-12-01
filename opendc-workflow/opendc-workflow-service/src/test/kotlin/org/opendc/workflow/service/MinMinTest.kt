package org.opendc.workflow.service

import kotlinx.coroutines.CoroutineScope
import org.junit.jupiter.api.Test
import org.opendc.compute.service.driver.Host
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.VCpuWeigher
import org.opendc.compute.simulator.SimHost
import org.opendc.compute.workload.ComputeServiceHelper
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
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowSchedulerListener
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.NullTaskEligibilityPolicy
import org.opendc.workflow.service.scheduler.task.SubmissionTimeTaskOrderPolicy
import org.opendc.workflow.service.scheduler.task.TaskOrderPolicy
import org.opendc.workflow.workload.WorkflowSchedulerSpec
import java.time.Clock
import java.time.Duration
import java.util.*
import kotlin.Comparator
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.coroutines.CoroutineContext

class MinMinTest {
    @Test
    fun testMinMin() = runBlockingSimulation {
        val workflow = createWorkflow()
        val host = createHostSpec(1)
        val schedulerSpec = createSchedulerSpec()
        val computeHelper = createComputeService(coroutineContext, clock)

        val hosts = mutableSetOf<SimHost>()
        repeat(2) {
            val spec = createHostSpec(1)
            val host = computeHelper.registerHost(spec)
            hosts.add(host)
        }
    }

    private fun createComputeService(context : CoroutineContext, clock : Clock): ComputeServiceHelper {
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        return ComputeServiceHelper(context, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))
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

    private fun createSchedulerSpec(): WorkflowSchedulerSpec {
        return WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = SubmissionTimeTaskOrderPolicy(),
        )
    }
}


