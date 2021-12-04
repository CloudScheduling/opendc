package org.opendc.workflow.service

import kotlinx.coroutines.CoroutineScope
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
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
import org.opendc.workflow.service.scheduler.task.MinMinPolicy
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
import kotlin.coroutines.Continuation
import kotlin.coroutines.CoroutineContext

class MinMinTest {
    @Test
    fun whenThereIsSingleHost_TasksAreOrderedByExecutionTime() = runBlockingSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 1000, "required-cpus" to 1)),
            Task(UUID(0L, 2L), "Task2", HashSet(),
                mutableMapOf("cpu-cycles" to 3000, "required-cpus" to 1)),
            Task(UUID(0L, 3L), "Task3", HashSet(),
                mutableMapOf("cpu-cycles" to 2000, "required-cpus" to 1)))

        val hostSpecs = mutableSetOf<HostSpec>()
        val spec = createHostSpec(1)
        // val host = computeHelper.registerHost(spec)
        hostSpecs.add(spec)

        val minmin = MinMinPolicy(hostSpecs)

        val cont = Continuation<Unit>(coroutineContext) { ; }
        val job = JobState(Job(UUID.randomUUID(), "onlyJob", tasks), 0, cont)
        val input = tasks.map({ TaskState(job, it) }).toHashSet()
        val orderedTasks = minmin.orderTasks(input)

        Assertions.assertEquals("Task1", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task3", orderedTasks.poll().task.name)
        Assertions.assertEquals("Task2", orderedTasks.poll().task.name)
    }

    @Test
    fun whenThereAreTwoHostsForTwoTasks_EveryTaskIsAssignedToOneOfTheHosts() = runBlockingSimulation {
        val tasks = hashSetOf(
            Task(UUID(0L, 1L), "Task1", HashSet(),
                mutableMapOf("cpu-cycles" to 1000, "required-cpus" to 1)),
            Task(UUID(0L, 2L), "Task2", HashSet(),
                mutableMapOf("cpu-cycles" to 1000, "required-cpus" to 1)))

        val hostSpecs = mutableSetOf<HostSpec>()
        repeat(2) {
            val spec = createHostSpec(it)
            // val host = computeHelper.registerHost(spec)
            hostSpecs.add(spec)
        }

        val minmin = MinMinPolicy(hostSpecs)

        val cont = Continuation<Unit>(coroutineContext) { ; }
        val job = JobState(Job(UUID.randomUUID(), "onlyJob", tasks), 0, cont)
        val input = tasks.map({ TaskState(job, it) }).toHashSet()
        val orderedTasks = minmin.orderTasks(input)

        Assertions.assertEquals("host-0", orderedTasks.poll().task.metadata["assigned-host"])
        Assertions.assertEquals("host-1", orderedTasks.poll().task.metadata["assigned-host"])
    }

    private fun createWorkflow() : Job {
        val tasks = HashSet<Task>()
        val workflow = Job(UUID.randomUUID(), "<unnamed>", tasks, HashMap())

        for (id in 0..9) {
            val task = Task(UUID(0L, id.toLong()), "task$id", HashSet(), mutableMapOf(
                "cpu-cycles" to 5000 * (id + 1),
                "required-cpus" to 1
            ))
            tasks.add(task)
        }
        return workflow
    }

    private fun createComputeService(context : CoroutineContext, clock : Clock): ComputeServiceHelper {
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        return ComputeServiceHelper(context, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))
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

    private fun createHostSpec(uid: Int): HostSpec {
        // Machine model based on: https://www.spec.org/power_ssj2008/results/res2020q1/power_ssj2008-20191125-01012.html
        val node = ProcessingNode("AMD", "am64", "EPYC 7742", 32)
        val cpus = listOf(ProcessingUnit(node, 1, 3400.0))
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


