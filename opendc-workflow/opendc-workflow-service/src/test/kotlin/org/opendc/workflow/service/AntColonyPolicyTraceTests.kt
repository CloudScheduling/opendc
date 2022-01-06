/*
 * Copyright (c) 2021 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.workflow.service

import io.opentelemetry.sdk.metrics.export.MetricProducer
import kotlinx.coroutines.coroutineScope
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.opendc.compute.service.scheduler.AssignmentExecutionScheduler
import org.opendc.compute.workload.ComputeServiceHelper
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.kernel.SimSpaceSharedHypervisorProvider
import org.opendc.simulator.compute.model.MachineModel
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.power.LinearPowerModel
import org.opendc.simulator.compute.power.SimplePowerDriver
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.trace.Trace
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.AntColonyPolicy
import org.opendc.workflow.service.scheduler.task.Constants
import org.opendc.workflow.service.scheduler.task.TaskReadyEligibilityPolicy
import org.opendc.workflow.workload.WorkflowSchedulerSpec
import org.opendc.workflow.workload.WorkflowServiceHelper
import org.opendc.workflow.workload.toJobs
import java.nio.file.Paths
import java.time.Duration
import java.util.*

/**
 * Integration test suite for the [WorkflowService].
 */
@DisplayName("AntColonyPolicyTraceTest")
internal class AntColonyPolicyTraceTest {
    @Test
    fun testTrace() = runBlockingSimulation {
        val hostSpecs = setOf(
            createHostSpec1(0),
            createHostSpec1(1),
            createHostSpec2(2),
            createHostSpec2(3),
            createHostSpec3(4),
            createHostSpec3(5),
        )

        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val computeScheduler = AssignmentExecutionScheduler()
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        for (spec in hostSpecs)
            computeHelper.registerHost(spec)

        val acoConstants = Constants(numIterations = 20, numAnts = 100, alpha = 0.8, beta = 2.0, gamma = 10.0,
            initialPheromone = 10.0, rho = 0.3)
        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
            taskEligibilityPolicy = TaskReadyEligibilityPolicy(),
            taskOrderPolicy = AntColonyPolicy(hostSpecs.toList(), acoConstants)
        )

        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        try {
            val trace = Trace.open(
                // Paths.get(checkNotNull(MinMinTraceTest::class.java.getResource(paths["trace"])).toURI()),
                // format = "wtf"
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/trace.gwf")).toURI()),
                format = "gwf"
            )

            coroutineScope {
                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely

                val makespans = jobs.map { (it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long }) / 1000}
                val waitTimes = jobs.map { it.tasks.minOf {t -> t.metadata["startedAt"] as Long } - it.metadata["submittedAt"] as Long}
                val responseTimes = makespans.zip(waitTimes).map { it.first + it.second }

                val completedTasksOverTime : MutableList<Double> = mutableListOf()
                for (job in jobs) {
                    for (task in job.tasks) {
                        val result = when((task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) < 1000){
                            false -> (task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) / 1000
                            true -> (task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) / 1000.0
                        }
                        completedTasksOverTime.add(completedTasksOverTime.size,
                            (result).toDouble()
                        )
                    }
                }
            }
        } finally {
            workflowHelper.close()
            computeHelper.close()
        }

        val metrics = collectMetrics(workflowHelper.metricProducer)

        /*
        assertAll(
            { assertEquals(2, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(30, metrics.tasksSubmitted, "Not all tasks have been submitted") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") }
        )
         */

        println("${metrics.tasksFinished} tasks executed in ${clock.millis()}ms")
    }

    /**
     * Construct a [HostSpec] for a simulated host.
     */
    private fun createHostSpec1(uid: Int): HostSpec {
        val node = ProcessingNode("AMD", "am64", "EPYC 7742", 2)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 1000.0) }
        val memory = List(16) { MemoryUnit("Samsung", "Unknown", 3200.0, 16_000) }

        val machineModel = MachineModel(cpus, memory)

        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimplePowerDriver(LinearPowerModel(250.0, 50.0)),
            SimSpaceSharedHypervisorProvider()
        )
    }

    private fun createHostSpec2(uid: Int): HostSpec {
        val node = ProcessingNode("Intel", "am64", "Xeon Platinum 8280L", 4)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 2000.0) }
        val memory = List(12) { MemoryUnit("Samsung", "Unknown", 3200.0, 16_000) }

        val machineModel = MachineModel(cpus, memory)

        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimplePowerDriver(LinearPowerModel(250.0, 50.0)),
            SimSpaceSharedHypervisorProvider()
        )
    }

    private fun createHostSpec3(uid: Int): HostSpec {
        val node = ProcessingNode("Intel", "am64", "Xeon Platinum 8280L", 2)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 4000.0) }
        val memory = List(12) { MemoryUnit("Samsung", "Unknown", 3200.0, 16_000) }

        val machineModel = MachineModel(cpus, memory)

        return HostSpec(
            UUID(0, uid.toLong()),
            "host-$uid",
            emptyMap(),
            machineModel,
            SimplePowerDriver(LinearPowerModel(250.0, 50.0)),
            SimSpaceSharedHypervisorProvider()
        )
    }

    class WorkflowMetrics {
        var jobsSubmitted = 0L
        var jobsActive = 0L
        var jobsFinished = 0L
        var tasksSubmitted = 0L
        var tasksActive = 0L
        var tasksFinished = 0L
    }

    /**
     * Collect the metrics of the workflow service.
     */
    private fun collectMetrics(metricProducer: MetricProducer): WorkflowMetrics {
        val metrics = metricProducer.collectAllMetrics().associateBy { it.name }
        val res = WorkflowMetrics()
        res.jobsSubmitted = metrics["jobs.submitted"]?.longSumData?.points?.last()?.value ?: 0
        res.jobsActive = metrics["jobs.active"]?.longSumData?.points?.last()?.value ?: 0
        res.jobsFinished = metrics["jobs.finished"]?.longSumData?.points?.last()?.value ?: 0
        res.tasksSubmitted = metrics["tasks.submitted"]?.longSumData?.points?.last()?.value ?: 0
        res.tasksActive = metrics["tasks.active"]?.longSumData?.points?.last()?.value ?: 0
        res.tasksFinished = metrics["tasks.finished"]?.longSumData?.points?.last()?.value ?: 0
        return res
    }
}
