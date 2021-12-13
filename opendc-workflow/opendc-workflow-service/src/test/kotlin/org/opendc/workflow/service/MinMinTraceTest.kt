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
import org.opendc.telemetry.compute.ComputeMetricExporter
import org.opendc.telemetry.compute.table.HostTableReader
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader
import org.opendc.trace.Trace
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.TaskReadyEligibilityPolicy
import org.opendc.workflow.service.scheduler.task.MinMinPolicy
import org.opendc.workflow.workload.WorkflowSchedulerSpec
import org.opendc.workflow.workload.WorkflowServiceHelper
import org.opendc.workflow.workload.toJobs
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import java.io.PrintWriter

/**
 * Integration test suite for the [WorkflowService].
 */
@DisplayName("MinMinTraceTest")
internal class MinMinTraceTest {
    /**
     * A large integration test where we check whether all tasks in some trace are executed correctly.
     */
    @Test
    fun testTraceHomoUnscaled() {
        val hostSpecs = setOf(
            createHostSpec1(0),
            createHostSpec1(1),
            createHostSpec1(2),
            createHostSpec1(3)
        )

        val paths = mapOf(
            "metrics" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-metrics-homo-unscaled.csv",
            "makespan" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-makespan-homo-unscaled.csv",
            "tasksOverTime" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-tasksOverTime-homo-unscaled.csv",
            "trace" to "/spec_trace-2_parquet" // "/askalon-new_ee11_parquet"
        )
        testTrace(hostSpecs, paths)
    }

    @Test
    fun testTraceHomoScaled() {
        val hostSpecs = setOf(
            createHostSpec1(0),
            createHostSpec1(1),
            createHostSpec1(2),
            createHostSpec1(3),
            createHostSpec1(4),
            createHostSpec1(5),
            createHostSpec1(6),
            createHostSpec1(7)
        )

        val paths = mapOf(
            "metrics" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-metrics-homo-scaled.csv",
            "makespan" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-makespan-homo-scaled.csv",
            "tasksOverTime" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-tasksOverTime-homo-scaled.csv",
            "trace" to "/spec_trace-2_parquet" // "/askalon-new_ee11_parquet"
        )
        testTrace(hostSpecs, paths)
    }

    @Test
    fun testTraceHeteroUnscaled() {
        val hostSpecs = setOf(
            createHostSpec1(0),
            createHostSpec1(1),
            createHostSpec2(2),
            createHostSpec2(3)
        )

        val paths = mapOf(
            "metrics" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-metrics-hetero-unscaled.csv",
            "makespan" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-makespan-hetero-unscaled.csv",
            "tasksOverTime" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-tasksOverTime-hetero-unscaled.csv",
            "trace" to "/spec_trace-2_parquet" // "/askalon-new_ee11_parquet"
        )

        testTrace(hostSpecs, paths)
    }

    @Test
    fun testTraceHeteroScaled() {
        val hostSpecs = setOf(
            createHostSpec1(0),
            createHostSpec1(1),
            createHostSpec1(2),
            createHostSpec1(3),
            createHostSpec2(0),
            createHostSpec2(1),
            createHostSpec2(2),
            createHostSpec2(3)
        )

        val paths = mapOf(
            "metrics" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-metrics-hetero-scaled.csv",
            "makespan" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-makespan-hetero-scaled.csv",
            "tasksOverTime" to System.getProperty("user.home") + "/OpenDC Test Automation/Min-Min"+"/minmin-tasksOverTime-hetero-scaled.csv",
            "trace" to "/spec_trace-2_parquet" // "/askalon-new_ee11_parquet"
        )

        testTrace(hostSpecs, paths)
    }

    fun testTrace(hostSpecs: Set<HostSpec>, paths: Map<String, String>) = runBlockingSimulation {
        val exportInterval = Duration.ofMinutes(1)

        val metricsFile = PrintWriter(paths["metrics"])
        val makespanFile =  PrintWriter(paths["makespan"])
        val tasksOverTimeFile = PrintWriter(paths["tasksOverTime"])

        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan (s),Workflow Response time (s)")
        tasksOverTimeFile.appendLine("Time (s),Tasks #")

        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val computeScheduler = AssignmentExecutionScheduler()
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        for (spec in hostSpecs)
            computeHelper.registerHost(spec)

        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
            taskEligibilityPolicy = TaskReadyEligibilityPolicy(),
            taskOrderPolicy = MinMinPolicy(hostSpecs),
        )

        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            var cpuIdleTime = 0L

            override fun record(reader: HostTableReader){
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerUsage
                metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,$energyUsage")
            }
        }, exportInterval = exportInterval)

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(MinMinTraceTest::class.java.getResource(paths["trace"])).toURI()),
                format = "wtf"
                // Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/trace2.gwf")).toURI()),
                // format = "gwf"
            )

            coroutineScope {
                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely

                val makespans = jobs.map { (it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long }) / 1000}
                val waitTimes = jobs.map { it.tasks.minOf {t -> t.metadata["startedAt"] as Long } - it.metadata["submittedAt"] as Long}
                val responseTimes = makespans.zip(waitTimes).map { it.first + it.second }

                val completedTasksOverTime : MutableList<Double> = mutableListOf()
                for (job in jobs){
                    for(task in job.tasks){
                        val result = when((task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) < 1000){
                            false -> (task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) / 1000
                            true -> (task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) / 1000.0
                        }
                        completedTasksOverTime.add(completedTasksOverTime.size,
                            (result).toDouble()
                        )
                    }
                }

                for (jobNo in 0 until jobs.size) {
                    makespanFile.appendLine("${makespans[jobNo]},${responseTimes[jobNo]}")
                }

                for ((key, value) in completedTasksOverTime.groupingBy { it }.eachCount().filter { it.value >= 1 }.entries){
                    tasksOverTimeFile.appendLine("$key,$value")
                }
            }
        } finally {
            workflowHelper.close()
            computeHelper.close()
            metricReader.close()
            metricsFile.close()
            makespanFile.close()
            tasksOverTimeFile.close()
        }

        val metrics = collectMetrics(workflowHelper.metricProducer)

        /*
        assertAll(
            { assertEquals(20, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(20 * 91, metrics.tasksSubmitted, "Not all tasks have been submitted") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
            { assertEquals(33214236L, clock.millis()) { "Total duration incorrect" } }
        )
         */
    }

    /**
     * Construct a [HostSpec] for a simulated host.
     */
    private fun createHostSpec1(uid: Int): HostSpec {
        val node = ProcessingNode("AMD", "am64", "EPYC 7742", 64)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 2450.0) }
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
        val node = ProcessingNode("Intel", "am64", "Xeon Platinum 8280L", 56)
        val cpus = List(node.coreCount) { ProcessingUnit(node, it, 2700.0) }
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
