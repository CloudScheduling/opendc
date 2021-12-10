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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.ComputeFilter
import org.opendc.compute.service.scheduler.filters.RamFilter
import org.opendc.compute.service.scheduler.filters.VCpuFilter
import org.opendc.compute.service.scheduler.weights.VCpuWeigher
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
import org.opendc.workflow.service.scheduler.job.ExecutionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.job.NullJobAdmissionPolicy
import org.opendc.workflow.service.scheduler.task.ExecutionTimeTaskOderPolicy
import org.opendc.workflow.service.scheduler.task.NullTaskEligibilityPolicy
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
@DisplayName("WorkflowService")
internal class WorkflowServiceTest {
    /**
     * A large integration test where we check whether all tasks in some trace are executed correctly.
     */
    @Test
    fun testTrace() = runBlockingSimulation {
        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val HOST_COUNT = 4
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        val metricsFile = PrintWriter("C:/scul/maxmin-metrics-homo-unscaled.csv")
        val makespanFile = PrintWriter("C:/scul/maxmin-makespan-homo-unscaled.csv")
        val tasksOverTimeFile = PrintWriter("C:/scul/maxmin-tasksOverTime-homo-unscaled.csv")
        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan(s)")
        tasksOverTimeFile.appendLine("Time(s),#Tasks")


        repeat(HOST_COUNT) { computeHelper.registerHost(createHomogenousHostSpec(it)) }
        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = ExecutionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = ExecutionTimeTaskOderPolicy(),
        )
        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            //Makespan

            override fun record(reader: HostTableReader){
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerUsage
                if(cpuUsage != 0.0 && energyUsage != 0.0){
                    metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,$energyUsage")
                    println("${reader.guestsRunning},$cpuUsage,$energyUsage")
                }
            }
        }, exportInterval = Duration.ofSeconds(1))

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/askalon-new_ee11_parquet")).toURI()),
//                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/spec_trace-2_parquet")).toURI()),
//                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/askalon-new_ee17_parquet")).toURI()),
                format = "wtf"
            )

            coroutineScope {

                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely
                val makespans = jobs.map { (it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long }) / 1000}
                val completedTasksOverTime : MutableList<Int> = mutableListOf()
                for(job in jobs){
                    for(task in job.tasks){
                        completedTasksOverTime.add(completedTasksOverTime.size,
                            ((task.metadata["finishedAt"] as Long - task.metadata["startedAt"]  as Long) / 1000).toInt()
                        )
                    }
                }

                for(span in makespans){
                    makespanFile.appendLine("$span")
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
        val path = System.getProperty("user.dir")

        println("Working Directory = $path")
        val metrics = collectMetrics(workflowHelper.metricProducer)
        print(metrics)

        assertAll(
            { assertEquals(20, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
        )
    }
    @Test
    fun testTraceHetero() = runBlockingSimulation {
        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val HOST_COUNT = 4
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        val metricsFile = PrintWriter("C:/scul/maxmin-metrics-hetero-unscaled.csv")
        val makespanFile = PrintWriter("C:/scul/maxmin-makespan-hetero-unscaled.csv")
        val tasksOverTimeFile = PrintWriter("C:/scul/maxmin-tasksOverTime-hetero-unscaled.csv")
        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan(s)")
        tasksOverTimeFile.appendLine("Time(s),#Tasks")

        repeat(HOST_COUNT/2) { computeHelper.registerHost(createHomogenousHostSpec(it)) }
        repeat(HOST_COUNT/2) { computeHelper.registerHost(createHomogenousHostSpec2(it)) }
        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = ExecutionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = ExecutionTimeTaskOderPolicy(),
        )
        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            //Makespan

            override fun record(reader: HostTableReader){
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerUsage
                if(cpuUsage != 0.0 && energyUsage != 0.0)
                    metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,$energyUsage")
            }
        }, exportInterval = Duration.ofSeconds(10))

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/askalon-new_ee11_parquet")).toURI()),
                format = "wtf"
            )

            coroutineScope {

                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely
                val makespans = jobs.map { it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long } }
                println(makespans)
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
        print(metrics)

        assertAll(
            { assertEquals(20, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
//            { assertEquals(33214236L, clock.millis()) { "Total duration incorrect" } }
        )
    }
    @Test
    fun testTraceHeteroScaled() = runBlockingSimulation {
        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val HOST_COUNT = 8
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        val metricsFile = PrintWriter("C:/scul/maxmin-metrics-homo-scaled.csv")
        val makespanFile = PrintWriter("C:/scul/maxmin-makespan-homo-scaled.csv")
        val tasksOverTimeFile = PrintWriter("C:/scul/maxmin-tasksOverTime-homo-scaled.csv")
        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan(s)")
        tasksOverTimeFile.appendLine("Time(s),#Tasks")

        repeat(HOST_COUNT/2) { computeHelper.registerHost(createHomogenousHostSpec(it)) }
        repeat(HOST_COUNT/2) { computeHelper.registerHost(createHomogenousHostSpec2(it)) }
        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = ExecutionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = ExecutionTimeTaskOderPolicy(),
        )
        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            //Makespan

            override fun record(reader: HostTableReader){
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerUsage
                if(cpuUsage != 0.0 && energyUsage != 0.0)
                    metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,$energyUsage")
            }
        }, exportInterval = Duration.ofSeconds(10))

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/askalon-new_ee11_parquet")).toURI()),
                format = "wtf"
            )


            coroutineScope {

                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely
                val makespans = jobs.map { it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long } }
                println(makespans)
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
        print(metrics)

        assertAll(
            { assertEquals(20, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
//            { assertEquals(33214236L, clock.millis()) { "Total duration incorrect" } }
        )
    }
    @Test
    fun testTraceHomoScaled() = runBlockingSimulation {
        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val HOST_COUNT = 8
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        val metricsFile = PrintWriter("C:/scul/maxmin-metrics-hetero-scaled.csv")
        val makespanFile = PrintWriter("C:/scul/maxmin-makespan-hetero-scaled.csv")
        val tasksOverTimeFile = PrintWriter("C:/scul/maxmin-tasksOverTime-hetero-scaled.csv")
        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan(s)")
        tasksOverTimeFile.appendLine("Time(s),#Tasks")

        repeat(HOST_COUNT) { computeHelper.registerHost(createHomogenousHostSpec(it)) }
        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = NullJobAdmissionPolicy,
            jobOrderPolicy = ExecutionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = ExecutionTimeTaskOderPolicy(),
        )
        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            //Makespan

            override fun record(reader: HostTableReader){
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerUsage
                if(cpuUsage != 0.0 && energyUsage != 0.0)
                    metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,$energyUsage")
            }
        }, exportInterval = Duration.ofSeconds(10))

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource("/askalon-new_ee11_parquet")).toURI()),
                format = "wtf"
            )

            coroutineScope {

                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely
                val makespans = jobs.map { it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long } }
                println(makespans)
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
        print(metrics)

        assertAll(
            { assertEquals(20, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
//            { assertEquals(33214236L, clock.millis()) { "Total duration incorrect" } }
        )
    }


    /**
     * Construct a [HostSpec] for a simulated host.
     */
    private fun createHomogenousHostSpec(uid: Int): HostSpec {
        // Machine model based on: https://www.spec.org/power_ssj2008/results/res2020q1/power_ssj2008-20191125-01012.html
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
    private fun createHomogenousHostSpec2(uid: Int): HostSpec {
        // Machine model based on: https://www.spec.org/power_ssj2008/results/res2019q3/power_ssj2008-20190520-00966.html
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
