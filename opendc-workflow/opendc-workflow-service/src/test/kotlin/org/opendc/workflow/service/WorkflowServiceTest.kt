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
import org.junit.jupiter.api.BeforeAll
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
import java.io.File
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import java.io.PrintWriter
import kotlin.collections.HashMap

/**
 * Integration test suite for the [WorkflowService].
 * It is pretty easy: create a new test by copying a test and modify the parameters in the config.
 */
@DisplayName("WorkflowService")
internal class WorkflowServiceTest {
    val basePath = System.getProperty("user.home") + "/OpenDC Test Automation/Max-Min"

    @BeforeAll
    fun setup() {
        // create the folder
        val file = File(System.getProperty("user.home") + "/OpenDC Test Automation/Max-Min").mkdirs()
    }

    @Test
    fun testHomo4() {
        val numHosts = 4
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_homo_scale4_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_homo_scale4_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_homo_scale4_taksOvertime.csv",
            "host_function" to listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)})),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    @Test
    fun testHomo8() {
        val numHosts = 8
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_homo_scale8_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_homo_scale8_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_homo_scale8_taksOvertime.csv",
            "host_function" to listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)})),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    @Test
    fun testHomo16() {
        val numHosts = 16
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_homo_scale16_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_homo_scale16_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_homo_scale16_taksOvertime.csv",
            "host_function" to listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)})),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    @Test
    fun testHetro4() {
        val numHosts = 4
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_hetro_scale4_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_hetro_scale4_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_hetro_scale4_taksOvertime.csv",
            "host_function" to listOf(
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec(id)}),
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec2(id)}),
            ),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    @Test
    fun testHetro8() {
        val numHosts = 8
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_hetro_scale8_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_hetro_scale8_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_hetro_scale8_taksOvertime.csv",
            "host_function" to listOf(
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec(id)}),
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec2(id)}),
            ),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    @Test
    fun testHetro16() {
        val numHosts = 16
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/specTrace2_maxMin_hetro_scale16_metrics.csv",
            "path_makespan" to "$basePath/specTrace2_maxMin_hetro_scale16_makespan.csv",
            "path_tasksOverTime" to "$basePath/specTrace2_maxMin_hetro_scale16_taksOvertime.csv",
            "host_function" to listOf(
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec(id)}),
                Pair(numHosts / 2, { id : Int -> createHomogenousHostSpec2(id)}),
            ),
            "metric_readoutMinutes" to 10.toLong(),
            "tracePath" to "/spec_trace-2_parquet",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(),
        )
        testTemplate(config)
    }

    /**
     * used variables:
     * "path_metrics" (String) - path to csv file in which the metrics shall be stored
     * "path_makespan" (String) - path to csv file in which the makespan shall be stored
     * "path_tasksOverTime" (String) - path to csv file in which the tasks over time shall be stored
     * "host_function" (List<Pair<Int, (Int) -> HostSpec>>) - list of all hosts that shall be created with quantify (first elem in pair) and function with which hosts are created (2nd elem in pair)
     * "metric_readoutMinutes" (Long) - determines the interval in (OpenDC) minutes in which metrics are written to a file
     * "tracePath" (String) - path to the trace to run
     * "traceFormat" (String) - format of the trace to run
     * "numberJobs" (Long) - number of jobs in trace (used for assertions at end of test)
     */
    fun testTemplate(config : HashMap<String, Any>) = runBlockingSimulation {
        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val computeScheduler = FilterScheduler(
            filters = listOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0)),
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))

        val metricsFile = PrintWriter(config["path_metrics"] as String)
        val makespanFile =  PrintWriter(config["path_makespan"] as String)
        val tasksOverTimeFile = PrintWriter(config["path_tasksOverTime"] as String)

        metricsFile.appendLine("No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan (s),Workflow Response time (s)")
        tasksOverTimeFile.appendLine("Time (s),Tasks #")

        val hostFns = config["host_function"] as List<Pair<Int, (Int) -> HostSpec>>
        var offSet = 0
        for (elem in hostFns) {
            val hostCount = elem.first
            val hostFn = elem.second
            repeat(hostCount) { computeHelper.registerHost(hostFn(it+offSet)) }
            offSet += hostCount
        }
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
                energyUsage = reader.powerTotal
                metricsFile.appendLine("${reader.guestsRunning},$cpuUsage,${energyUsage.toInt()}")

            }
        }, exportInterval = Duration.ofMinutes(config["metric_readoutMinutes"] as Long))

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource(config["tracePath"] as String)).toURI()),
                format = config["traceFormat"] as String
            )

            coroutineScope {

                val jobs = trace.toJobs()
                workflowHelper.replay(jobs) // Wait for all jobs to be executed completely
                val makespans = jobs.map { (it.tasks.maxOf { t -> t.metadata["finishedAt"] as Long } - it.tasks.minOf {t -> t.metadata["startedAt"] as Long }) / 1000}

                val workflowWaitTime = jobs.map { (it.tasks.minOf {t -> t.metadata["startedAt"] as Long } - it.metadata["submittedAt"] as Long) / 1000}
                val completedTasksOverTime : MutableList<Double> = mutableListOf()

                for(job in jobs){
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
                val workflowResponseTime = (workflowWaitTime.indices).map { workflowWaitTime[it] + makespans[it] }

                (workflowWaitTime.indices).map{
                    makespanFile.appendLine("${makespans[it]},${kotlin.math.round(workflowResponseTime[it].toDouble())}")

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
            { assertEquals(config["numberJobs"] as Long, metrics.jobsSubmitted, "No jobs submitted") },
            { assertEquals(0, metrics.jobsActive, "Not all submitted jobs started") },
            { assertEquals(metrics.jobsSubmitted, metrics.jobsFinished, "Not all started jobs finished") },
            { assertEquals(0, metrics.tasksActive, "Not all started tasks finished") },
            { assertEquals(metrics.tasksSubmitted, metrics.tasksFinished, "Not all started tasks finished") },
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
