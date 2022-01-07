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
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.opendc.compute.service.scheduler.FilterScheduler
import org.opendc.compute.service.scheduler.filters.*
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
import org.opendc.simulator.compute.workload.SimFlopsWorkload
import org.opendc.simulator.core.runBlockingSimulation
import org.opendc.telemetry.compute.ComputeMetricExporter
import org.opendc.telemetry.compute.table.HostTableReader
import org.opendc.telemetry.sdk.metrics.export.CoroutineMetricReader
import org.opendc.trace.Trace
import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import org.opendc.workflow.service.internal.JobState
import org.opendc.workflow.service.scheduler.job.ElopAdmissionPolicy
import org.opendc.workflow.service.scheduler.job.SubmissionTimeJobOrderPolicy
import org.opendc.workflow.service.scheduler.task.NullTaskEligibilityPolicy
import org.opendc.workflow.service.scheduler.task.SubmissionTimeTaskOrderPolicy
import org.opendc.workflow.workload.WorkflowSchedulerSpec
import org.opendc.workflow.workload.WorkflowServiceHelper
import org.opendc.workflow.workload.toJobs
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class WorkflowServiceTest {
    val policyName = "ELoP"
    val basePath = System.getProperty("user.home") + "/OpenDC Test Automation/${policyName}"
    val readOutInterval = 10

    @BeforeAll
    fun setup() {
        // create the folder
        val file = File(System.getProperty("user.home") + "/OpenDC Test Automation/${policyName}").mkdirs()
    }

    @Test
    @DisplayName("Test for general elop logic")
    fun testElop() {
        val a = Task(UUID(0L, 1.toLong()), "A", HashSet(), hashMapOf("workflow:task:cores" to 1, "workload" to SimFlopsWorkload(4000, 0.8)))
        val b = Task(UUID(0L, 2.toLong()), "B", listOf(a).toHashSet(), hashMapOf("workflow:task:cores" to 2, "workload" to SimFlopsWorkload(2000, 0.5)))
        val c = Task(UUID(0L, 3.toLong()), "C", listOf(a).toHashSet(), hashMapOf("workflow:task:cores" to 3, "workload" to SimFlopsWorkload(3000, 0.4)))
        val d = Task(UUID(0L, 4.toLong()), "D", listOf(a,b).toHashSet(), hashMapOf("workflow:task:cores" to 3, "workload" to SimFlopsWorkload(1000, 1.0)))
        val e = Task(UUID(0L, 5.toLong()), "E", listOf(c,d).toHashSet(), hashMapOf("workflow:task:cores" to 2, "workload" to SimFlopsWorkload(2000, 0.5)))
        val f = Task(UUID(0L, 6.toLong()), "F", listOf(e).toHashSet(), hashMapOf("workflow:task:cores" to 1, "workload" to SimFlopsWorkload(1000, 0.1)))

        val job = Job(UUID(0L, 7.toLong()), "a job", listOf(a,b,c,d,e,f).toHashSet(), HashMap())

        val a2 = Task(UUID(0L, 8.toLong()), "A'", HashSet(), hashMapOf("workflow:task:cores" to 4, "workload" to SimFlopsWorkload(4000, 0.2)))
        val b2 = Task(UUID(0L, 9.toLong()), "B'", listOf(a2).toHashSet(), hashMapOf("workflow:task:cores" to 7, "workload" to SimFlopsWorkload(4000, 0.8)))

        val job2 = Job(UUID(0L, 10.toLong()), "a job", listOf(a2,b2).toHashSet(), HashMap())

        val trace = listOf(job, job2)

        /*assertAll(
            { assertEquals(job.calculateLop(), 5)},
            { assertEquals(job2.calculateLop(), 7)},
        )*/
    }

    /**
     * We run with a fixed environment kind and workload.
     * We vary the scale.
     * We observe makespan (s), energy spend (kWh) and utilization (%).
     */
    @ParameterizedTest(name= "{0} hosts")
    @ValueSource(ints = [2, 4, 6, 8, 10, 12, 14])
    @DisplayName("Experiment scale")
    fun experimentScale(numHosts : Int) {
        val traceName = "askalon_ee2_parquet" // TODO: change to right trace name
        val traceNameConverted = traceName.replace("_", "-")
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/${traceNameConverted}_${policyName}_homogeneous_scale${numHosts}_metrics.csv",
            "path_makespan" to "$basePath/${traceNameConverted}_${policyName}_homogeneous_scale${numHosts}_makespan.csv",
            "path_tasksOverTime" to "$basePath/${traceNameConverted}_${policyName}_homogeneous_scale${numHosts}_taksOvertime.csv",
            "path_hostInfo" to "$basePath/${traceNameConverted}_${policyName}_homogeneous_scale${numHosts}_hostInfo.csv",
            "path_variableStore" to "$basePath/${traceNameConverted}_${policyName}_homogeneous_scale${numHosts}_variableStore.csv",
            "host_function" to listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)})),
            "metric_readoutMinutes" to readOutInterval.toLong(),
            "tracePath" to "/$traceName",
            "traceFormat" to "wtf",
            "numberJobs" to 1031.toLong(), // TODO: depends on trace
        )
        elopExtension(config)
        testTemplate(config)
    }

    fun commonExperimentEnvironment(numHosts: Int, envKind : String): HashMap<String, Any> {
        val traceName = "shell_parquet" // TODO: change to right trace name
        val traceNameConverted = traceName.replace("_", "-")
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_metrics.csv",
            "path_makespan" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_makespan.csv",
            "path_tasksOverTime" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_taksOvertime.csv",
            "path_hostInfo" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_hostInfo.csv",
            "path_variableStore" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_variableStore.csv",
            "metric_readoutMinutes" to readOutInterval.toLong(),
            "tracePath" to "/$traceName",
            "traceFormat" to "wtf",
            "numberJobs" to 3403.toLong(), // TODO: depends on trace
        )
        elopExtension(config)
        return config
    }

    /**
     * We run with a fixed scale and workload.
     * We vary the environment kind.
     * We observe makespan (s), energy spend (kWh) and utilization (%).
     */
    @Test
    @DisplayName("Experiment Environment Homogeneous")
    fun experimentEnvironmentHomo() {
        val numHosts = 2 // TODO: change to right amount
        val config = commonExperimentEnvironment(numHosts, "homogeneous")
        config["host_function"] = listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)}))
        testTemplate(config)
    }

    /**
     * We run with a fixed scale and workload.
     * We vary the environment kind.
     * We observe makespan (s), energy spend (kWh) and utilization (%).
     */
    @Test
    @DisplayName("Experiment Environment Heterogeneous")
    fun experimentEnvironmentHetero() {
        val numHosts = 2 // TODO: change to right amount
        val config = commonExperimentEnvironment(numHosts, "heterogeneous")
        config["host_function"] = listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec2(id)}))
        testTemplate(config)
    }

    /**
     * We run with a fixed scale and environment kind.
     * We vary the workload.
     * We observe makespan (s), energy spend (kWh) and utilization (%).
     */
    @ParameterizedTest(name = "Workload {0}")
    @ValueSource(strings = ["shell_parquet", "Galaxy", "askalon-new_ee49_parquet", "askalon_ee2_parquet"])
    @DisplayName("Experiment Workload")
    fun experimentWorkload(traceName : String) {
        val numHosts = 2 // TODO: change to right amount
        val envKind = "homogeneous" // TODO: change if it shall be heterogeneous
        val traceNameConverted = traceName.replace("_", "-")
        val config = hashMapOf<String, Any>(
            "path_metrics" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_metrics.csv",
            "path_makespan" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_makespan.csv",
            "path_tasksOverTime" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_taksOvertime.csv",
            "path_hostInfo" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_hostInfo.csv",
            "path_variableStore" to "$basePath/${traceNameConverted}_${policyName}_${envKind}_scale${numHosts}_variableStore.csv",
            "host_function" to listOf(Pair(numHosts, { id : Int -> createHomogenousHostSpec(id)})), // TODO: change, if env changed
            "metric_readoutMinutes" to readOutInterval.toLong(),
            "tracePath" to "/$traceName",
            "traceFormat" to "wtf",
            "numberJobs" to 200.toLong(), // TODO: depends on trace
        )
        elopExtension(config)
        testTemplate(config)
    }



    fun elopExtension(config : HashMap<String, Any>) {
        val jobHostMapping = HashMap<JobState, Set<UUID>>()
        val elopConfig = hashMapOf<String, Any>(
            "filterSchedulerFilters" to listOf(ElopFilter(jobHostMapping)),
            "jobHostMapping" to jobHostMapping
        )
        config.putAll(elopConfig)
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
     *
     * OPTIONAL:
     * "filterSchedulerFilters" (List<HostFilter>) - list of filters for hosts to add to the standard set we are using
     * "jobHostMapping" (HashMap<JobState, Set<UUID>>) - mapping betweeen jobs and assigned hosts for them. Used in ELOP to exchange data between J1 and HostFilter
     */
    fun testTemplate(config : HashMap<String, Any>) = runBlockingSimulation {
        val hostFilters = mutableListOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.0))
        if (config["filterSchedulerFilters"] != null) {
            hostFilters.addAll(config["filterSchedulerFilters"] as List<HostFilter>)
        }

        // Configure the ComputeService that is responsible for mapping virtual machines onto physical hosts
        val computeScheduler = FilterScheduler(
            filters = hostFilters,
            weighers = listOf(VCpuWeigher(1.0, multiplier = 1.0))
        )
        val computeHelper = ComputeServiceHelper(coroutineContext, clock, computeScheduler, schedulingQuantum = Duration.ofSeconds(1))
        var readoutTime = Duration.ofSeconds(config["metric_readoutMinutes"] as Long)

        if (config["tracePath"] == "/Galaxy"){
            readoutTime = Duration.ofMinutes(config["metric_readoutMinutes"] as Long)
        }


        val metricsFile = BufferedWriter(FileWriter(config["path_metrics"] as String), 32768)
        val makespanFile = BufferedWriter(FileWriter(config["path_makespan"] as String), 32768)
        val tasksOverTimeFile = BufferedWriter(FileWriter(config["path_tasksOverTime"] as String), 32768)
        val variableStoreFile = BufferedWriter(FileWriter(config["path_variableStore"] as String), 32768)
//
//        val metricsFile = PrintWriter(config["path_metrics"] as String)
//        val makespanFile =  PrintWriter(config["path_makespan"] as String)
//        val tasksOverTimeFile = PrintWriter(config["path_tasksOverTime"] as String)
//        val variableStoreFile = PrintWriter(config["path_variableStore"] as String)

        metricsFile.appendLine("Timestamp(s),HostId,No# Tasks running,cpuUsage(CPU usage of all CPUs of the host in MHz),energyUsage(Power usage of the host in W)")
        makespanFile.appendLine("Makespan (s),Workflow Response time (s)")
        tasksOverTimeFile.appendLine("Time (s),Tasks #")
        variableStoreFile.appendLine("variable,value,unit")
        variableStoreFile.appendLine("readOutInterval,${readOutInterval},m")

        val hostFns = config["host_function"] as List<Pair<Int, (Int) -> HostSpec>>
        var offSet = 0
        for (elem in hostFns) {
            val hostCount = elem.first
            val hostFn = elem.second
            repeat(hostCount) { computeHelper.registerHost(hostFn(it+offSet)) }
            offSet += hostCount
        }

        // write generic infos about the host to special file
//        val hostInfoFile = PrintWriter(config["path_hostInfo"] as String)
        val hostInfoFile = BufferedWriter(FileWriter(config["path_hostInfo"] as String), 32768)

        hostInfoFile.appendLine("HostNo,maxCapacity(MHz)")
        for (host in computeHelper.hosts) {
            var maxCapacity = host.machine.cpus.sumOf { it.capacity }
            hostInfoFile.appendLine("${host.uid},${maxCapacity}")
        }
        hostInfoFile.close()

        // Configure the WorkflowService that is responsible for scheduling the workflow tasks onto machines
        val workflowScheduler = WorkflowSchedulerSpec(
            schedulingQuantum = Duration.ofMillis(100),
            jobAdmissionPolicy = ElopAdmissionPolicy(),
            jobOrderPolicy = SubmissionTimeJobOrderPolicy(),
            taskEligibilityPolicy = NullTaskEligibilityPolicy,
            taskOrderPolicy = SubmissionTimeTaskOrderPolicy()
        )
        val workflowHelper = WorkflowServiceHelper(coroutineContext, clock, computeHelper.service.newClient(), workflowScheduler)

        if (config["jobHostMapping"] != null) {
            workflowHelper.service.hosts = computeHelper.hosts.map { Pair(it.uid, it.model.cpuCount) }.toMutableSet()
            workflowHelper.service.jobHostMapping = config["jobHostMapping"] as HashMap<JobState, Set<UUID>>
        }

        val metricReader = CoroutineMetricReader(this, computeHelper.producers, object : ComputeMetricExporter(){
            var energyUsage = 0.0
            var cpuUsage = 0.0
            //Makespan

            override fun record(reader: HostTableReader){
                var host = reader.host.id
                var timeStamp = reader.timestamp.getEpochSecond()
                cpuUsage = reader.cpuUsage
                energyUsage = reader.powerTotal
                metricsFile.appendLine("${timeStamp},${host},${reader.guestsRunning},$cpuUsage,${energyUsage.toInt()}")

            }
        }, exportInterval = readoutTime)

        try {
            val trace = Trace.open(
                Paths.get(checkNotNull(WorkflowServiceTest::class.java.getResource(config["tracePath"] as String)).toURI()),
                format = config["traceFormat"] as String
            )

            coroutineScope {

                var jobs : List<Job>
                if (config["tracePath"] == "/askalon_ee2_parquet") { // we cut askalon ee2 because it is too long
                    jobs = trace.toJobs().sortedBy { it.metadata.getOrDefault("WORKFLOW_SUBMIT_TIME", Long.MAX_VALUE) as Long }
                    jobs = jobs.subList(0, 906)
                }
                else {
                    jobs = trace.toJobs()
                }
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

                // how long did we run in total?
                val milliSecToSec = 1000
                variableStoreFile.appendLine("totalRuntime,${clock.millis() / milliSecToSec},s")
            }
        } finally {
            workflowHelper.close()
            computeHelper.close()
            metricReader.close()
            metricsFile.close()
            makespanFile.close()
            tasksOverTimeFile.close()
            variableStoreFile.close()
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
