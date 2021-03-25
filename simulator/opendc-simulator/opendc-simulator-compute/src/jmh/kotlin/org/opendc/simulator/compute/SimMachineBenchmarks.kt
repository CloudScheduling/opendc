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

package org.opendc.simulator.compute

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.opendc.simulator.compute.model.MemoryUnit
import org.opendc.simulator.compute.model.ProcessingNode
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.simulator.compute.workload.SimWorkload
import org.opendc.simulator.utils.DelayControllerClockAdapter
import org.opendc.utils.TimerScheduler
import org.openjdk.jmh.annotations.*
import java.time.Clock
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@OptIn(ExperimentalCoroutinesApi::class)
class SimMachineBenchmarks {
    private lateinit var scope: TestCoroutineScope
    private lateinit var clock: Clock
    private lateinit var scheduler: TimerScheduler<Any>
    private lateinit var machineModel: SimMachineModel

    @Setup
    fun setUp() {
        scope = TestCoroutineScope()
        clock = DelayControllerClockAdapter(scope)
        scheduler = TimerScheduler(scope.coroutineContext, clock)

        val cpuNode = ProcessingNode("Intel", "Xeon", "amd64", 2)

        machineModel = SimMachineModel(
            cpus = List(cpuNode.coreCount) { ProcessingUnit(cpuNode, it, 1000.0) },
            memory = List(4) { MemoryUnit("Crucial", "MTA18ASF4G72AZ-3G2B1", 3200.0, 32_000) }
        )
    }

    @State(Scope.Thread)
    class Workload {
        lateinit var workloads: Array<SimWorkload>

        @Setup
        fun setUp() {
            workloads = Array(2) { createSimpleConsumer() }
        }
    }

    @Benchmark
    fun benchmarkBareMetal(state: Workload) {
        return scope.runBlockingTest {
            val machine = SimBareMetalMachine(scope.coroutineContext, clock, machineModel)
            return@runBlockingTest machine.run(state.workloads[0])
        }
    }

    @Benchmark
    fun benchmarkSpaceSharedHypervisor(state: Workload) {
        return scope.runBlockingTest {
            val machine = SimBareMetalMachine(coroutineContext, clock, machineModel)
            val hypervisor = SimSpaceSharedHypervisor()

            launch { machine.run(hypervisor) }

            val vm = hypervisor.createMachine(machineModel)

            try {
                return@runBlockingTest vm.run(state.workloads[0])
            } finally {
                vm.close()
                machine.close()
            }
        }
    }

    @Benchmark
    fun benchmarkFairShareHypervisorSingle(state: Workload) {
        return scope.runBlockingTest {
            val machine = SimBareMetalMachine(coroutineContext, clock, machineModel)
            val hypervisor = SimFairShareHypervisor()

            launch { machine.run(hypervisor) }

            val vm = hypervisor.createMachine(machineModel)

            try {
                return@runBlockingTest vm.run(state.workloads[0])
            } finally {
                vm.close()
                machine.close()
            }
        }
    }

    @Benchmark
    fun benchmarkFairShareHypervisorDouble(state: Workload) {
        return scope.runBlockingTest {
            val machine = SimBareMetalMachine(coroutineContext, clock, machineModel)
            val hypervisor = SimFairShareHypervisor()

            launch { machine.run(hypervisor) }

            coroutineScope {
                repeat(2) { i ->
                    val vm = hypervisor.createMachine(machineModel)

                    launch {
                        try {
                            vm.run(state.workloads[i])
                        } finally {
                            machine.close()
                        }
                    }
                }
            }
            machine.close()
        }
    }
}