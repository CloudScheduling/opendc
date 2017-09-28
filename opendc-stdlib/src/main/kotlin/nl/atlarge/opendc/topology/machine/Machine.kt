/*
 * MIT License
 *
 * Copyright (c) 2017 atlarge-research
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

package nl.atlarge.opendc.topology.machine

import mu.KotlinLogging
import nl.atlarge.opendc.topology.destinations
import nl.atlarge.opendc.platform.workload.Task
import nl.atlarge.opendc.kernel.Context
import nl.atlarge.opendc.kernel.Process
import nl.atlarge.opendc.kernel.time.Duration
import nl.atlarge.opendc.topology.Entity

/**
 * A Physical Machine (PM) inside a rack of a datacenter. It has a speed, and can be given a workload on which it will
 * work until finished or interrupted.
 *
 * @author Fabian Mastenbroek (f.s.mastenbroek@student.tudelft.nl)
 */
open class Machine : Entity<Machine.State>, Process<Machine> {
	/**
	 * The logger instance to use for the simulator.
	 */
	private val logger = KotlinLogging.logger {}

	/**
	 * The status of a machine.
	 */
	enum class Status {
		HALT, IDLE, RUNNING
	}

	/**
	 * The shape of the state of a [Machine] entity.
	 *
	 * @property status The status of the machine.
	 * @property task The task assign to the machine.
	 * @property memory The memory usage of the machine (defaults to 50mb for the kernel)
	 * @property load The load on the machine (defaults to 0.0)
	 * @property temperature The temperature of the machine (defaults to 23 degrees Celcius)
	 */
	data class State(val status: Status,
					 val task: Task? = null,
					 val memory: Int = 50,
					 val load: Double = 0.0,
					 val temperature: Double = 23.0)

	/**
	 * The initial state of a [Machine] entity.
	 */
	override val initialState = State(Status.HALT)

	/**
	 * Run the simulation kernel for this entity.
	 */
	override suspend fun Context<Machine>.run() {
		update(State(Status.IDLE))

		val interval: Duration = 10
		val cpus = outgoingEdges.destinations<Cpu>("cpu")
		val speed = cpus.fold(0, { acc, cpu -> acc + cpu.clockRate * cpu.cores })

		var task: Task = receiveTask()
		update(State(Status.RUNNING, task, load = 1.0, memory = state.memory + 50, temperature = 30.0))
		task.run { start() }

		while (true) {
			if (task.remaining <= 0) {
				task.run { finalize() }
				logger.info { "${entity.id}: Task ${task.id} finished. Machine idle at $time" }
				update(State(Status.IDLE))
				task = receiveTask()
			} else {
				task.run { consume(speed * delta) }
			}

			// Check if we have received a new order in the meantime.
			val msg = receive(interval)
			if (msg is Task) {
				task = msg
				update(State(Status.RUNNING, task, load = 1.0, memory = state.memory + 50, temperature = 30.0))
				task.run { start() }
			}
		}
	}

	/**
	 * Wait for a [Task] to be received by the [Process] and discard all other messages received in the meantime.
	 *
	 * @return The task that has been received.
	 */
	private suspend fun Context<Machine>.receiveTask(): Task {
		while (true) {
			val msg = receive()
			if (msg is Task)
				return msg
		}
	}
}
