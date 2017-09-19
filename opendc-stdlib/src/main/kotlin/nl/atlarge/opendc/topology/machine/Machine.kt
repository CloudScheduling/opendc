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

import nl.atlarge.opendc.extension.destinations
import nl.atlarge.opendc.workload.Task
import nl.atlarge.opendc.kernel.Context
import nl.atlarge.opendc.kernel.Process
import nl.atlarge.opendc.topology.Entity

/**
 * A Physical Machine (PM) inside a rack of a datacenter. It has a speed, and can be given a workload on which it will
 * work until finished or interrupted.
 *
 * @author Fabian Mastenbroek (f.s.mastenbroek@student.tudelft.nl)
 */
class Machine : Entity<Machine.State>, Process<Machine> {
	/**
	 * The status of a machine.
	 */
	enum class Status {
		HALT, IDLE, RUNNING
	}

	/**
	 * The shape of the state of a [Machine] entity.
	 */
	data class State(val status: Status, val task: Task? = null)

	/**
	 * The initial state of a [Machine] entity.
	 */
	override val initialState = State(Status.HALT)

	/**
	 * Run the simulation kernel for this entity.
	 */
	override suspend fun Context<Machine>.run() {
		update(State(Status.IDLE))

		val cpus = outgoingEdges.destinations<Cpu>("cpu")
		val speed = cpus.fold(0, { acc, (speed, cores) -> acc + speed * cores }).toLong()
		var task: Task? = null

		while (true) {
			if (task != null) {
				if (task.finished) {
					task = null
					update(State(Status.IDLE))
				} else {
					task.consume(speed * delta)
				}
			}

			val msg = receive()
			if (msg is Task) {
				task = msg
				update(State(Status.RUNNING, task))
			}
		}
	}
}
