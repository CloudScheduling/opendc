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

package nl.atlarge.opendc.kernel.messaging

import nl.atlarge.opendc.kernel.time.Duration
import nl.atlarge.opendc.topology.Entity

/**
 * A [Writable] instance allows entities to send messages.
 *
 * @author Fabian Mastenbroek (f.s.mastenbroek@student.tudelft.nl)
 */
interface Writable {
	/**
	 * Send the given message to the specified entity.
	 *
	 * @param msg The message to send.
	 * @param delay The amount of time to wait before the message should be received.
	 * @return A [Receipt] of the message that has been sent.
	 */
	suspend fun Entity<*>.send(msg: Any, delay: Duration = 0): Receipt

	/**
	 * Send the given message to the specified entity.
	 *
	 * @param msg The message to send.
	 * @param sender The sender of the message.
	 * @param delay The amount of time to wait before the message should be received.
	 * @return A [Receipt] of the message that has been sent.
	 */
	suspend fun Entity<*>.send(msg: Any, sender: Entity<*>, delay: Duration = 0): Receipt
}
