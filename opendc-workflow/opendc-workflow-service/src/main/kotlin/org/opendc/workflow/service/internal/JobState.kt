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

package org.opendc.workflow.service.internal

import org.opendc.workflow.api.Job
import org.opendc.workflow.api.Task
import java.util.*
import kotlin.collections.HashMap
import kotlin.coroutines.Continuation
import kotlin.math.max

public class JobState(public val job: Job, public val submittedAt: Long, internal val cont: Continuation<Unit>) {
    /**
     * A flag to indicate whether this job is finished.
     */
    public val isFinished: Boolean
        get() = tasks.isEmpty()

    public var lop : Int = 0
        private set

    internal val tasks: MutableSet<TaskState> = mutableSetOf()

    override fun equals(other: Any?): Boolean = other is JobState && other.job == job

    override fun hashCode(): Int = job.hashCode()

    public fun calculateLop() : Int {
        this.lop = this.job.calculateLop()
        return lop
    }

    public val metadata : HashMap<String, Any> = HashMap()
}
