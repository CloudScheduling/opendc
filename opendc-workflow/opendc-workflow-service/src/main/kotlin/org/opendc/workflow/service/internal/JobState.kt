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
import kotlin.collections.HashSet
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
        for (t in this.tasks) {
            t.task.enables = HashSet()
        }
        // reverse the whole thing, but only look at finished tasks! :)
        // this way, lop changes in between
        // no filters needed -> job has tasks automatically removed
        for (t in this.tasks) {
            for (t2 in t.dependencies) {
                t2.task.enables.add(t.task)
            }
        }

        // identify the start task -> task that comes first (get task with min submittedAt)
        val startTasks = this.tasks.filter { it.dependencies.isEmpty() }.map { it.task }
        if (startTasks.isEmpty()) {
            throw Exception("No start task found")
        }
        var startTask : Task?
        var proxyIntroduced = false
        var proxyStart : Task? = null
        if (startTasks.size > 1) {
            proxyIntroduced = true
            proxyStart = Task(UUID.randomUUID(), "Proxy-Start", HashSet(), hashMapOf("workflow:task:cores" to 0))
            proxyStart.enables = startTasks.toHashSet()
            for (task in startTasks) {
                task.dependencies = task.dependencies.plus(proxyStart)
            }
            startTask = proxyStart
        }
        else {
            startTask = startTasks[0]
        }
        startTask.token = true
        // do breitensuche: for each path -> give token
        var nextLevel = HashSet<Task>()
        nextLevel.add(startTask)

        var lop = 0
        while (nextLevel.isNotEmpty()) {
            var localCounter = 0
            var newElements = HashSet<Task>()
            var elemsToRemove = HashSet<Task>()
            var assignTokens = HashSet<Task>()
            for (currElem in nextLevel) {
                newElements.addAll(currElem.enables) // set -> unique
                if (currElem.allDependenciesHaveTokens()) { // counts proxyStart, but that node has 0 cpus -> no influence on LOP
                    assignTokens.add(currElem)
                    elemsToRemove.add(currElem)
                    localCounter += currElem.metadata["workflow:task:cores"] as Int
                }
            }
            for (elem in assignTokens) elem.token = true
            nextLevel.addAll(newElements)
            nextLevel.removeAll(elemsToRemove)
            lop = max(localCounter, lop)
        }

        // if proxy introduced, remove it
        if (proxyIntroduced && proxyStart != null) {
            for (task in startTasks) {
                task.dependencies = task.dependencies.minus(proxyStart)
            }
        }

        this.lop = lop
        return lop
    }

    public val metadata : HashMap<String, Any> = HashMap()
}
