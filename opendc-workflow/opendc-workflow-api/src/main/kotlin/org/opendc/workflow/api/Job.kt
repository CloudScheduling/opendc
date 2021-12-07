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

package org.opendc.workflow.api

import java.util.*
import kotlin.collections.HashSet
import kotlin.math.max

/**
 * A workload that represents a directed acyclic graph (DAG) of tasks with control and data dependencies between tasks.
 *
 * @property uid A unique identified of this workflow.
 * @property name The name of this workflow.
 * @property tasks The tasks that are part of this workflow.
 * @property metadata Additional metadata for the job.
 */
public data class Job(
    val uid: UUID,
    val name: String,
    val tasks: Set<Task>,
    val metadata: Map<String, Any> = emptyMap()
) {
    override fun equals(other: Any?): Boolean = other is Job && uid == other.uid

    override fun hashCode(): Int = uid.hashCode()

    override fun toString(): String = "Job(uid=$uid, name=$name, tasks=${tasks.size}, metadata=$metadata)"

    public fun calculateLop() : Int {
        // reverse the whole thing :)
        for (t in this.tasks) {
            for (t2 in t.dependencies) {
                t2.enables.add(t)
            }
        }

        // identify the start task -> task that comes first (get task with min submittedAt)
        val startTasks = this.tasks.filter { it.dependencies.isEmpty() }
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
                if (currElem.allDependenciesHaveTokens()) {
                    assignTokens.add(currElem)
                    elemsToRemove.add(currElem)
                    localCounter += currElem.metadata["workflow:task:cores"] as Int
                }
            }
            for (elem in assignTokens) elem.token = true
            nextLevel.addAll(newElements)
            nextLevel.removeAll(elemsToRemove)
            lop = max(localCounter, lop) // TODO: or do I need to count the CPU cores?
        }

        // if proxy introduced, remove it
        if (proxyIntroduced && proxyStart != null) {
            for (task in startTasks) {
                task.dependencies = task.dependencies.minus(proxyStart)
            }
        }

        return lop
    }
}
