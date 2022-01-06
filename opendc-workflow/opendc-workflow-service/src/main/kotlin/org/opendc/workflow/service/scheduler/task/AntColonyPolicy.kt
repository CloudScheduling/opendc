package org.opendc.workflow.service.scheduler.task

import org.opendc.compute.workload.topology.HostSpec
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import java.util.*
import kotlin.math.log10
import kotlin.math.pow
import kotlin.random.Random

internal class Core(val id: Int, val host: HostSpec, val frequency: Double) {
    private val execTimes: MutableMap<TaskState, Double> = mutableMapOf()
    private val scheduledTasks: MutableList<TaskState> = mutableListOf()
    private var activeTime: Double = 0.0
    private var committedTime: Double = 0.0

    fun getCompletionTimeForTask(task: TaskState): Double {
        return this.committedTime + this.activeTime + this.execTimes.getValue(task)
    }

    fun scheduleTask(task: TaskState) {
        this.scheduledTasks.add(task)
        this.activeTime += this.execTimes.getValue(task)
    }

    fun commitTask(task: TaskState) {
        this.committedTime += this.execTimes.getValue(task)
    }

    fun getActiveTime() = this.activeTime

    /**
     * Clear previous information and precompute execution times for tasks
     */
    fun precomputeExecTimes(tasks: List<TaskState>) {
        this.execTimes.clear()

        for (task in tasks) {
            val cpuCycles = task.task.metadata["cpu-cycles"] as Long
            val execTime = cpuCycles / frequency
            this.execTimes[task] = maxOf(execTime, 0.1)
        }
    }

    fun reset() {
        this.scheduledTasks.clear()
        this.activeTime = 0.0
    }
}

internal class Tour() {
    private val nodes: MutableList<Pair<TaskState, Core>> = mutableListOf()
    private var makespan: Double = 0.0

    fun addNode(task: TaskState, core: Core) {
        this.nodes.add(Pair(task, core))
        this.makespan = maxOf(this.makespan, core.getActiveTime())
    }

    fun getMakespan() = this.makespan
    fun getNodes() = this.nodes.toList()
}

internal class Ant(val id: Int) {
    private var _tour = Tour()
    val tour: Tour
        get() { return _tour }

    fun addNode(task: TaskState, core: Core) {
        core.scheduleTask(task)
        this.tour.addNode(task, core)
    }

    fun reset() {
        _tour = Tour()
    }
}

public data class Constants(val numIterations: Int,
                            val numAnts: Int,
                            val alpha: Double,
                            val beta: Double,
                            val gamma: Double,
                            val initialPheromone: Double,
                            val rho: Double) {}

public class AntColonyPolicy(private val hosts: List<HostSpec>, private val constants: Constants) : HolisticTaskOrderPolicy {
    private val _cores = hosts.flatMap { host -> host.model.cpus.map { Core(it.id, host, it.frequency) } }

    public override fun orderTasks(tasks: List<TaskState>): Queue<TaskState> {
        if (tasks.isEmpty())
            return LinkedList()

        _cores.forEach { it.precomputeExecTimes(tasks) }

        val goodTour = acoProc(tasks, _cores)

        for ((task, core) in goodTour.getNodes()) {
            task.task.metadata["assigned-host"] = Pair(core.host.uid, core.host.name)
            core.commitTask(task)
        }

        println("Result makespan: ${goodTour.getMakespan()}")

        // Tasks are ordered FCFS
        return LinkedList(tasks)
    }

    private fun acoProc(tasks: List<TaskState>, cores: List<Core>): Tour {
        val bestTours: MutableSet<Tour> = mutableSetOf()

        val ants = initializeAnts(constants.numAnts)
        val trails = initializeTrails(constants.initialPheromone, tasks, cores)

        for (i in 0 until constants.numIterations) {
            for (ant in ants) {
                ant.reset()
                for (core in cores)
                    core.reset()

                for (task in tasks) {
                    val selectedCore = selectCore(task, cores, trails)
                    ant.addNode(task, selectedCore)
                }

                val minMakespan = bestTours.firstOrNull()?.getMakespan() ?: Double.MAX_VALUE
                val currentMakespan = ant.tour.getMakespan()
                if (currentMakespan < minMakespan) {
                    bestTours.clear()
                    bestTours.add(ant.tour)
                } else if (currentMakespan == minMakespan) {
                    bestTours.add(ant.tour)
                }
            }

            val minMakespan = bestTours.first().getMakespan()
            updatePheromoneLocally(trails, ants, minMakespan)
            updatePheromoneGlobally(trails, bestTours, minMakespan)

            /*
            val taskInfos = mutableMapOf<TaskState, MutableList<String>>()
            for ((node, level) in trails) {
                taskInfos.putIfAbsent(node.first, mutableListOf())
                taskInfos.getValue(node.first).add("Core ${node.second.id}: $level")
            }
            for ((task, infos) in taskInfos) {
                println("Task ${task.task.uid.leastSignificantBits}: ${infos.joinToString()}")
            }
            */

            // println("Iteration $i: Makespan ${bestTours.first().getMakespan()}")
        }

        return bestTours.random()
    }

    private fun initializeAnts(numAnts: Int) : Set<Ant> {
        return (0 until numAnts).map { Ant(it) }.toSet()
    }

    private fun initializeTrails(initialValue: Double, tasks: List<TaskState>, cores: List<Core>): MutableMap<Pair<TaskState, Core>, Double> {
        val trails = mutableMapOf<Pair<TaskState, Core>, Double>()
        for (task in tasks) {
            for (core in cores) {
                trails[Pair(task, core)] = initialValue
            }
        }
        return trails
    }

    private fun selectCore(task: TaskState, cores: List<Core>, trails: Map<Pair<TaskState, Core>, Double>): Core {
        val attractivenesses = mutableListOf<Double>()
        var attractivenessSum = 0.0
        for (i in cores.indices) {
            val core = cores[i]
            val trailLevel = trails.getValue(Pair(task, core))
            val completionTime = core.getCompletionTimeForTask(task)

            val attractiveness = calculateAttractiveness(trailLevel, completionTime)
            attractivenesses.add(i, attractiveness)
            attractivenessSum += attractiveness
        }

        val toss = Random.nextDouble(attractivenessSum)
        var x = 0.0
        for (i in cores.indices) {
            x += attractivenesses[i]
            if (x > toss) {
                return cores[i]
            }
        }
        throw Exception("Toss was greater than attractivenessSum.")
    }

    private fun calculateAttractiveness(trailLevel: Double, completionTime: Double): Double {
        val desirability = 100.0 / maxOf(1.0, log10(completionTime)).pow(constants.beta)
        return trailLevel.pow(constants.alpha) * desirability.pow(constants.beta) + constants.gamma
    }

    private fun updatePheromoneLocally(trails: MutableMap<Pair<TaskState, Core>, Double>, ants: Set<Ant>, minMakespan: Double) {
        for ((node, oldValue) in trails.entries) {
            trails[node] = (1 - constants.rho) * oldValue
        }

        for (ant in ants) {
            for (node in ant.tour.getNodes()) {
                val pheromoneDelta = minMakespan / ant.tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta * 1.5
            }
        }
    }

    private fun updatePheromoneGlobally(trails: MutableMap<Pair<TaskState, Core>, Double>, bestTours: Set<Tour>, minMakespan: Double) {
        for (tour in bestTours) {
            for (node in tour.getNodes()) {
                val pheromoneDelta = minMakespan / tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta * 3.0
            }
        }
    }

    public override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
    }
}
