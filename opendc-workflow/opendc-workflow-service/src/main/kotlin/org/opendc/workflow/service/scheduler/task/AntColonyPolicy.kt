package org.opendc.workflow.service.scheduler.task

import org.opendc.compute.workload.topology.HostSpec
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import java.util.*
import kotlin.math.pow
import kotlin.random.Random

internal class Core(val id: Int, val host: HostSpec, val frequency: Double, tasks: List<TaskState>) {
    private val execTimes: MutableMap<TaskState, Double> = mutableMapOf()
    private val scheduledTasks: MutableList<TaskState> = mutableListOf()
    private var activeTime: Double = 0.0

    init {
        for (task in tasks) {
            val cpuCycles = task.task.metadata["cpu-cycles"] as Long
            val execTime = cpuCycles / frequency
            this.execTimes[task] = execTime
        }
    }

    fun getCompletionTimeForTask(task: TaskState): Double {
        return activeTime + execTimes.getValue(task)
    }

    fun scheduleTask(task: TaskState) {
        scheduledTasks.add(task)
        activeTime += execTimes.getValue(task)
    }

    fun getActiveTime() = activeTime

    fun reset() {
        scheduledTasks.clear()
        activeTime = 0.0
    }
}

internal class Tour() {
    private val nodes: MutableList<Pair<TaskState, Core>> = mutableListOf()
    private var makespan: Double = 0.0

    fun addNode(task: TaskState, core: Core) {
        nodes.add(Pair(task, core))
        makespan = maxOf(makespan, core.getActiveTime())
    }

    fun getMakespan() = makespan
    fun getNodes() = nodes.toList()
}

internal class Ant(val id: Int) {
    private var _tour = Tour()
    val tour: Tour
        get() { return _tour }

    fun addNode(task: TaskState, core: Core) {
        core.scheduleTask(task)
        tour.addNode(task, core)
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
                              val rho: Double,
                              val Q: Double) {}

public class AntColonyPolicy(private val hosts: List<HostSpec>, private val constants: Constants) : HolisticTaskOrderPolicy {
    public override fun orderTasks(tasks: List<TaskState>): Queue<TaskState> {
        if (tasks.isEmpty())
            return LinkedList()

        val cores = hosts.flatMap { host -> host.model.cpus.map { Core(it.id, host, it.frequency, tasks) } }
        val goodTour = acoProc(tasks, cores)

        for ((task, core) in goodTour.getNodes()) {
            task.task.metadata["assigned-host"] = Pair(core.host.uid, core.host.name)
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
                if (ant.tour.getMakespan() < minMakespan) {
                    bestTours.clear()
                    bestTours.add(ant.tour)
                } else if (ant.tour.getMakespan() == minMakespan) {
                    bestTours.add(ant.tour)
                }
            }

            updatePheromoneLocally(trails, ants)
            updatePheromoneGlobally(trails, bestTours)

            println("Iteration $i: Makespan ${bestTours.first().getMakespan()}")

            /*
            for (tour in bestTours) {
                printTour(tour)
            }

            val infoPerTask = mutableMapOf<TaskState, MutableList<String>>()
            for ((node, level) in trails) {
                infoPerTask.putIfAbsent(node.first, mutableListOf<String>())
                val info = "Core ${node.second.id}: ${level}"
                infoPerTask.getValue(node.first).add(info)
            }
            for ((task, infos) in infoPerTask.entries) {
                println("Task ${task.task.uid.leastSignificantBits}: " + infos.joinToString())
            }
            */
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
        val desirability = 1.0 / completionTime
        return trailLevel.pow(constants.alpha) * desirability.pow(constants.beta) + constants.gamma
    }

    private fun updatePheromoneLocally(trails: MutableMap<Pair<TaskState, Core>, Double>, ants: Set<Ant>) {
        for ((node, oldValue) in trails.entries) {
            trails[node] = (1 - constants.rho) * oldValue
        }

        for (ant in ants) {
            for (node in ant.tour.getNodes()) {
                val pheromoneDelta = constants.Q / ant.tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta
            }
        }
    }

    private fun updatePheromoneGlobally(trails: MutableMap<Pair<TaskState, Core>, Double>, bestTours: Set<Tour>) {
        for (tour in bestTours) {
            for (node in tour.getNodes()) {
                val pheromoneDelta = constants.Q / tour.getMakespan()
                val oldValue = trails.getValue(node)
                trails[node] = oldValue + pheromoneDelta
            }
        }
    }

    private fun printTour(tour: Tour) {
        println("Makespan: ${tour.getMakespan()}")
        val tasksPerCore = mutableMapOf<Core, MutableSet<TaskState>>()
        for ((task, core) in tour.getNodes()) {
            tasksPerCore.putIfAbsent(core, mutableSetOf())
            tasksPerCore.getValue(core).add(task)
        }

        for (core in tasksPerCore.keys) {
            val tasks = tasksPerCore[core]!!
            var activeTime = 0.0
            for (task in tasks) {
                activeTime += (task.task.metadata["cpu-cycles"] as Long) / core.frequency
            }
            println("    Core ${core.id}, active time: ${activeTime}, numTasks: ${tasks.size}")
        }
    }

    public override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
    }
}
