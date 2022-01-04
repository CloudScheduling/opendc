package org.opendc.workflow.service.scheduler.task

import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.compute.workload.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import java.util.*
import kotlin.Comparator
import kotlin.math.pow
import kotlin.random.Random

internal class Ant() {
    val Path: LinkedList<Pair<TaskState, ProcessingUnit>> = LinkedList()
    var TourMakespan: Long = 0L
}

internal data class Constants(val NumIterations: Int,
                              val NumAnts: Int,
                              val Alpha: Double,
                              val Beta: Double,
                              val InitialPheromone: Double,
                              val Rho: Double,
                              val Q: Double) {}

public class AntColonyPolicy(public val hosts: List<HostSpec>) : HolisticTaskOrderPolicy {
    private val _constants = Constants(NumIterations = 1000, NumAnts = 100, Alpha = 0.5, Beta = 0.5,
                                       InitialPheromone = 5.0, Rho = 0.4, Q = 100.0)
    private val _cores = hosts.flatMap { it.model.cpus }

    public override fun orderTasks(tasks: List<TaskState>): Queue<TaskState> {
        val chunkSize = _cores.size
        val chunks = tasks.chunked(chunkSize)

        for (chunk in chunks) {
            acoProc(chunk, _cores)
        }

        return LinkedList()
    }

    private fun acoProc(tasks: List<TaskState>, cores: List<ProcessingUnit>): Unit {
        var bestTour: List<Pair<TaskState, ProcessingUnit>> = emptyList()
        var bestTourMakespan = Long.MAX_VALUE

        val ants = setOf<Ant>()

        val pheromones = initializePheromones(_constants.InitialPheromone, tasks, cores)

        val execTimes = calculateExecutionTimes(tasks, cores)

        for (i in 0 until _constants.NumIterations) {
            for (ant in ants) {
                ant.Path.clear()
                val unvisitedCores = cores.toMutableList()

                val startTask = tasks[0]
                val startCore = unvisitedCores.random()
                ant.Path.addLast(Pair(startTask, startCore))
                unvisitedCores.remove(startCore)

                for (task in tasks.drop(1)) {
                    val selectedCore = selectCore(task,  unvisitedCores, execTimes)

                    ant.Path.addLast(Pair(task, selectedCore))
                    unvisitedCores.remove(selectedCore)

                    val execTime = execTimes.getValue(Pair(task, selectedCore))
                    ant.TourMakespan = maxOf(ant.TourMakespan, execTime)
                }

                //TODO Assign bestTour and bestTourMakespan
            }

            updatePheromoneLocally(pheromones, ants)
            updatePheromoneGlobally(pheromones, bestTour, bestTourMakespan)
        }
    }

    private fun initializePheromones(initialValue: Double, tasks: List<TaskState>, cores: List<ProcessingUnit>): MutableMap<Pair<TaskState, ProcessingUnit>, Double> {
        val pheromones = mutableMapOf<Pair<TaskState, ProcessingUnit>, Double>()
        for (task in tasks) {
            for (core in cores) {
                pheromones[Pair(task, core)] = initialValue
            }
        }
        return pheromones
    }

    private fun calculateExecutionTimes(tasks: List<TaskState>, cores: List<ProcessingUnit>): Map<Pair<TaskState, ProcessingUnit>, Long> {
        val execTimes = mutableMapOf<Pair<TaskState, ProcessingUnit>, Long>()
        for (task in tasks) {
            for (core in cores) {
                val cpuCycles = task.task.metadata["cpu-cycles"] as Long
                val execTime = cpuCycles / core.frequency
                execTimes[Pair(task, core)] = execTime.toLong()
            }
        }
        return execTimes
    }

    private fun selectCore(task: TaskState, unvisitedCores: List<ProcessingUnit>, execTimes: Map<Pair<TaskState, ProcessingUnit>, Long>,
                           pheromones: Map<Pair<TaskState, ProcessingUnit>, Double>): ProcessingUnit {
        val attractivenesses = mutableListOf<Double>()
        var attractivenessSum = 0.0
        for (i in unvisitedCores.indices) {
            val key = Pair(task, unvisitedCores[i])
            val pheromone = pheromones.getValue(key)
            val execTime = execTimes.getValue(key)

            val attractiveness = calculateAttractiveness(pheromone, execTime)
            attractivenesses[i] = attractiveness
            attractivenessSum += attractiveness
        }

        val toss = Random.nextDouble(attractivenessSum)
        var x = 0.0
        for (i in unvisitedCores.indices) {
            x += attractivenesses[i]
            if (x > toss) {
                return unvisitedCores[i]
            }
        }
        throw Exception("Toss was greater than attractivenessSum.")
    }

    private fun calculateAttractiveness(pheromone: Double, execTime: Long): Double {
        val desirability = 1.0 / execTime
        return pheromone.pow(_constants.Alpha) * desirability.pow(_constants.Beta)
    }

    private fun updatePheromoneLocally(pheromones: MutableMap<Pair<TaskState, ProcessingUnit>, Double>, ants: Set<Ant>) {
        for ((node, oldValue) in pheromones.entries) {
            pheromones[node] = (1 - _constants.Rho) * oldValue
        }

        for (ant in ants) {
            for (node in ant.Path) {
                val pheromoneDelta = _constants.Q / ant.TourMakespan
                val oldValue = pheromones.getValue(node)
                pheromones[node] = oldValue + pheromoneDelta
            }
        }
    }

    private fun updatePheromoneGlobally(pheromones: MutableMap<Pair<TaskState, ProcessingUnit>, Double>,
                                        bestTour: List<Pair<TaskState, ProcessingUnit>>, bestTourMakespan: Long) {
        for (node in bestTour) {
            val pheromoneDelta = _constants.Q / bestTourMakespan
            val oldValue = pheromones.getValue(node)
            pheromones[node] = oldValue + pheromoneDelta
        }
    }

    public override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
    }
}
