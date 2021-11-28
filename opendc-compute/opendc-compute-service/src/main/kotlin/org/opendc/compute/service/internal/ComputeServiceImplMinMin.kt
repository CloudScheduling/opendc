package org.opendc.compute.service.internal

import io.opentelemetry.api.metrics.MeterProvider
import org.opendc.compute.service.ComputeService
import org.opendc.compute.service.driver.HostListener
import org.opendc.compute.service.scheduler.ComputeScheduler
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext

public class ComputeServiceImplMinMin(
    private val context: CoroutineContext,
    private val clock: Clock,
    meterProvider: MeterProvider,
    private val scheduler: ComputeScheduler,
    private val schedulingQuantum: Duration
) : ComputeServiceImpl(context, clock, meterProvider, scheduler, schedulingQuantum) {
    override fun doSchedule() {
        val now = clock.millis()

        // have arr for ect (each elem = current min ect for server)
        // need to store pair of ect + host -> take host and add meta data to it

        // for each server
            // for each machine
                // min (calcECT, ects[server])

        // min ect
        // for each elem: ects -> min

        // with server + host matching

    }
}
