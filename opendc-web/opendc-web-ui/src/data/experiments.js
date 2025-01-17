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

import { useQuery } from 'react-query'
import { fetchTraces } from '../api/traces'
import { fetchSchedulers } from '../api/schedulers'

/**
 * Configure the query defaults for the experiment endpoints.
 */
export function configureExperimentClient(queryClient, auth) {
    queryClient.setQueryDefaults('traces', { queryFn: () => fetchTraces(auth) })
    queryClient.setQueryDefaults('schedulers', { queryFn: () => fetchSchedulers(auth) })
}

/**
 * Return the available traces to experiment with.
 */
export function useTraces() {
    return useQuery('traces')
}

/**
 * Return the available schedulers to experiment with.
 */
export function useSchedulers() {
    return useQuery('schedulers')
}
