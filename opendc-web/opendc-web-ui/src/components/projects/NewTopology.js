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

import PropTypes from 'prop-types'
import { PlusIcon } from '@patternfly/react-icons'
import { Button } from '@patternfly/react-core'
import { useState } from 'react'
import { useDispatch } from 'react-redux'
import { addTopology } from '../../redux/actions/topologies'
import NewTopologyModal from './NewTopologyModal'

function NewTopology({ projectId }) {
    const [isVisible, setVisible] = useState(false)
    const dispatch = useDispatch()

    const onSubmit = (name, duplicateId) => {
        dispatch(addTopology(projectId, name, duplicateId))
        setVisible(false)
    }
    return (
        <>
            <Button icon={<PlusIcon />} isSmall onClick={() => setVisible(true)}>
                New Topology
            </Button>
            <NewTopologyModal
                projectId={projectId}
                isOpen={isVisible}
                onSubmit={onSubmit}
                onCancel={() => setVisible(false)}
            />
        </>
    )
}

NewTopology.propTypes = {
    projectId: PropTypes.string,
}

export default NewTopology
