import React from 'react'
import { Group } from 'react-konva'
import { Tile } from '../../../../shapes'
import { RACK_BACKGROUND_COLOR } from '../../../../util/colors'
import TileObject from '../elements/TileObject'
import RackSpaceFillContainer from '../RackSpaceFillContainer'
import RackEnergyFillContainer from '../RackEnergyFillContainer'

function RackGroup({ tile }) {
    return (
        <Group>
            <TileObject positionX={tile.positionX} positionY={tile.positionY} color={RACK_BACKGROUND_COLOR} />
            <Group>
                <RackSpaceFillContainer tileId={tile._id} positionX={tile.positionX} positionY={tile.positionY} />
                <RackEnergyFillContainer tileId={tile._id} positionX={tile.positionX} positionY={tile.positionY} />
            </Group>
        </Group>
    )
}

RackGroup.propTypes = {
    tile: Tile,
}

export default RackGroup
