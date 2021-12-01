package org.opendc.workflow.service

import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Test
import org.opendc.trace.util.parquet.LocalParquetReader
import java.io.File
import java.nio.file.Path

class ParquetReaderTest {

    @Test
    fun readerTest() {
        // get file
        var path = File("/part.0.parquet")
        var a = LocalParquetReader<GenericRecord>(path)
        a

    }
}
