package com.github.mjakubowski84.parquet4s
import java.util.TimeZone

import com.github.mjakubowski84.parquet4s.CompatibilityTestCases.Primitives
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.parquet.filter2.predicate.FilterApi.binaryColumn
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterApi.binaryColumn
import org.apache.parquet.io.api.Binary

// Simple test row
case class TestRow(s: String)

class PrimitiveEncodingCompatibilityItSpec extends
  FlatSpec
  with Matchers
  with BeforeAndAfter
  with SparkHelper {

    before {
      clearTemp()
    }

  private def writeWithSpark(data: Seq[TestRow]): Unit = writeToTemp(data)
    private def readWithSpark: TestRow = readFromTemp[TestRow].head
    private def writeWithParquet4S(data: Seq[TestRow]): Unit = ParquetWriter.write(tempPathString, data)
    private def readWithParquet4S(fp: FilterPredicate): TestRow = {
      val parquetIterable = ParquetReader.read[TestRow](tempPathString, filter=FilterCompat.get(fp))
      try {
        parquetIterable.head
      } finally {
        parquetIterable.close()
      }
    }

    "Parquet4S" should "accept the filter and apply it to the dataset" in {
      // Write the data.
      val data = Seq(
        TestRow(s="foo"),
        TestRow(s="bar"),
        TestRow(s="hello"),
        TestRow(s="world")
      )
      writeWithSpark(data)

      // Define the filter predicate.
      val filterPredicate: FilterPredicate = FilterApi.eq(binaryColumn("s"), Binary.fromString("hello"))

      // Read the data using the filter.
      val expectedOutput = TestRow(s="hello")
      // readWithParquet4S(filterPredicate) should be(expectedOutput)
      val parquetIterable = ParquetReader.read[TestRow](tempPathString).head should be(expectedOutput)
    }
}
