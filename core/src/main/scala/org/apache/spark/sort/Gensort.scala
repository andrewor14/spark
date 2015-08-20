package org.apache.spark.sort

import java.io.{File, PrintWriter}

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}


/**
 * Data generator using gensort provided by the sort benchmark. This writes generated data to
 * local disks on worker nodes.
 */
object Gensort {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Gensort [sizeInGB] [numParts] [skew-boolean] [output-dir]")
      System.exit(0)
    }
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val skew = args(2).toBoolean
    val dir = args(3) + s"/sort-${sizeInGB}g-$numParts" + (if (skew) "-skew" else "")
    val sc = new SparkContext(
      new SparkConf().setAppName(s"Gensort - $dir - " + (if (skew) "skewed" else "non-skewed")))
    genSort(sc, sizeInGB, numParts, dir, skew)
  }

  private def genSort(
      sc: SparkContext,
      sizeInGB: Int,
      numParts: Int,
      dir: String,
      skew: Boolean): Unit = {

    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Utils.readSlaves()

    // host, partition index, output path, stdout, stderr
    val output = new NodeLocalRDD[(String, Int, String, String, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node

        val start = recordsPerPartition * part
        if (!new File(dir).exists()) {
          new File(dir).mkdirs()
        }

        val outputFile = s"$dir/part$part.dat"
        val skewFlag = if (skew) " -s " else " "
        // e.g. /root/gensort/64/gensort -c -s -b35714286 -t1 35714286 /mnt/local/part1.dat
        val cmd = s"/root/gensort/64/gensort -c$skewFlag-b$start -t1 $recordsPerPartition $outputFile"
        val (_, stdout, stderr) = Utils.runCommand(cmd)
        Iterator((host, part, outputFile, stdout, stderr))
      }
    }.collect()

    output.foreach { case (host, part, outputFile, stdout, stderr) =>
      println(s"$part\t$host\t$outputFile\t$stdout\t$stderr")
    }

    // Write the checksum file; since we passed `-c` to gensort,
    // the checksum will go to the stderr of the process, one line per record
    val maybeSkew = if (skew) "-skew" else ""
    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts-gensort$maybeSkew.log"
    val writer = new PrintWriter(new File(checksumFile))
    try {
      output.foreach { case (_, _, _, _, stderr: String) => writer.write(stderr) }
    } finally {
      writer.close()
    }
    println(s"checksum output: $checksumFile")
  }
}
