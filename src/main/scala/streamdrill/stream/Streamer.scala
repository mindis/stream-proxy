
/*
 * Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package streamdrill.stream

import java.io._
import java.text.SimpleDateFormat
import java.util.zip.GZIPInputStream
import java.util.{Calendar, Date, Locale, UUID}

import grizzled.slf4j.Logging
import net.minidev.json.parser.JSONParser
import streamdrill.io.{TailFileInputStream, TailFileLineIterable}

/**
 * File streamer that reads the files and sends them to the output.
 *
 * @author Matthias L. Jugel
 */
object Streamer extends Logging {
  final val FILE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
  final val TWEET_DATE = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH)

  def apply(dir: File, os: OutputStream, start: Option[Calendar] = None, end: Option[Calendar] = None) {
    val UID = UUID.randomUUID
    val writer = new BufferedWriter(new OutputStreamWriter(os))

    val startDate = start match {
      case Some(c: Calendar) => c
      case None => Calendar.getInstance
    }
    val fileNameDate = cleanCalendar(startDate.getTime)

    info("%s: streaming starts at %s from file %s"
        .format(UID, startDate.getTime, FILE_DATE_FORMAT.format(fileNameDate.getTime)))

    val monitor = new Monitor(writer, 5000L)
    var streaming = true
    var waitingForFile = false
    while (streaming) {
      streamFile(dir, fileNameDate, start == None && fileNameDate.before(start)) match {
        case Some(in: InputStream) =>
          waitingForFile = false

          val base = FILE_DATE_FORMAT.format(fileNameDate.getTime)
          info("%s: found stream file for %s".format(UID, base))

          val reader: BufferedReader = new BufferedReader(new InputStreamReader(in))
          val lines = new TailFileLineIterable(reader)

          // skip to the timestamp if necessary
          if (fileNameDate.before(startDate)) skip(lines, fileNameDate.getTime, startDate.getTime, writer)

          debug("%s: streaming %s".format(UID, base))
          monitor.reader = Some(reader)

          try {
            lines.foreach {
              line =>
                try {
                  writer.write(line)
                  writer.write('\n')
                  //writer.flush()
                  monitor.keepAlive()
                } catch {
                  case e: IOException =>
                    monitor.completed = true
                    warn("%s: client has closed the connection: %s".format(UID, e.getMessage))
                    try {reader.close()} catch {case e: Exception => error("%s: error closing reader".format(UID), e)}
                    return
                }
            }
          } catch {
            case e: Exception =>
              warn("%s: stream input is gone: %s".format(UID, e.getMessage))
          }

          debug("%s: streaming %s finished".format(UID, base))

          fileNameDate.add(Calendar.DAY_OF_YEAR, 1)
        case None =>
          val currentDay = cleanCalendar(new Date())

          if (fileNameDate.after(currentDay)) {
            if (!waitingForFile) info("%s: waiting for %s".format(UID, fileNameDate.getTime))
            waitingForFile = true
            Thread.sleep(1000)
          } else {
            fileNameDate.add(Calendar.DAY_OF_YEAR, 1)
            warn("%s: skipping to %s".format(UID, fileNameDate.getTime))
          }
      }

      streaming = end match {
        case Some(d: Calendar) => !fileNameDate.after(d)
        case None => true
      }
    }
    monitor.completed = true
  }

  class Monitor(writer: Writer, timeout: Long = 5000L) {
    var reader: Option[Reader] = None
    var completed = false
    var lastMessage = 0L

    def keepAlive() {
      lastMessage = System.nanoTime
    }

    def await(condition: (Boolean) => Boolean = c => c) {
      while (!condition(completed)) {
        Thread.sleep(timeout)
        if (System.nanoTime - lastMessage > 10000000) {
          try {
            writer.write("\n")
            writer.flush()
          } catch {
            case e: Exception =>
              reader.foreach {
                r => try {r.close()} catch {case e: Exception => error("error closing reader", e)}
              }
          }
        }
      }
    }
  }

  /**
   * Skip lines until we arrive at the start time
   */
  private def skip(lines: TailFileLineIterable, initDate: Date, startDate: Date, writer: Writer) {
    info("skipping to start time: %s".format(startDate))
    warn("if this appears to be stuck, it waits for a new line in the tailed file")

    val jsonParser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR)
    val skipStart = System.nanoTime
    var firstTime: Long = 0
    var lastTime = startDate
    var skipCounter: Long = 10
    var lineCounter: Long = 0
    lines.find {
      line =>
        lineCounter += 1
        skipCounter -= 1
        if (skipCounter > 0) false
        else try {
          jsonParser.parse(line) match {
            case null => false // empty or non-parsable lines
            case json: java.util.Map[String, Any] @unchecked if json.containsKey("created_at") =>
              try {
                val d = json.get("created_at").asInstanceOf[String]
                lastTime = TWEET_DATE.parse(d)
                if (firstTime == 0) firstTime = lastTime.getTime
                val timeToTarget = startDate.getTime - lastTime.getTime
                skipCounter = timeToTarget / ((lastTime.getTime - firstTime) / (lineCounter + 1)) / 20
                info("%s, %dms to target, skipping next %d lines".format(d, timeToTarget, skipCounter))
                writer.write("\n")
                writer.flush()
                timeToTarget <= 1000
              } catch {
                case e: Exception => false
              }
          }
        } catch {
          case e: Exception =>
            error("failed to parse json line: %s: %s".format(line, e.getMessage))
            false
        }
    }
    info("now at %s".format(lastTime))
    info("skipping finished in %ds".format(((System.nanoTime - skipStart) / 1e9).toLong))
  }


  /**
   * Get the stream to follow. Returns a normal or a gzip input stream.
   * @param dir the directory of the files to tail
   * @param calendar the file name date
   * @param tail whether to seek to the end of the file (not for gzip)
   */
  private def streamFile(dir: File, calendar: Calendar, tail: Boolean): Option[InputStream] = {
    val baseName = FILE_DATE_FORMAT.format(calendar.getTime)
    val gzipFile = new File(dir, "%s.json.gz".format(baseName))
    if (gzipFile.exists) {
      debug("found %s, tailing = %s".format(gzipFile.getAbsolutePath, tail))
      return Some(new GZIPInputStream(tailStream(gzipFile, calendar, tail = false)))
    }

    val jsonFile = new File(dir, "%s.json".format(baseName))
    if (jsonFile.exists) {
      debug("found %s, tailing = %s".format(jsonFile.getAbsolutePath, tail))
      return Some(tailStream(jsonFile, calendar, tail))
    }

    None
  }

  /**
   * Get the tail stream for the corresponding date. Initialized the condition with the
   * next file to be created after this is finished (by date)
   */
  private def tailStream(file: File, calendar: Calendar, tail: Boolean): InputStream = {
    val nextFileNameDate = Calendar.getInstance
    nextFileNameDate.setTime(calendar.getTime)
    nextFileNameDate.add(Calendar.DAY_OF_YEAR, 1)
    val nextFileName = FILE_DATE_FORMAT.format(nextFileNameDate.getTime)
    val nextGzipFile = new File(file.getParentFile, "%s.json.gz".format(nextFileName))
    val nextJsonFile = new File(file.getParentFile, "%s.json".format(nextFileName))

    val currentDay = cleanCalendar(new Date())

    new TailFileInputStream(file, tail) {
      override def eofCondition = {
        nextGzipFile.exists || nextJsonFile.exists || nextFileNameDate.before(currentDay)
      }
    }
  }

  /**
   * Get a calendar with hour, minute and second set to zero.
   */
  private def cleanCalendar(date: Date): Calendar = {
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar
  }
}
