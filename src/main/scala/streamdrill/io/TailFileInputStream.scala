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

package streamdrill.io

import java.io._
import java.lang.Thread._
import java.nio.ByteBuffer

import scala.annotation.tailrec

/**
 * Tail a file channel bytewise and wait at end of file for new data.
 * Only stops waiting if the condition for stopping at EOF is met.
 *
 * @param file the file to read/tail
 * @param tail whether to start tailing the file at the current position
 * @author Matthias L. Jugel
 */

class TailFileInputStream(val file: File, tail: Boolean = false) extends InputStream {
  val channel = new FileInputStream(file).getChannel
  if (tail) channel.position(channel.size)

  val buffer: ByteBuffer = ByteBuffer.allocate(8192)
  buffer.limit(0)

  /**
   * The polling time to look for new data at the end of file.
   */
  def pollTimeout: Long = 10L

  /**
   * What to do at EOF. Override this method to handle this condition.
   * @return true to stop at EOF or false to continue waiting for more data
   */
  def eofCondition: Boolean = false


  /**
   * How many bytes are available in the channel
   */
  private def channelAvailable: Int = (channel.size - channel.position).toInt

  /**
   * Monitor the file channel until the file position changes.
   *
   * @throws EOFException if the condition for EOF is met
   */
  @tailrec
  private def monitor: Long = {
    if (channelAvailable > 0) return channelAvailable

    sleep(pollTimeout)
    if (eofCondition) {
      channel.close()
      return -1
    }

    monitor
  }

  /**
   * Fill our internal buffer with as much data as is available.
   * If there is not data available monitor the file and fill when data
   * becomes available or return -1 at the EOF condition.
   */
  private def fillBuffer: Long = {
    if (channel.isOpen) {
      buffer.clear
      var limit = channel.read(buffer)
      while (limit <= 0) {
        if (monitor < 0) return -1
        limit = channel.read(buffer)
      }
      buffer.position(0)
      buffer.limit(limit)
      return limit
    }
    -1
  }

  /**
   * Whatever is in the buffer is available.
   */
  override def available = {
    if (buffer.hasRemaining) buffer.remaining
    else scala.math.min(channelAvailable, buffer.capacity)
  }

  /**
   * Copy from our internal buffer to the target buffer. This is necessary to
   * make tail work with InputStream, which blocks for a single read() when
   * doing array copy effectively blocking everything until the complete
   * internal buffer provided is filled. Tries to fill its buffer when the
   * internal buffer is empty.
   */
  override def read(target: Array[Byte], position: Int, length: Int): Int = {
    if (!buffer.hasRemaining && fillBuffer < 0) return -1
    val n = scala.math.min(buffer.remaining, length)
    buffer.get(target, position, n)
    n
  }

  /**
   * Tail the file channel byte wise. Read from the internal buffer until it
   * is empty and then tries to fill before reading again.
   */
  def read: Int = {
    if (!buffer.hasRemaining && fillBuffer < 0) return -1
    buffer.get & 0xff // ensure we are reading an unsigned byte value
  }
}