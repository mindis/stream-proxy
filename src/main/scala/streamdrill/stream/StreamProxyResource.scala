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

import java.io.{File, OutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import javax.annotation.security.RolesAllowed
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.ws.rs._
import javax.ws.rs.core.MediaType._
import javax.ws.rs.core.{Context, StreamingOutput}

import grizzled.slf4j.Logging

/**
 * The actual servlet resource that provides access to the proxy service.
 *
 * @author Matthias L. Jugel
 */

@Path("/stream")
class StreamProxyResource(@Context request: HttpServletRequest,
                           @Context response: HttpServletResponse) extends Logging {

  final val FORMAT = "{format:[.](json)}"
  final val DATEPARSER = new SimpleDateFormat("yyyyMMddHHmmss")
  final val FILE_DIR = new File(request.getServletContext.getAttribute("dir").asInstanceOf[String])

  @POST
  @Path("/tweets" + FORMAT)
  @Produces(Array(APPLICATION_JSON))
  @RolesAllowed(Array("subscriber", "administrator"))
  def retweets(@PathParam("format") format: String): StreamingOutput = {
    info("retweet live stream requested (%s)".format(request.getRemoteAddr))
    val o = new StreamingOutput {
      def write(os: OutputStream) {
        Streamer(FILE_DIR, os)
        debug("live: finished")
      }
    }
    o
  }

  @GET
  @Path("/tweets" + FORMAT)
  @Produces(Array(APPLICATION_JSON))
  @RolesAllowed(Array("subscriber", "administrator"))
  def retweetsGet(@PathParam("format") format: String): StreamingOutput = retweets(format)

  @POST
  @Path("/tweets/range" + FORMAT)
  @Produces(Array(APPLICATION_JSON))
  @RolesAllowed(Array("administrator"))
  def retweetRange(@PathParam("format") format: String,
                   @QueryParam("start") start: String,
                   @DefaultValue("UNLIMITED") @QueryParam("end") end: String) = {
    info("retweet range stream requested %s:%s (%s)".format(start, end, request.getRemoteAddr))

    new StreamingOutput {
      def write(os: OutputStream) {
        val startDate = Calendar.getInstance
        startDate.setTime(DATEPARSER.parse(start))
        if (end != "UNLIMITED") {
          val endDate = Calendar.getInstance
          endDate.setTime(DATEPARSER.parse(end))
          Streamer(FILE_DIR, os, Some(startDate), Some(endDate))
        } else {
          Streamer(FILE_DIR, os, Some(startDate))
        }
      }
    }
  }

  @GET
  @Path("/tweets/range" + FORMAT)
  @Produces(Array(APPLICATION_JSON))
  @RolesAllowed(Array("administrator"))
  def retweetRangeGet(@PathParam("format") format: String,
                      @QueryParam("start") start: String,
                      @DefaultValue("UNLIMITED") @QueryParam("end") end: String) =
    retweetRange(format, start, end)

}