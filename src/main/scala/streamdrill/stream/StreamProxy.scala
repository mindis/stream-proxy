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

import java.text.SimpleDateFormat
import java.util.Locale

import com.sun.jersey.spi.container.servlet.ServletContainer
import net.minidev.json.JSONArray
import net.minidev.json.parser.JSONParser
import org.eclipse.jetty.server.{Connector, Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.collection.JavaConverters._

/**
 * Stream Proxy that provides twitter streams from disk or live.
 *
 * @author Matthias L. Jugel
 */
object StreamProxy extends App {
  val AUTH_ID = "stream-proxy-auth"

//  // VERY IMPORTANT: make sure we can parse numbers correctly
//  JSON.globalNumberParser = {
//    v: String => if (v.contains(".")) {
//      v.toDouble
//    } else {
//      v.toLong
//    }
//  }

  val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH)

  private val version = {
    val tmp = getClass.getPackage.getImplementationVersion
    if (tmp == null) "<debug>" else "v" + tmp
  }

  def installVersion(version: String, server: Server) = {

    context
  }

  println("Stream Proxy %s".format(version))
  println("(c) streamdrill inc. All rights reserved.")
  println()

  if (args.length < 4) {
    println("usage: StreamProxy host port authfile dumpdir")
    System.exit(0)
  }

  try {
    val jsonParser = new JSONParser(JSONParser.MODE_JSON_SIMPLE | JSONParser.IGNORE_CONTROL_CHAR)
    val users = jsonParser.parse(scala.io.Source.fromFile(args(2), "UTF-8").mkString)
        .asInstanceOf[JSONArray].asScala.map {
      case data: java.util.Map[String, Any] @unchecked =>
        val id = data.get("id").asInstanceOf[String]
        val password = data.get("password").asInstanceOf[String]
        val roles = data.get("roles").asInstanceOf[java.util.List[String]].asScala.toList
        id -> new User(id, password, roles)
    }.toMap

    SecurityResourceFilter.realm = "stream proxy"
    SecurityResourceFilter.users = {id => users.get(id)}
  } catch {
    case e: Exception =>
      e.printStackTrace()
      println("The stream proxy needs an access file to protect its resources.")
      println("Please create the file %s and add the json data user records.".format(args(2)))
      System.exit(1)
  }

  val server = new Server()
  val connector: ServerConnector = new ServerConnector(server)
  connector.setHost(args(0))
  connector.setPort(args(1).toInt)
  server.setConnectors(Array[Connector](connector))

  val holder = new ServletHolder(classOf[ServletContainer])
  holder.setInitParameter(
    "com.sun.jersey.spi.container.ResourceFilters",
    "streamdrill.stream.SecurityResourceFilterFactory")
  holder.setInitParameter(
    "com.sun.jersey.config.property.resourceConfigClass",
    "com.sun.jersey.api.core.PackagesResourceConfig")
  holder.setInitParameter(
    "com.sun.jersey.config.property.packages",
    "streamdrill.stream")
  val context = new ServletContextHandler(server, "/1", ServletContextHandler.SESSIONS)
  context.addServlet(holder, "/*")
  context.setAttribute("dir", args(3))

  server.start()
  server.join()
}

