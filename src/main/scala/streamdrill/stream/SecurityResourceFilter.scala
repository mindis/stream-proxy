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

import java.security.Principal
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Response, SecurityContext}

import com.sun.jersey.spi.container.{ContainerRequest, ContainerRequestFilter, ResourceFilter}
import grizzled.slf4j.Logging
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.DigestUtils._

case class User(username: String, password: String, roles: List[String])
case class UserCredentials(username: String, password: String)

/**
 * The security resource filter configures access logic for the proxy.
 *
 * @author Matthias L. Jugel
 */
object SecurityResourceFilter extends Logging {
  var users: String => Option[User] = (_ => None)
  var realm: String = "secure"

  def apply(realm: String, users: String => Option[User]) {
    this.users = users
    this.realm = realm
  }
}

class SecurityResourceFilter(rolesAllowed: Option[List[String]]) extends ResourceFilter with ContainerRequestFilter with Logging {
  import streamdrill.stream.SecurityResourceFilter._

  def getRequestFilter = this

  def getResponseFilter = null

  def filter(request: ContainerRequest): ContainerRequest = {
    rolesAllowed match {
      case Some(roles) =>
        authenticate(credentials(request)) match {
          case Some(user: User) =>
            request.setSecurityContext(createSecurityContext(request, user))
            if (roles.exists(request.isUserInRole)) {
              debug("%s: authenticated sucessfully for %s".format(user.username, request.getPath))
              request
            } else {
              debug("%s: denied access to %s".format(user.username, request.getPath))
              throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN).build)
            }
          case None =>
            throw new WebApplicationException(
              Response
                  .status(Response.Status.UNAUTHORIZED)
                  .header("WWW-Authenticate", "Basic realm=\"" + realm + "\"").build
            )
        }
        request
      case None =>
        throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN).build)
    }
  }

  private final val BasicAuthDigest = """Basic (.+)""".r
  private final val UserAndPassword = """(.*):(.*)""".r

  /**
   * Extract credentials from request and return them.
   */
  protected def credentials(request: ContainerRequest): Option[UserCredentials] = {
    request.getHeaderValue("Authorization") match {
      case BasicAuthDigest(token: String) =>
        new String(Base64.decodeBase64(token)) match {
          case UserAndPassword(username, password) =>
            Some(UserCredentials(username, password))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  /**
   * Authenticate the credentials by looking up the user and return it if ok.
   */
  def authenticate(credentials: Option[UserCredentials]) = {
    credentials match {
      case Some(UserCredentials(username: String, password: String)) =>
        users(username) match {
          case Some(user) if verify(user, password) => Some(user)
          case _ => None
        }
      case None => None
    }
  }

  private val PW = """(^[^:]+):(.*)""".r

  protected def verify(user: User, password: String): Boolean = {
    user.password match {
      case PW("MD5", digest: String) => digest == md5Hex(password)
      case PW("SHA256", digest: String) => digest == sha256Hex(password)
      case PW("plain", plain: String) =>
        warn("%s: PLAIN PASSWORDS ARE DISCOURAGED. CHANGE IT!".format(user.username))
        plain == password
      case _ =>
        error("%s: password entry is unreadable".format(user.username))
        false
    }
  }

  protected def createSecurityContext(request: ContainerRequest, user: User): SecurityContext = {
    new SecurityContext {
      val principal = new Principal {
        def getName = user.username
      }

      def getAuthenticationScheme = SecurityContext.BASIC_AUTH

      def isSecure = request.isSecure

      def isUserInRole(r: String) = user.roles.contains(r)

      def getUserPrincipal = principal
    }
  }
}
