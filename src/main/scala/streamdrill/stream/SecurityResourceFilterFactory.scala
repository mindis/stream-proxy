
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

import java.util.Collections._
import javax.annotation.security.{DenyAll, PermitAll, RolesAllowed}

import com.sun.jersey.api.model.AbstractMethod
import com.sun.jersey.spi.container.{ResourceFilter, ResourceFilterFactory}

/**
 * A resource filter factory for the stack.
 *
 * @author Matthias L. Jugel
 */
class SecurityResourceFilterFactory extends ResourceFilterFactory {

  override def create(method: AbstractMethod): java.util.List[ResourceFilter] = {
    if (method.isAnnotationPresent(classOf[DenyAll])) {
      return singletonList(new SecurityResourceFilter(None))
    }

    val methodRolesAllowed = method.getAnnotation(classOf[RolesAllowed])
    if (methodRolesAllowed ne null) {
      return singletonList(new SecurityResourceFilter(Some(methodRolesAllowed.value.toList)))
    }

    if (method.isAnnotationPresent(classOf[PermitAll])) {
      return null
    }

    val resourceRolesAllowed = method.getResource.getAnnotation(classOf[RolesAllowed])
    if (resourceRolesAllowed ne null) {
      return singletonList(new SecurityResourceFilter(Some(resourceRolesAllowed.value.toList)))
    }

    null
  }
}
