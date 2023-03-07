package tech.odes.utils

import org.slf4j.LoggerFactory

trait Logging {
  final val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
}
