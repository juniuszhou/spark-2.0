package dcom

import org.apache.log4j.{Level, Logger}

object SetLogLevel {
  // call it as the first code line before init spark conf and context.
  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setLogLevels() {
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }
}
