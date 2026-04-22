package rules

import java.util.logging.{FileHandler, Level, Logger, SimpleFormatter}

object logger {
  /**
   * Object `logger` configures and provides a centralized Logger instance.
   *
   * Logger details:
   * - Logs written to file at "logs/rule_engine.log" (appending to existing file)
   * - Uses SimpleFormatter for log message formatting
   * - Parent handlers disabled to prevent duplicate logging to console
   * - Logging level set to INFO (logs INFO and above)
   *
   */
  val logger: Logger = {
    val log = Logger.getLogger("OrderProcessingLogger")

    // Create logs directory
    val logDir = new java.io.File("logs")
    if (!logDir.exists()) logDir.mkdirs()

    val logPath = "logs/rule_engine.log"
    val fileHandler = new FileHandler(logPath, true)
    fileHandler.setFormatter(new SimpleFormatter())
    log.addHandler(fileHandler)
    log.setUseParentHandlers(false)
    log.setLevel(Level.INFO)
    log
  }
}