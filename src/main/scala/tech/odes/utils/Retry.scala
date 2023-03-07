package tech.odes.utils

object Retry {
  /**
   * Retry invocation of given code.
   * @param attempts Number of attempts to try executing given code. -1 represents infinity.
   * @param pauseMs Number of backoff milliseconds.
   * @param retryExceptions Types of exceptions to retry.
   * @param code Function to execute.
   * @tparam A Type parameter.
   * @return Returns result of function execution or exception in case of failure.
   */
  def apply[A](attempts: Int, pauseMs: Long, retryExceptions: Class[_]*)(code: => A): A = {
    var result: Option[A] = None
    var success = false
    var remaining = attempts
    while (!success) {
      try {
        remaining -= 1
        result = Some(code)
        success = true
      }
      catch {
        case e: Exception =>
          if (retryExceptions.contains(e.getClass) && (attempts == -1 || remaining > 0)) {
            Thread.sleep(pauseMs)
          } else {
            throw e
          }
      }
    }
    result.get
  }
}
