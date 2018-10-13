package chrism.sdsc.util

import scala.io.Source

object ResourceHandle {

  def loadResource(path: String, encoding: String = "UTF-8"): Iterator[String] =
    Source.fromInputStream(getClass.getResourceAsStream(path), encoding)
      .getLines()

  /** From Beginning Scala by David Pollak.
    *
    * @param param of type A; passed to func
    * @param func any function or whatever that matches the signature
    * @tparam A any type with {{{def close(): Unit}}}: Unit; Java's Closeable interface should be compatible
    * @tparam B any type including Any
    * @return of type B
    */
  def using[A <: {def close() : Unit}, B](param: A)(func: A => B): B =
    try {
      func(param)
    } finally {
      // close even when an Exception is caught
      if (param != null) {
        param.close()
      }
    }
}