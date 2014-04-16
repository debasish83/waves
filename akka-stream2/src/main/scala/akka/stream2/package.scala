package akka

package object stream2 {
  private[stream2]type ==>[-I, +O] = Operation[I, O]

  // shorten entry point into operations DSL
  def operation[T]: Operation.Identity[T] = Operation.Identity()
}