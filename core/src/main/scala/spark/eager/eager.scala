package spark

import java.io._
import org.apache.mesos._
import org.apache.mesos.Protos._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

trait EagerReducer[M,R] extends Serializable {
  def zero(initialValue: R): R

  def reduceLocal(oldVar: EagerVar[M,R], message: M) : Boolean
  def reduceTop(oldVar: EagerVar[M,R], message: M) : EagerDiff[M,R]
  def mapMessage (oldVar: EagerVar[M,R], message: M) : MapMessage[M,R]
}

trait MapMessage[M,R] extends Serializable {
  def id : Long
  def message : M
}

trait EagerDiff[M,R] extends Serializable {
    def update(original : EagerVar[M,R])
    def id : Long
}


// this is asymmetric!
class EagerVar[M,R] (
  @transient initialValue: R, reducer: EagerReducer[M,R]) extends Serializable
{
  val id = EagerVars.newId
  var value_ = initialValue
  val zero = reducer.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  EagerVars.register(this, true)

  def eagerReduce (message: M) = {
    var doSend = reducer.reduceLocal(this, message)
    if (doSend) {
        println("new value to send will be " + message)
        var upmm = reducer.mapMessage(this, message)

        val eagerSharer = SparkEnv.get.eagerSharer
        eagerSharer.sendMapMessage(upmm)
    }
  }

  def reduceTop (message: M) : EagerDiff[M,R] = {
    return reducer.reduceTop(this, message)
  }

  def updateValue (v : R) = {
    value_ = v
  }

  def updateWithoutSend (message: M) : Boolean = {
    // todo: don't send this, send intermediate object
    return reducer.reduceLocal(this, message)
  }

  def value = this.value_
  def value_= (t: R) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    // don't initialize to 0 use same one
    //value_ = zero
    deserialized = true
    EagerVars.register(this, false)
  }


  override def toString = value_.toString
}




// TODO: The multi-thread support in this is kind of lame; check
// if there's a more intuitive way of doing it right
private object EagerVars
{
  var Z = ArrayBuffer[Double]()
  var numIterations = 0


  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, EagerVar[_,_]]()
  val localVars = Map[Thread, Map[Long, EagerVar[_,_]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized { lastId += 1; return lastId }
    
  def register(a: EagerVar[_,_], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val vars = localVars.getOrElseUpdate(Thread.currentThread, Map())
      vars(a.id) = a
    }
  }

  def hasOriginal(id: Long) : Boolean = {
    return originals.contains(id)
  }

  // Clear the local (non-original) vars for the current thread
  def clear: Unit = synchronized { 
    //localVars.remove(Thread.currentThread)
  }

  // Get the values of the local vars for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, v) <- localVars.getOrElse(Thread.currentThread, Map()))
      ret(id) = v.value
    return ret
  }

  def applyDiff[M,R] (diff : EagerDiff[M,R]) = synchronized {
    for (thread <- localVars.keys) {
        // todo: jperla: apply()?  why not just localVars(thread) ?
        var localMap = localVars.apply(thread)
        val v = localMap(diff.id)
        var up = v.asInstanceOf[EagerVar[M,R]]
        diff.update(up)
    }
  }

  // Add values to the original vars with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[EagerVar[Any,Any]].updateWithoutSend(value)
      }
    }
  }
}
