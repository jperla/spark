package spark

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeMap

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.collection.mutable.HashSet

sealed trait EagerSharerMessage
case class SlaveLocation(slaveHost: String, slavePort: Int) extends EagerSharerMessage
case class EagerMessageToMaster(message: MapMessage[_,_]) extends EagerSharerMessage
case class EagerDiffToSlave(diff: EagerDiff[_,_]) extends EagerSharerMessage
case object StopEagerSharer extends EagerSharerMessage

class EagerMasterSenderActor(_serverUris: ArrayBuffer[(String, Int)]) extends DaemonActor with Logging {
    var diffsToSend = new HashMap[Long, EagerDiff[_,_]]
    var serverUris = _serverUris

    var diffsWillSend = diffsToSend.clone()
    var serverWillSend = 0
    var serverFirstSent = 0

    var mustStop = false

    def act() {
        val port = System.getProperty("spark.master.port").toInt
        RemoteActor.alive(port)
        RemoteActor.register('EagerMasterSender, self)
        logInfo("Registered actor on port " + port)

        while(true) {
          diffsToSend.synchronized {
            if (diffsToSend.size > 0) {
              for ((key, value) <- diffsToSend) {
                diffsWillSend.update(key, value)
              }
              // will send all diffs, so clear
              diffsToSend.clear()

              // todo: jperla: this is a tiny bit bad if there are lots of servers 
              // and lots of different shared variables updated a lot
              // not our applications though

              // keep track of this so we can stop when we loop back to this server
              serverFirstSent = serverWillSend
              println("new diff to send, new first server: " + serverFirstSent + "/" + serverUris.size)
            }
          }

          var host = ""
          var port = 0

          serverUris.synchronized {
            if (serverUris.size > 0) {
              val pair = serverUris(serverWillSend)
              host = pair._1
              port = pair._2
              //println("sending to server " + serverWillSend + " @ " + host + ":" + port)
            }
          }

          if (diffsWillSend.size > 0 && port > 0) {
            //println("diffs to send > 0!")
            // still diffs to send, and still new servers to send to

            // todo: jperla: this sets up and tears down a TCP connection, slow!
            // improve this by using keeping connections around, or using UDP or something
            // Q: can master actor handle keeping 800 connections open to slaves?

            // iterate over serveruris, and send messages
            var slave = RemoteActor.select(Node(host, port), 'EagerSharerSlave)
            // send all the diffs to this one server
            for(diff <- diffsWillSend.values) {
	      
              println("sending actual diff id: " + diff.id)
              slave ! EagerDiffToSlave(diff)
            }
            slave = null
          } else {
            // if no diffs; let them accumulate for 50 ms
            Thread.sleep(50)
          }

          // next loop, do the next server
          // do this outside the loop so that it sends to different server first every time
          // loop back around to 0 when hit the end
          // assumption: serverUris never shrinks
          if (serverUris.size > 0) {
            serverWillSend = (serverWillSend + 1) % serverUris.size
          } else {
            serverWillSend = 0
          }

          if (serverWillSend == serverFirstSent && diffsWillSend.size > 0) {
              // looped back to first server, so clear the diffs we would send
              println("sent to all servers, done")
              diffsWillSend.clear() 
          }

          if (mustStop) {
            println("has must stop true")
            exit()
          }
        }
    }

    def stop() {
      println("stop() called")
      mustStop = true
    }
}


class EagerMasterReceiverActor(masterSenderActor: EagerMasterSenderActor) extends DaemonActor with Logging {
  val uniqueServers = new HashSet[String]

  def act() {
    val port = System.getProperty("spark.master.port").toInt
    RemoteActor.alive(port)
    RemoteActor.register('EagerMasterReceiver, self)
    logInfo("Registered actor on port " + port)

    
    loop {
      react {
        case SlaveLocation(slaveHost: String, slavePort: Int) =>
          println("Asked to register new slave at " + slaveHost + ":" + slavePort)
          if (!uniqueServers.contains(slaveHost)) {
            uniqueServers.add(slaveHost)
            masterSenderActor.serverUris.synchronized {
              masterSenderActor.serverUris.append((slaveHost, slavePort))
            }
          }
          reply('OK)
        case EagerMessageToMaster(newVar: MapMessage[_,_]) =>
          println("Received message @ master from " + sender + " " + newVar.id + ":" + newVar)
          updateAndSendIfNeeded(newVar.id, newVar)
        case StopEagerSharer =>
          println("received StopEagerSharer")
          masterSenderActor.stop()
          reply('OK)
          exit()
      }
    }
  }

  def updateAndSendIfNeeded[G,T] (varId : Long, message : MapMessage[G,T]) {
        var oldVar = EagerVars.originals(varId).asInstanceOf[EagerVar[G,T]]
        var updateToSend = oldVar.reduceTop(message.message)

        if (updateToSend != null) {
            EagerVars.applyDiff(updateToSend) // need to apply to all local Master threads

            masterSenderActor.diffsToSend.synchronized {
                masterSenderActor.diffsToSend.put(updateToSend.id, updateToSend)
            }

            println("updateToSend is not null")
        } else {
            println("updateToSend is null")
        }
  }
}

class EagerSharerSlaveActor()
extends DaemonActor with Logging {

  def act() {
    val slavePort = System.getProperty("spark.slave.port").toInt
    RemoteActor.alive(slavePort)
    RemoteActor.register('EagerSharerSlave, self)
    logInfo("Registered actor on port " + slavePort)

    loop {
      react {
        case EagerDiffToSlave(diff: EagerDiff[_,_]) =>
          println("testing ws value after receiving" + diff)
          EagerVars.applyDiff(diff)
        case StopEagerSharer =>
          reply('OK)
          exit()
      }
    }
  }
}

class EagerSharer(isMaster: Boolean) extends Logging {
  var masterReceiverActor: AbstractActor = null
  var slaveActor: AbstractActor = null
  var masterSenderActor: AbstractActor = null

  private var serverUris = new ArrayBuffer[(String, Int)]

  println("is master: " + isMaster)
  
  if (isMaster) {
    val masterSenderActor = new EagerMasterSenderActor(serverUris)
    masterSenderActor.start()

    val sharer = new EagerMasterReceiverActor(masterSenderActor)
    sharer.start()
    masterReceiverActor = sharer
  } else {
    val host = System.getProperty("spark.master.host")
    val port = System.getProperty("spark.master.port").toInt
    masterReceiverActor = RemoteActor.select(Node(host, port), 'EagerMasterReceiver)

    val slaveActor = new EagerSharerSlaveActor()
    slaveActor.start()

    val slavePort = System.getProperty("spark.slave.port").toInt
    // todo: jperla: can we get host and port name from val slaveActor itself?
    masterReceiverActor !?  SlaveLocation(Utils.localIpAddress, slavePort)
  }

  def sendMapMessage[G,T](p: MapMessage[G,T]) {
    println("sending eager map message")
    masterReceiverActor ! EagerMessageToMaster(p)
  }
  
  def stop() {
    println("stop everything!")
    masterReceiverActor !? StopEagerSharer
    serverUris.clear()
    masterReceiverActor = null

    if (slaveActor != null) {
        slaveActor !? StopEagerSharer
        slaveActor = null
    }
  }
}
