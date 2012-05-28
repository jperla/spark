package spark

import java.net.InetAddress
import scala.math
import scala.math.random
import scala.io.Source
import scala.io.Source._
import scala.collection.mutable._
import scala.util.Random
import java.io._

import scala.collection.mutable.Map

object LocalRandom {
    val seed = InetAddress.getLocalHost().hashCode()
    val rand = new Random(seed)

    var tempTour = randomCycle(7663, rand)

    var data = ArrayBuffer.empty[Array[Double]]
    //for(line <- Source.fromFile("/home/princeton_ram/weakshared/ym7663.tsp").          getLines()) {
    for(line <- Source.fromFile("/Users/josephperla/weakshared/ym7663.tsp").          getLines()) {
            var city = new Array[Double](2)
            var node = line.split(' ')
            city(0) = node(1).toDouble
            city(1) = node(2).toDouble
            data += city
    }

    def getData() : ArrayBuffer[Array[Double]] = {
        return data
    }

    def getRandom () : Random = {
        return rand
    }

    def randomCycle (size: Int, rand: Random): ArrayBuffer[Int] =  {
        var randNodes = ArrayBuffer.empty[Int]
        for(i <- 0 until size) {
            randNodes += i
        }
        return rand.shuffle(randNodes)
    }
}


object ETSP {


    def randomCycle (size: Int, rand: Random): ArrayBuffer[Int] =  {
        var randNodes = ArrayBuffer.empty[Int]
        for(i <- 0 until size) {
            randNodes += i
        }
        return rand.shuffle(randNodes)
    }
    
    def perturbCycle (nodes: ArrayBuffer[Int], rand: Random) {
        val a = rand.nextInt(nodes.length)

        var temp = nodes(a)
        nodes(a) = nodes((a+1)%nodes.length)
        nodes((a+1)%nodes.length) = temp

    }

    def scoreCycle (nodes: ArrayBuffer[Int], points: ArrayBuffer[Array[Double]]): Double = {
        var distance : Double = 0
        for (i <- 0 until nodes.length){
            var p1 = points(nodes(i))
            var p2 = points(nodes((i+1)%nodes.length))
            distance += Math.sqrt((p1(1) - p2(1)) * (p1(1) - p2(1)) + (p1(0) - p2(0)) * (p1(0) - p2(0)))
        }
        return distance
    }
   
    
    
    def main(args: Array[String]) {
        if (args.length == 0) {
            System.err.println("Usage: eTSP <host> <input> <slices> <numIter>")
            System.exit(1)
        }

        val sc = new SparkContext(args(0), "EagerTSP")
        val slices = if (args.length > 2) args(2).toInt else 2
        val iter = if (args.length > 3) args(3).toInt else 100000
    
        /*
        val data  = ArrayBuffer.empty[Array[Double]]
        for(line <- Source.fromFile(args(1)).getLines()) {
            var city = new Array[Double](2)
            var node = line.split(' ')
            city(0) = node(1).toDouble
            city(1) = node(2).toDouble
            data += city
        }
       */
 
        //WeakShared.ws = new DoubleWeakSharable(Double.PositiveInfinity)
        var tour = sc.accumulator(new TSPState())(TSPStateAccumulatorParam)
        //data.foreach(p => p.foreach(q => println(q)))
        
        /*getting errors with broadcast variable even with their example. reading 
        everything in locally for now*/
        //var bdata = sc.broadcast(data)


        var bestTourScore = sc.eagerReducer(Double.PositiveInfinity, MinDoubleEagerVar.Reducer)
        var randomCounter = sc.eagerReducer(Double.PositiveInfinity, MinDoubleEagerVar.Reducer)
        //var modInts = sc.eagerReducer(Map[String,Int](), StringMaxIntMapEagerVar.Reducer)

        for (i <- sc.parallelize(1 to iter, slices)) {
            var rand = LocalRandom.getRandom()
            var shuffled = ArrayBuffer.empty[Int]
            
            /*
            if (tour.value.cost == WeakShared.ws.value){
                //throw new IOException("found an equal value")
                println("found equal value!!!")
                perturbCycle(LocalRandom.tempTour, rand)
                shuffled = LocalRandom.tempTour
            } else {
                shuffled = randomCycle(LocalRandom.tempTour.length, rand)
            }
            */

            shuffled = randomCycle(LocalRandom.tempTour.length, rand)
                
            var score = scoreCycle(shuffled, LocalRandom.getData())

            //WeakShared.ws.monotonicUpdate(new DoubleWeakSharable(score))
            bestTourScore.eagerReduce(score)

            if (i % 1000 == 0) {
                randomCounter.eagerReduce(-i)
            }
            var s = (i % 10).toString
            //odInts.eagerReduce((s, i / 1000))

            /*
            if ( i % 1000 == 0 ) {
                WeakShared.sendWeakShared(WeakShared.ws)
            }
            */

            LocalRandom.tempTour = shuffled

            var latestTour = new TSPState(shuffled, score)
            tour += latestTour
            var fromopt = score - 27603
            println("Score is : " + score + " difference is : " + fromopt + " best is : ")// + WeakShared.ws.value)
        }
        //println("the final tour is: " + tour)
        println("the final score is: " + tour.value.cost)

        sc.stop()
    }
}
