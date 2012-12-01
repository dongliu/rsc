//== WFii -- Wide Finder 2 for Scala 2.8.0.Beta1-prerelease
//== Author: Rex Kerr    Last update: 2010 02 17     License: LGPL2.1

//== updated by Dong Liu to compile and run it with Ruby 1.8.7

//== Preamble section =================================================

import java.io._
import java.nio._
import java.nio.channels._
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic._
import scala.actors._
import scala.collection.mutable._
import java.util.Arrays.copyOfRange
import System.arraycopy

object Handy {
  def notnullOrElse[T>:Null<:AnyRef](t: T,f: => T) = if (t eq null) f else t
  def bigEnough(c: Chunk, size: Int) = {
    if (c==null || c.data.length<size) new Chunk(new Array[Byte](size),0,size) else c.to(size)
  }
  implicit def string2chunk(s: String) = new Chunk(s.getBytes)
  def highbit(i: Int) = {
    var (j,k) = (-1,i)
    while (k!=0) { j+=1 ; k = k>>>1 }
    if (j<0) 0 else 1<<j
  }
  case object Ping { }
}
import Handy._

//== Low Level Library section ========================================

class Chunk(val data: Array[Byte], var start: Int, var end: Int) {
  private var hash = 0

  def this(data0: Array[Byte]) = this(data0,0,data0.length)

  def size = end-start
  def to(end0: Int) = { start=0; end=end0; hash=0; this }
  def set(start0: Int, end0: Int) = { start=start0; end=end0; hash=0; this }
  def set(c: Chunk) = { start = c.start; end = c.end; hash = 0; this }
  def set(c: Chunk, ds: Int, de: Int) = {
    start = c.start + ds; end = c.end + de; hash = 0; this
  }
  def copy = new Chunk(data,start,end)
  def deepcopy = new Chunk( copyOfRange(data,start,end) , 0 , size )

  def toInt = {
    var sum = 0
    var i = start
    while (i<end) { sum = sum*10 + (data(i)-'0':Int); i += 1 }
    sum
  }
  def is(c: Chunk) = {
    if (size != c.size) false
    else {
      var i = start
      var j = c.start
      while (i<end && data(i)==c.data(j)) { i += 1; j += 1 }
      i == end
    }
  }
  def startsWith(c: Chunk) = {
    if (size < c.size) false
    else {
      var i = start
      var j = c.start
      while (j<c.end && data(i)==c.data(j)) { i += 1; j += 1 }
      j == c.end
    }
  }
  override def hashCode = {
    if (hash!=0) hash
    else {
      var i = start
      while (i < end) { hash = hash*113 + data(i); i += 1 }
      hash
    }
  }
  override def toString = new String(data,start,size)
  override def equals(o: Any) = o match {
    case c: Chunk => this is c
    case _ => false
  }
}

class ChunkIterator(delim: Byte) extends Iterator[Chunk] {
  protected var start,end = 0
  protected var ch = null:Chunk
  def reset { ch = null }
  def over(master: Chunk) = {
    start = master.start ; end = master.end
    if (ch != null && (ch.data eq master.data)) ch.set(start,start)
    else ch = new Chunk(master.data,start,start)
    this
  }
  def hasNext = ch.end < end
  def next = {
    ch.start = ch.end
    while (ch.end < end && ch.data(ch.end)!=delim) ch.end += 1
    if (ch.end < end) ch.end += 1
    ch
  }
}

class FileGobbler(f: File, bsize: Int, val nblocks: Int) extends Thread {
  val more = new AtomicBoolean(true)
  val ready = new LinkedBlockingQueue[ByteBuffer](nblocks)
  val recycled = new LinkedBlockingQueue[ByteBuffer]()
  val fis = new FileInputStream(f)
  val fc = fis.getChannel
  override def run() {
    setPriority(Thread.MAX_PRIORITY)
    while(more.get) {
      val block = notnullOrElse(recycled.poll, ByteBuffer.allocateDirect(bsize))
      fc.read(block)
      ready.put(block)
    }
    fis.close()
  }
}

class FileChunker(val gobbler: FileGobbler, val nblocks: Int, trim: Chunk => Int)
extends Thread {
  val more = new AtomicBoolean(true)
  val ready = new LinkedBlockingQueue[Chunk](nblocks)
  val recycled = new LinkedBlockingQueue[Chunk]()
  var saved = new Array[Byte](0)
  override def run() {
    setPriority(Thread.MAX_PRIORITY)
    while (more.get) {
      val block = gobbler.ready.take()
      block.flip
      val chunk = bigEnough(recycled.poll, block.remaining+saved.length)
      arraycopy(saved,0,chunk.data,0,saved.length)
      block.get(chunk.data,saved.length,block.remaining)
      val last = (saved.length max trim(chunk)+1) min chunk.end
      saved = copyOfRange(chunk.data,last,chunk.end)
      chunk.end = last
      ready.put(chunk)
      block.clear
      gobbler.recycled.put(block)
    }
    gobbler.more.set(false) ; gobbler.ready.clear
  }
}

trait Accumulator[T >: Null <: AnyRef] {
  def key: T
  def ++(): Unit
  def +=(a: Accumulator[T]): Unit
  override def hashCode = key.hashCode
  override def equals(o: Any) = o match {
    case a: Accumulator[_] => key == a.key
    case _ => false
  }
}

abstract class HashCount[T >: Null <: AnyRef]
extends Iterable[Accumulator[T]] with FlatHashTable[Accumulator[T]] {
  def default(key:T): Accumulator[T]

  protected def maxClearedSize = 1024
  private def rawInsert(a: Accumulator[T],i: Int) = {
    table(i) = a
    tableSize += 1
    if (tableSize >= threshold) expandTable()
    a
  }
  private def findNearest(h0: Int, key: T) = {
    var h = h0
    while ((table(h) ne null) && table(h).asInstanceOf[Accumulator[T]].key!=key) {
      h = if (h+1 < table.length) h+1 else 0
    }
    h
  }
  private def allocateTable(n: Int) {
    table = new Array[AnyRef](n)
    // threshold = ((n.toLong * loadFactor) / loadFactorDenum).toInt
    threshold = ((n.toLong * 750) / 1000).toInt
    tableSize = 0
  }
  protected def findOrInsert(key: T): Accumulator[T] = {
    var h = findNearest( index(key.hashCode) , key )
    if (table(h)==null) rawInsert( default(key) , h )
    else table(h).asInstanceOf[Accumulator[T]]
  }
  protected def adopt(a: Accumulator[T]): Boolean = {
    var h = findNearest( index(a.key.hashCode) , a.key )
    if (table(h)==null) { rawInsert(a,h); true }
    else { table(h).asInstanceOf[Accumulator[T]] += a; false }
  }
  protected def expandTable() {
    val old = table
    allocateTable(old.length * 2)
    var i = 0
    while (i < old.length) {
      if (old(i)!=null) addEntry(old(i).asInstanceOf[Accumulator[T]])
      i += 1
    }
  }
  override protected def clearTable() {
    if (table.length > maxClearedSize) allocateTable(maxClearedSize)
    else {
      var i = 0
      while (i < table.length) { table(i) = null; i += 1 }
      tableSize = 0
    }
  }

  override def size = tableSize

  def +=(a: Accumulator[T]) {
    val ha = findOrInsert(a.key)
    ha += a
  }
  def +=(t: T) {
    val ht = findOrInsert(t)
    ht++
  }

  def merge(hc: HashCount[T]) = {
    hc.foreach(this adopt _)
    hc.clearTable
    this
  }
}

class TopN[T <: AnyRef](zero: T,val n: Int) {
  // val top = new GenericArray[T](n)
  val top = new ArraySeq[T](n)
  reset
  def reset = {
    var i = 0
    while (i < top.length) { top(i) = zero ; i += 1 }
  }
  def insert(t: T, i: Int) {
    var j = top.length - 1
    while (j>i) { top(j) = top(j-1) ; j -= 1 }
    top(i) = t
  }
  def find(tlist: Iterable[T] , ordinal: T => Long) = {
    var all = 0
    if (top(0)!=zero) reset
    tlist foreach (t => {
      if (ordinal(t)>0) all += 1
      if (ordinal(top(n-1)) < ordinal(t)) {
        var i = n-2
        while (i>=0 && ordinal(top(i)) < ordinal(t)) i -= 1
        insert(t,i+1)
      }
    })
    all
  }
}

//== Wide-Finding-specific section ====================================

class AccumNIJ(var key: Chunk,var n: Long, var i: Int, var j: Int)
extends Accumulator[Chunk] {
  def set(c: Chunk, n0: Long, a: Boolean, b: Boolean) = {
    key = c; n = n0
    i = if (a) 1 else 0
    j = if (b) 1 else 0
    this
  }
  def +=(a: Accumulator[Chunk]) = a match {
    case nij: AccumNIJ => n += nij.n; i += nij.i; j += nij.j
    case _ =>
  }
  def ++() { i += 1 }
}

class HashNIJ extends HashCount[Chunk] {
  def default(key: Chunk) = new AccumNIJ(key.deepcopy, 0L, 0, 0)
}
object HashNIJ {
  def getN(acc: Accumulator[Chunk]) = acc.asInstanceOf[AccumNIJ].n
  def getI(acc: Accumulator[Chunk]) = acc.asInstanceOf[AccumNIJ].i.toLong
  def getJ(acc: Accumulator[Chunk]) = acc.asInstanceOf[AccumNIJ].j.toLong
  def get(acc: Accumulator[Chunk]) = acc.asInstanceOf[AccumNIJ]
}

class Supplier(val chunker: FileChunker, foreman: Foreman) extends Actor {
  def act() { while (true) {
    val chunk = chunker.ready.take()
    foreman.chunks.put(chunk)
    foreman ! Ping
    if (chunk.size < 1) {
      foreman ! None
      chunker.more.set(false) ; chunker.ready.clear
      exit()
    }
  }}
}

class Foreman(chunker: FileChunker, n: Int) extends ReplyReactor {
  val chunks = new LinkedBlockingQueue[Chunk]((n+1)/2)
  val workforce = (1 to n).map(i=>new Worker(i)).toList
  val tomerge,idle,done = new ArrayBuffer[Worker] { def pop = remove(size-1) }
  workforce foreach(w => { w.start(); idle += w })
  var dataless = false
  def barren = dataless && chunks.isEmpty
  def delegate() {
    if ((!tomerge.isEmpty && idle.size>0) || (barren && idle.size>1)) {
      val x = if (tomerge.isEmpty) idle.pop else tomerge.pop
      if (barren) {
        val w = idle.pop
        if (w.id < x.id) w ! x else x ! w
      }
      else {
        var i = 0
        while (i < idle.size) {
          if (idle(i).id==x.pid) {
            val w = idle.remove(i)
            w ! x
            i = idle.size+1
          }
          i += 1
        }
        if (i==idle.size) tomerge += x
      }
    }
    if (idle.size>0) {
      val c = chunks.poll
      if (c ne null) idle.pop ! c
    }
  }
  def act() { Actor.loop { react {
    case Ping =>
      if (!idle.isEmpty) delegate()
    case (w: Worker, c: Chunk) =>
      if (w.id!=w.pid && tomerge.isEmpty) tomerge += w
      else idle += w
      delegate()
      chunker.recycled.put(c)
    case (w: Worker, x: Worker) =>
      if (barren && done.size == n-2) {
        val e = (new Editor(w)).start
        workforce.filter(_ ne w).foreach(_ ! None)
        exit()
      }
      else {
        if (barren) done += x else idle.insert(0,x);
        if (w.id!=w.pid && tomerge.isEmpty) tomerge += w else idle += w
        delegate()
      }
    case None =>
      dataless = true
      delegate()
  }}}
}

class Counter {
  val pages,clients,referrers = new HashNIJ()
  val liner = new ChunkIterator(WFii.nl)
  val tokener = new ChunkIterator(WFii.sp)

  def size = pages.size + clients.size + referrers.size
  def merge(counter: Counter) {
    pages merge counter.pages
    clients merge counter.clients
    referrers merge counter.referrers
  }
  def isHit(c: Chunk): Boolean = {
    if (c.size < WFii.hitstart.size + WFii.hitmid.size + 1) false
    else {
      var i = c.start+WFii.hitstart.size
      var j = 0
      var b = 0:Byte
      while (j < WFii.hitmid.end) {
        b = c.data(i)
        if (b>WFii.d0 && b<=WFii.d9) b = WFii.d0
        if (b != WFii.hitmid.data(j)) return false
        i += 1
        j += 1
      }
      while (i < c.end) {
        if (c.data(i)==WFii.sp || c.data(i)==WFii.dot) return false
        i += 1
      }
      c startsWith WFii.hitstart
    }
  }
  def count(c: Chunk) {
    val uri,parse,ref,client = new Chunk(c.data,0,0)
    var code,bytes = 0
    val result = new AccumNIJ(c,0L,0,0)
    var hit = false

    (liner over c) foreach (line => {
      tokener over line
      client.set( tokener.next, 0, -1 )
      tokener.drop(4)
      parse.set( tokener.next, 1, 0 )
      if (parse is WFii.get) {
        uri.set( tokener.next, 0, -1 )
        tokener.drop(1)
        code = parse.set( tokener.next, 0, -1 ).toInt
        if (code==200 || code==304 || code==404) {
          if (code==404) { bytes = 0; hit = false }
          else if (code==200 || code==304) {
            bytes = parse.set(tokener.next,0,-1).toInt
            hit = isHit(uri)
            if (hit) {
              clients += client
              ref.set( tokener.next, 1, -2 )
              if (!(ref is WFii.refless) && !(ref startsWith WFii.local)) referrers += ref
            }
            if (code==304) bytes = 0
          }
          pages += result.set(uri,bytes,hit,code==404)
        }
      }
    })
    liner.reset
    tokener.reset
  }
}

class Worker(val id: Int) extends ReplyReactor {
  val pid = id - highbit(id-1)
  val counter = new Counter()
  def act() { Actor.loop { react {
    case c: Chunk =>
      counter.count(c)
      sender ! (this,c)
    case w: Worker =>
      counter merge w.counter
      sender ! (this,w)
    case _ => exit()
  }}}
}

case class WrapFNIJ(f: Accumulator[Chunk] => Long) { }

class Reporter extends ReplyReactor {
  def act() { Actor.loop { react {
    case (title: String, hc: HashNIJ, WrapFNIJ(f) ) =>
      val top = new TopN[Accumulator[Chunk]](hc.default(WFii.refless),10)
      val n = top find (hc,f)
      sender ! (
        n ,
        (  title ::
           top.top.map(kv => "  %14d : %s".format(f(kv),kv.key)).toList :::
           "" :: Nil
        ).mkString("\n")
      )
    case _ => exit()
  }}}
}

class Editor(w: Worker) extends Actor {
  val cn = w.counter
  val pagerep,clientrep,refrep = (new Reporter)
  val reps = List(pagerep,clientrep,refrep)
  reps.foreach(_.start())
  def get(a:Any) = a match { case (i:Int,s:String) => (s,i); case _ => ("",0) }
  def act() {
    val clientFuture = clientrep     !! (("Top 10 client IPs by hits on articles", cn.clients, WrapFNIJ(HashNIJ.getI)))
    val refFuture =    refrep        !! (("Top 10 referrers by hits on articles", cn.referrers, WrapFNIJ(HashNIJ.getI)))
    val (topByte,_) = get(   pagerep !? (("Top 10 URIs by total response bytes", cn.pages, WrapFNIJ(HashNIJ.getN))) )
    val (top404,n404) = get( pagerep !? (("Top 10 URIs returning 404 (Not Found)", cn.pages, WrapFNIJ(HashNIJ.getJ))) )
    val (topHit,nHit) = get( pagerep !? (("Top 10 URIs by hits on articles", cn.pages, WrapFNIJ(HashNIJ.getI))) )
    val (topClient,nClient) = get( clientFuture() )
    val (topRef,_) = get( refFuture() )
    printf("%d resources, %d 404s, %d clients\n\n",nHit,n404,nClient)
    List(topHit,topByte,top404,topClient,topRef) foreach println
    reps.foreach(_ ! None)
    w ! None
  }
}

object WFii {
  val List(sp,nl,dot,d0,d9) = " \n.09".toList.map(_.toByte)
  val refless = "-":Chunk
  val get = "GET ":Chunk
  val hitstart = "/ongoing/When/":Chunk
  val hitmid = "000x/0000/00/00/":Chunk
  val local = "http://www.tbray.org/ongoing/":Chunk

  def lastNL(c: Chunk) = {
    var i = c.end-1
    while (i>c.start && c.data(i)!=nl) i -= 1
    i
  }
  def main(args: Array[String]) {
    // val threads = 4 max (3*java.lang.Runtime.getRuntime.availableProcessors)/4
    val threads = 1
    val gobbler = new FileGobbler(new File(args(0)),1024*1024,4*threads)
    gobbler.start()
    val chunker = new FileChunker(gobbler,threads,lastNL)
    chunker.start()
    val foreman = new Foreman(chunker,threads)
    (new Supplier(chunker,foreman)).start()
    foreman.start()
  }
}
