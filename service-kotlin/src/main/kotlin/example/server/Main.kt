package example.server

import io.ktor.server.cio.*
import io.rsocket.kotlin.RSocketRequestHandler
import io.rsocket.kotlin.core.RSocketServer
import io.rsocket.kotlin.payload.Payload
import io.rsocket.kotlin.transport.ktor.tcp.TcpServerTransport
import io.rsocket.kotlin.transport.ktor.websocket.server.WebSocketServerTransport
import kotlinx.coroutines.runBlocking
import io.rsocket.kotlin.transport.ServerTransport
import io.rsocket.kotlin.transport.ktor.tcp.TcpServer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.job
import java.util.concurrent.atomic.AtomicInteger

object Main {
        @JvmStatic
        fun main(args: Array<String>) {
            val transportStr = System.getProperty("transport","tcp")
            val transport: ServerTransport<*> =
                    when (transportStr) {
                        "ws" -> WebSocketServerTransport(CIO, host = "localhost", port = 7000)
                        "tcp" -> TcpServerTransport(hostname = "localhost", port = 7000)
                        else -> throw java.lang.IllegalArgumentException(
                                "Unknown transport: $transportStr")
                    }

            val connector = RSocketServer {
                //configuration goes here
            }
            val server = connector.bind(transport) {
                RSocketRequestHandler {
                    requestResponse { request: Payload ->
                        //println(request.data.readText()) //print request payload data
                        val data = request.data
                        val response = data.copy()
                        //data.release()
                        Payload(response)
                    }
                    requestStream { request:Payload ->
                        flow {
                            val data = request.data
                            while (true) {
                                emit(Payload(data.copy()))
                            }
                        }
                    }
                }
            }
            println("rsocket-kotlin server started, transport: $transportStr")
            runBlocking {
                when (transportStr) {
                    "ws" -> (server as /*NettyApplicationEngine*/CIOApplicationEngine).application.coroutineContext.job.join()
                    "tcp" -> (server as TcpServer).handlerJob.join()
                    else -> throw java.lang.AssertionError("Unknown transport: $transportStr")
                }
            }
        }
}