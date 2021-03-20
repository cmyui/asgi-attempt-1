#!/usr/bin/python3.9

import asyncio
import functools
import http
import os
import re
import signal
import socket
import time
import threading
import urllib
from email.utils import formatdate
from ssl import SSLContext
from typing import Coroutine
from typing import Optional
from typing import Any
from time import perf_counter_ns as clock_ns

import httptools

# first asgi server attempt
# obviously heavy inspiration taken from
# uvicorn/hypercorn; would recommend either

STATUS_LINES = {
    c.value: f'HTTP/1.1 {c.value} {c.name}\r\n'.encode()
    for c in http.HTTPStatus
}

# TODO: real
IP_REGEX = re.compile(r'^(?P<host>(?:\d{1,3}\.){1,3}\d{1,3})(?P<port>:\d{1,5})?$')

HIGH_WATER_LIMIT = 0x10000 # TODO: low water limit
CLOSE_HEADER = (b'connection', b'close')

class ServerState:
    """Shared server start between all protocol instances."""
    __slots__ = ('total_requests', 'connections',
                 'tasks', 'default_headers')
    def __init__(self) -> None:
        self.total_requests = 0
        self.connections = set()
        self.tasks = set()
        self.default_headers = []

class RequestResponseCycle:
    __slots__ = (
        'scope', 'transport', 'flow', 'default_headers',
        'message_event', 'on_response',

        # connection state
        'keep_alive', 'disconnected',

        # request state
        'body', 'more_body',

        # response state
        'response_started', 'response_complete',
        'chunked_encoding', 'expected_content_length'
    )
    def __init__(
        self, scope, transport, flow, default_headers,
        message_event, keep_alive, on_response
    ) -> None:
        self.scope = scope
        self.transport = transport
        self.flow = flow
        self.default_headers = default_headers
        self.message_event = message_event
        self.on_response = on_response

        # connection state
        self.keep_alive = keep_alive
        self.disconnected = False

        # request state
        self.body = b''
        self.more_body = True

        # response state
        self.response_started = False
        self.response_complete = False
        self.chunked_encoding = None
        self.expected_content_length = 0

    async def run_asgi(self, app) -> None:
        try:
            result = await app(self.scope, self.receive, self.send)
        except BaseException as exc:
            if not self.response_started:
                await self.send_500_response()
            else:
                self.transport.close()
        else:
            # no exceptions
            if result is not None:
                print(f'ASGI callable returned {result!r} (should be None).')
                self.transport.close()
            elif not (
                self.response_started or
                self.disconnected
            ):
                print('ASGI callable returned without starting response.')
                await self.send_500_response()
            elif not (
                self.response_complete or
                self.disconnected
            ):
                print('ASGI callable returned without completing response.')
                self.transport.close()
        finally:
            self.on_response = None

    async def send_500_response(self):
        await self.send({
            'type': 'http.response.start',
            'status': 500,
            'headers': [
                (b'content-type', b'text/plain; charset=utf-8'),
                (b'connection', b'close')
            ]
        })
        await self.send({
            'type': 'http.response.body',
            'body': b'Internal Server Error'
        })

    # asgi interface

    #@profile
    async def send(self, message) -> None:
        message_type = message['type']

        if self.flow.write_paused and not self.disconnected:
            await self.flow.drain()

        if self.disconnected:
            return

        if not self.response_started:
            # sending response status line & headers
            if message_type != 'http.response.start':
                raise RuntimeError(f'Expected ASGI message "http.response.start" but got {message_type!r}')

            self.response_started = True

            status_code = message['status']
            headers = self.default_headers + list(message.get('headers', []))

            if (
                CLOSE_HEADER in self.scope['headers'] and
                CLOSE_HEADER not in headers
            ):
                headers = headers + [CLOSE_HEADER]

            content = [STATUS_LINES[status_code]]

            for name, value in headers:
                # todo: ensure header key & val are legit

                name = name.lower()
                if (
                    name == b'content-length' and
                    self.chunked_encoding is None
                ):
                    self.expected_content_length = int(value.decode())
                    self.chunked_encoding = False
                elif (
                    name == b'transfer-encoding' and
                    value.lower() == b'chunked'
                ):
                    self.expected_content_length = 0
                    self.chunked_encoding = True
                elif (
                    name == b'connection' and
                    value.lower() == b'close'
                ):
                    self.keep_alive = False
                content.extend([name, b': ', value, b'\r\n'])

            if (
                self.chunked_encoding is None and
                self.scope['method'] != 'HEAD' and
                status_code not in (204, 304)
            ):
                # neither content-length not transfer-encoding specified
                self.chunked_encoding = True
                content.append(b'transfer-encoding: chunked\r\n')

            content.append(b'\r\n')
            self.transport.write(b''.join(content))

        elif not self.response_complete:
            # sending response body
            if message_type != 'http.response.body':
                raise RuntimeError(f'Expected ASGI message "http.response.body", but got {message_type!r}.')

            body = message.get('body', b'')
            more_body = message.get('more_body', False)

            if self.scope['method'] == 'HEAD':
                self.expected_content_length = 0
            elif self.chunked_encoding:
                if body:
                    content = [b'%x\r\n' % len(body), body, b'\r\n']
                else:
                    content = []
                if not more_body:
                    content.append(b'0\r\n\r\n')
                self.transport.write(b''.join(content))
            else:
                num_bytes = len(body)
                if num_bytes > self.expected_content_length:
                    raise RuntimeError('Response content longer than Content-Length.')
                else:
                    self.expected_content_length -= num_bytes
                self.transport.write(body)

            # handle response completion
            if not more_body:
                if self.expected_content_length != 0:
                    raise RuntimeError('Response content shorter than Content-Length.')
                self.response_complete = True
                self.message_event.set()
                if not self.keep_alive:
                    self.transport.close()
                self.on_response()

        else:
            # response already semt
            raise RuntimeError(f'Unexpected ASGI message "{message_type}" (resp already complete).')

    async def receive(self) -> None:
        # TODO: 100 continue

        if not (
            self.disconnected or
            self.response_complete
        ):
            self.flow.resume_reading()
            await self.message_event.wait()
            self.message_event.clear()

        if self.disconnected or self.request_complete:
            message = {'type': 'http.disconnect'}
        else:
            message = {
                'type': 'http.request',
                'body': self.body,
                'more_body': self.more_body
            }
            self.body = b''

        return message

# - on_message_begin()
# - on_url(url: bytes)
# - on_header(name: bytes, value: bytes)
# - on_headers_complete()
# - on_body(body: bytes)
# - on_message_complete()
# - on_chunk_header()
# - on_chunk_complete()
# - on_status(status: bytes)

class FlowControl:
    __slots__ = ('_transport', 'read_paused',
                 'write_paused', '_is_writable_event')
    def __init__(self, transport) -> None:
        self._transport = transport
        self.read_paused = False
        self.write_paused = False
        self._is_writable_event = asyncio.Event()
        self._is_writable_event.set()

    async def drain(self) -> None:
        await self._is_writable_event.wait()

    def pause_reading(self) -> None:
        if not self.read_paused:
            self.read_paused = True
            self._transport.pause_reading()

    def resume_reading(self) -> None:
        if self.read_paused:
            self.read_paused = False
            self._transport.resume_reading()

    def pause_writing(self):
        if not self.write_paused:
            self.write_paused = True
            self._is_writable_event.clear()

    def resume_writing(self):
        if self.write_paused:
            self.write_paused = False
            self._is_writable_event.set()

async def service_unavailable(scope, receive, send):
    await send({
        'type': 'http.response.start',
        'status': 503,
        'headers': [
            (b'content-type', b'text/plain; chartset=utf-8'),
            (b'connection', b'close')
        ]
    })
    await send({
        'type': 'http.response.body',
        'body': b'Service Unavailable'
    })

# TODO: asyncio.BufferedProtocol
class HttpToolsRequestProtocol(asyncio.Protocol):
    __slots__ = (
        'app', 'loop', 'parser', 'concurrency_limit',

        # timeouts
        'timeout_keep_alive', 'timeout_keep_alive_task',

        # global state
        'server_state', 'connections', 'tasks', 'default_headers',

        # connection state
        'transport', 'flow', 'server', 'client', 'scheme', 'queue',

        # request state
        'url', 'scope', 'headers', 'cycle', 'request_start'
    )
    def __init__(self, app: Coroutine, server_state: ServerState,
                 _loop: asyncio.AbstractEventLoop = None,
                 concurrency_limit: Optional[int] = None) -> None:
        print('making proto')
        self.app = app
        self.loop = _loop or asyncio.get_running_loop()
        self.parser = httptools.HttpRequestParser(self)
        self.concurrency_limit = concurrency_limit

        # timeouts
        self.timeout_keep_alive = 1800
        self.timeout_keep_alive_task = None

        # global state
        self.server_state = server_state
        self.connections = server_state.connections
        self.tasks = server_state.tasks
        self.default_headers = server_state.default_headers

        # per connection
        self.transport = None
        self.flow = None
        self.server = None
        self.client = None
        self.scheme = None
        self.queue = []

        # per request
        self.url = None
        self.scope = None
        self.headers = None
        self.cycle = None
        self.request_start = None

        # TODO: once i make a websockets protocol, it
        # should actually be added here so we can allow
        # users to upgrade from http v1.1 to websockets :D

    """ protocol api """
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.request_start = clock_ns()
        print('connection made')
        self.connections.add(self)

        self.transport = transport
        self.flow = FlowControl(transport)

        sn_info = pn_info = None # sockname, peername

        t_sock = transport.get_extra_info('socket')
        if t_sock is not None:
            t_sock: socket.socket
            # INET: (host, port)
            # INET6: ???
            # UNIX: path

            sn_info = t_sock.getsockname()
            if not sn_info:
                sn_info = transport.get_extra_info('sockname')

            if sn_info:
                self.server = sn_info

            pn_info = t_sock.getpeername()
            if not pn_info:
                pn_info = transport.get_extra_info('peername')

            if pn_info:
                self.client = pn_info
        else:
            print('couldnt get socket from transport')
            breakpoint()

        is_ssl = bool(transport.get_extra_info('sslcontext'))
        self.scheme = 'https' if is_ssl else 'http'

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.connections.discard(self)

        if self.cycle and not self.cycle.response_complete:
            self.cycle.disconnected = True
        if self.cycle is not None:
            self.cycle.message_event.set()
        if self.flow is not None:
            self.flow.resume_writing()

    def data_received(self, data: bytes) -> None:
        try:
            self.parser.feed_data(data)
        except httptools.HttpParserError as exc:
            print('Invalid HTTP request received.')
            self.transport.close()
        except httptools.HttpParserUpgrade:
            self.handle_upgrade()

    def handle_upgrade(self) -> None:
        ... # TODO: upgrade to websockets

    def eof_received(self) -> Optional[bool]:
        pass

    """ httptools api """
    # feed_data
    # get_http_version
    # get_method
    # should_keep_alive
    # should_upgrade
    #def on_message_begin(self):
    #    ...

    def on_url(self, url: bytes) -> None:
        method = self.parser.get_method()
        parsed_url = httptools.parse_url(url)
        raw_path = parsed_url.path
        path = raw_path.decode('ascii')
        if '%' in path:
            path = urllib.parse.unquote(path)

        self.url = url
        self.headers = []

        self.scope = {
            'type': 'http',
            'http_version': '1.1',
            'server': self.server,
            'client': self.client,
            'scheme': self.scheme,
            'method': method.decode('ascii'),
            'path': path,
            'raw_path': raw_path,
            'query_string': parsed_url.query or b'',
            'headers': self.headers
        }

    def on_header(self, name: bytes, value: bytes) -> None:
        self.headers.append((name.lower(), value))

    def on_headers_complete(self) -> None:
        http_version = self.parser.get_http_version()
        if http_version != '1.1':
            self.scope['http_version'] = http_version

        # TODO self.parser.should_upgrade

        # TODO: concurrency limits (service_unavailable 503)
        if self.concurrency_limit is not None and (
            len(self.tasks) >= self.concurrency_limit or
            len(self.connections) >= self.concurrency_limit
        ):
            print(f'\x1b[0;31munavailable [{len(self.tasks)}]\x1b[0m')
            app = service_unavailable
        else:
            app = self.app

        existing_cycle = self.cycle
        self.cycle = RequestResponseCycle(
            scope = self.scope,
            transport = self.transport,
            flow = self.flow,
            default_headers=self.default_headers,
            message_event = asyncio.Event(),
            keep_alive = http_version != '1.0',
            on_response = self.on_response_complete
        )

        if (
            existing_cycle is None or
            existing_cycle.request_complete
        ):
            # process normally
            task = self.loop.create_task(self.cycle.run_asgi(app))
            task.add_done_callback(self.tasks.discard)
            self.tasks.add(task)
        else:
            # enqueue the task for later
            self.flow.pause_reading()
            self.queue.insert(0, (self.cycle, app))

    def on_body(self, body: bytes) -> None:
        if self.cycle.response_complete:
            return

        # TODO self.parser.should_upgrade
        self.cycle.body += body
        if len(self.cycle.body) > HIGH_WATER_LIMIT:
            self.flow.pause_reading()
        self.cycle.message_event.set()

    def on_message_complete(self) -> None:
        if self.cycle.response_complete:
            return

        # TODO self.parser.should_upgrade
        self.cycle.more_body = False
        self.cycle.message_event.set()

    def on_response_complete(self) -> None:
        self.server_state.total_requests += 1
        print(f'took {(clock_ns()-self.request_start)/1e6}ms')
        print(f'{self.server_state.total_requests} connections handled '
              f'({len(self.tasks)} tasks, {len(self.connections)} conns)')

        if self.transport.is_closing():
            return

        # TODO: update keepalive

        self.timeout_keep_alive_task = self.loop.call_later(
            self.timeout_keep_alive, self.timeout_keep_alive_handler
        )

        self.flow.resume_reading()

        # get next from queue
        if self.queue:
            cycle, app = self.queue.pop()
            task = self.loop.create_task(cycle.run_asgi(app))
            task.add_done_callback(self.tasks.discard)
            self.tasks.add(task)

    # called in `on_response_complete`
    def timeout_keep_alive_handler(self) -> None:
        if not self.transport.is_closing():
            self.transport.close()

    def shutdown(self) -> None:
        if (
            self.cycle is None or
            self.cycle.response_complete
        ):
            self.transport.close()
        else:
            self.cycle.keep_alive = False

    # called by transport
    def pause_writing(self):
        self.flow.pause_writing()
    def resume_writing(self):
        self.flow.resume_writing()

def _parse_address(addr: Any) -> Optional[tuple[socket.AddressFamily, Any]]:
    # figure out which kind of server to
    # make based on the input address type.
    if isinstance(addr, (str, bytes)):
        if isinstance(addr, bytes):
            addr = addr.decode()

        if match := IP_REGEX.match(addr): # inet4: '127.0.0.1:5001'
            return (socket.AF_INET, (match['host'], int(match['port'][1:])))
        elif addr.startswith('unix:'): # 'unix:/tmp/gulag.sock'
            return (socket.AF_UNIX, addr[5:])
    elif isinstance(addr, (tuple, list)) and len(addr) == 2:
        host, port = addr
        if (
            isinstance(host, (str, bytes)) and
            isinstance(port, int)
        ):
            if isinstance(host, bytes):
                host = host.decode()
            return (socket.AF_INET, (host, port))

    raise ValueError('Found no matching server implementation '
                     'for the given address type.')

class Server:
    __slots__ = (
        'sock_family', 'addr', 'app',
        'loop', 'server_state', 'backlog', 'ssl',

        'started', 'should_exit', 'force_exit',
        'handle_signals',

        # internals
        'servers'
    )
    def __init__(
        self, addr, app: Optional[Coroutine] = None,
        _loop: asyncio.AbstractEventLoop = None,
        handle_signals: bool = True,
        backlog: int = 100,
        ssl: Optional[SSLContext] = None
    ) -> None:
        self.sock_family, self.addr = _parse_address(addr)
        self.app = app

        self.loop = _loop or asyncio.get_running_loop()
        self.server_state = ServerState()
        self.backlog = backlog
        self.ssl = ssl

        self.started = False
        self.should_exit = False
        self.force_exit = False

        self.handle_signals = handle_signals

        # internals
        self.servers = []

    def run(self) -> None:
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.serve())

    async def serve(self):
        if self.handle_signals:
            self.install_signal_handlers()

        await self.startup()
        if self.should_exit:
            return
        await self.main_loop()
        await self.shutdown()

    async def startup(self) -> None:
        print(f'Starting {self.sock_family.name} server @ {self.addr}')
        protocol_factory = functools.partial(
            HttpToolsRequestProtocol, app=self.app,
            server_state=self.server_state
        )

        if self.sock_family == socket.AF_INET:
            server = await self.loop.create_server(
                protocol_factory, *self.addr, # (host, port)
                backlog=self.backlog, ssl=self.ssl
            )
        elif self.sock_family == socket.AF_UNIX:
            uds_perms = 0o666
            if os.path.exists(self.addr):
                uds_perms = os.stat(self.addr).st_mode

            server = await self.loop.create_unix_server(
                protocol_factory, path=self.addr,
                backlog=self.backlog, ssl=self.ssl
            )
            os.chmod(self.addr, uds_perms)

        self.servers = [server]
        self.started = True

    async def main_loop(self) -> None:
        ticks = 0
        should_exit = await self.on_tick(ticks)
        while not should_exit:
            ticks += 1
            ticks %= 864000
            await asyncio.sleep(0.1)
            should_exit = await self.on_tick(ticks)

    async def on_tick(self, ticks: int) -> bool:
        if ticks % 10 == 0:
            # update date header every second
            date_fmt = formatdate(time.time(), usegmt=True).encode()

            self.server_state.default_headers = [
                (b'date', date_fmt)
            ]

        return self.should_exit

    async def shutdown(self) -> None:
        print('Shutting down gracefully')
        for server in self.servers:
            server.close()
        for server in self.servers:
            await server.wait_closed()

        for connection in list(self.server_state.connections):
            connection.shutdown()
        await asyncio.sleep(0.1)

        if self.sock_family == socket.AF_UNIX:
            if os.path.exists(self.addr):
                os.remove(self.addr)

        # wait for existing connections to finish resp
        if self.server_state.connections and not self.force_exit:
            print('Waiting for connections to close. (CTRL+C to force quit)')
            while self.server_state.connections and not self.force_exit:
                await asyncio.sleep(0.1)

        # wait for existing tasks to complete
        if self.server_state.tasks and not self.force_exit:
            print('Waiting for tasks to complete. (CTRL+C to force quit)')
            while self.server_state.tasks and not self.force_exit:
                await asyncio.sleep(0.1)

        self.loop.stop()

    def install_signal_handlers(self) -> None:
        # signals may only be added from main py interpreter thread
        if threading.current_thread() is not threading.main_thread():
            return

        signals = {signal.SIGINT, signal.SIGTERM, signal.SIGHUP}

        for sig in signals:
            self.loop.add_signal_handler(sig, self.handle_exit, sig)

    def handle_exit(self, sig: signal.Signals):
        if sig is signal.SIGINT:
            # remove ^C from console for
            # KeyboardInterrupt signals
            print('\33[2K', end='\r')

        if self.should_exit:
            self.force_exit = True
        else:
            self.should_exit = True

if __name__ == '__main__': # for testing
    import sys
    from pathlib import Path

    SHIT = b'Hello World!' * 100000
    async def app(
        scope: dict[str, Any],
        receive: Coroutine,
        send: Coroutine
    ) -> None:
        assert scope['type'] == 'http'

        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain']
            ],
        })

        await send({
            'type': 'http.response.body',
            'body': SHIT,
        })

    async def run_tests(loop: asyncio.AbstractEventLoop) -> None: # TODO lol
        #server = Server(addr=('127.0.0.1', 5001), app=app, _loop=loop)
        #server = Server(addr='127.0.0.1:5001', app=app, _loop=loop)
        #server = Server(addr='/tmp/gulag.sock', app=app)
        ...

    def restart() -> None:
        executable = sys.executable
        script_path = Path(sys.argv[0]).resolve()
        args = sys.argv[1:]
        main_package = sys.modules['__main__'].__package__

        if not main_package:
            # executed by filename
            if script_path.is_file() and os.access(script_path, os.X_OK):
                # ./run.py
                executable = str(script_path)
            else:
                # python run.py
                args.append(str(script_path))
        else:
            # TODO: executed as a module
            breakpoint()

        os.execv(executable, [executable] + args)

    async def main() -> None:
        loop = asyncio.get_running_loop()
        await run_tests(loop)

        loop.set_debug(True)
        server = Server('unix:/tmp/gulag.sock', app=app, _loop=loop)
        await server.serve()

    loop = asyncio.new_event_loop()
    should_restart = False
    sig_restart = signal.SIGUSR1

    def set_restart() -> None:
        global should_restart
        should_restart = True

        # call shutdown signal handler
        os.kill(os.getpid(), signal.SIGTERM)

    loop.add_signal_handler(signal.SIGUSR1, set_restart)

    try:
        loop.create_task(main())
        loop.run_forever()
    except Exception as e:
        ...
    finally:
        if should_restart:
            print('Rebooting')
            restart()
        else:
            print('Closing')


"""
async def main() -> None:
    print('Running server')
    loop = asyncio.get_running_loop()

    server = await loop.create_unix_server(
        HttpToolsProtocol,
        '/tmp/gulag.sock', start_serving=False
    )

    os.chmod('/tmp/gulag.sock', 0o666)

    try:
        async with server:
            print('Serving')
            await server.serve_forever()
    except Exception as e:
        print(e)

loop = asyncio.new_event_loop()

def t(*a,**k):
    return a,k

try:
    v = loop.create_task(main())
    v.add_done_callback(t)
    loop.run_forever()
except:
    print('Exception')
    pass
finally:
    print('Shutting down server')
    loop.stop()

    tasks = asyncio.all_tasks()
    for task in tasks:
        task.cancel()

    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

    loop.shutdown_asyncgens()
    loop.shutdown_default_executor()
    loop.close()
"""
