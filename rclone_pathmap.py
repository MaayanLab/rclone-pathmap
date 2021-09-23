'''
This script is a wrapper around rclone for path to
rclone path mappings for the purpose of an overlayfs-style mount.

More specifically, an rclone-compatible http backend is constructed
on the fly to serve the `lowerdir` in read-only fashion, and the union
provider is used to join this with the `upperdir` for read-write access.

\b
Example usage:
  ```bash
  rclone-pathmap mount ':s3,env_auth=True:workbucket/workdir' mnt << END
  /input-file-1: :s3,env_auth=True:mybucket/mybigfile
  /input-file-2: :ftp,host=ftp.example.com:myftp/file
  END
  ```
'''
import sys
import json
import yaml
import click
import signal
import asyncio
import pathlib
import logging
import tempfile
import datetime
import functools
import contextlib
import inspect
from aiohttp import web
from collections import OrderedDict
from datetime import datetime
from dataclasses import dataclass
from dateutil.parser import parse as fromTs

logger = logging.getLogger(__name__)

chunk_size = 8192

def _escape_quotes(s):
  return s.replace(r"'", r"''")

async def _await(maybe_awaitable):
  if inspect.isawaitable(maybe_awaitable):
    return await maybe_awaitable
  else:
    return maybe_awaitable

async def _try_wait_for(cond, args=tuple(), max_tries=3, backoff=1):
  ''' Try waiting for a condition, otherwise continue
  '''
  while True:
    if await _await(cond(*args)):
      return True
    max_tries -= 1
    if max_tries <= 0:
      return False
    await asyncio.sleep(backoff)

def _async_lru_cache(cache_size=None):
  def decorator(func):
    _cache = {} if cache_size is None else OrderedDict()
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
      k = json.dumps(dict(args=args, kwargs=kwargs))
      if k in _cache:
        logger.debug(f"cache hit {func} {k}")
        return _cache[k]
      else:
        logger.debug(f"cache miss {func} {k}")
        _cache[k] = await func(*args, **kwargs)
        if cache_size is not None:
          while len(_cache) > cache_size:
            _cache.popitem(False)
      return _cache[k]
    return wrapper
  return decorator

def _datetime_to_rfc2822(dt):
  return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

@_async_lru_cache()
async def _rclone_file_headers(rclone_path):
  proc = await asyncio.create_subprocess_exec(
    'rclone', 'lsjson', rclone_path,
    stdout=asyncio.subprocess.PIPE,
    stderr=sys.stderr,
  )
  stdout, _stderr = await proc.communicate()
  if proc.returncode != 0: return None
  file_metadata, = json.loads(stdout)
  return {
    'Content-Length': str(file_metadata['Size']),
    'Content-Type': file_metadata['MimeType'],
    'Date': _datetime_to_rfc2822(datetime.now()),
    'Last-Modified': _datetime_to_rfc2822(fromTs(file_metadata['ModTime'])),
  }

async def _serve_rclone_file(request, rclone_path):
  headers = await _rclone_file_headers(rclone_path)
  if headers is None: raise web.HTTPNotFound
  if request.method == 'HEAD':
    return web.Response(
      body=b'',
      status=200,
      headers=headers,
    )
  #
  try:
    proc = await asyncio.create_subprocess_exec(
      'rclone', 'cat', rclone_path,
      stdout=asyncio.subprocess.PIPE,
      stderr=sys.stderr,
    )
  except Exception as e:
    logger.error(e)
  response = web.StreamResponse(
    status=200,
    reason='OK',
    headers=headers,
  )
  await response.prepare(request)
  while not proc.stdout.at_eof():
    chunk = await proc.stdout.read(chunk_size)
    await response.write(chunk)
  await response.write_eof()
  return response

def _create_app(mappings):
  '''
  [path_on_webserver]: [rclone_uri]

  Example config:
  ```yml
  /a: :s3,env_auth=True:bucket/prefix/file1
  /b/c: :s3,env_auth=True:bucket/prefix/file2
  ```
  '''
  listing = {}
  for mapping in mappings:
    src_split = mapping.split('/')
    for i in range(1, len(src_split)):
      path_parent = '/'.join(src_split[:i-1]) + '/'
      path_current = '/'.join(src_split[:i]) + '/'
      if path_parent not in listing: listing[path_parent] = {}
      listing[path_parent][path_current] = True
    path_parent = '/'.join(src_split[:-1]) + '/'
    if path_parent not in listing: listing[path_parent] = {}
    listing[path_parent][src_split[-1]] = True
  #
  logger.debug(f"{mappings=}")
  logger.debug(f"{listing=}")
  #
  async def handler(request):
    path = f"/{request.match_info['path']}"
    if path in mappings:
      logger.info(f"serve {path}")
      return await _serve_rclone_file(request, mappings[path])
    elif path in listing:
      logger.info(f"list {path}")
      return web.Response(
        body=''.join(
          f"<a href='{p}'>{p}</a>"
          for p in listing[path]
        ),
        content_type='text/html',
      )
    else:
      logger.info(f"unhandled {path}")
      raise web.HTTPNotFound
  #
  app = web.Application()
  app.add_routes([web.get('/{path:.*}', handler)])
  return app

@contextlib.asynccontextmanager
async def _serve(mappings):
  app = _create_app(mappings)
  runner = web.AppRunner(app)
  await runner.setup()
  site = web.TCPSite(runner, 'localhost', 0)
  await site.start()
  logger.info(runner.addresses)
  try:
    yield runner
  finally:
    await runner.cleanup()

@contextlib.asynccontextmanager
async def MountTemporaryDirectory(mountdir=None):
  ''' Like tempfile.TemporaryDirectory, but specifically
  '''
  if mountdir is None:
    _mountdir = pathlib.Path(tempfile.mkdtemp())
  else:
    _mountdir = pathlib.Path(mountdir)
  assert _mountdir.is_dir()
  #
  try:
    yield _mountdir
  finally:
    if await _try_wait_for(_mountdir.is_mount) and mountdir is None:
      _mountdir.rmdir()

@contextlib.asynccontextmanager
async def RCloneMount(remote, mountdir, *flags):
  ''' Usage:
  async with RCloneMount(':s3,env_auth=True:bucket/workdir', mountdir) as proc:
    pass # do things in tmpdir
  '''
  mountdir = pathlib.Path(mountdir)
  args = ('rclone', 'mount', *flags, remote, str(mountdir))
  logger.debug(' '.join(args))
  # start rclone mount
  proc = await asyncio.create_subprocess_exec(*args)
  # wait for directory to be mounted
  while proc.returncode is None and not mountdir.is_mount():
    await asyncio.sleep(1)
  # ensure the process is still running
  assert proc.returncode is None
  #
  try:
    yield proc
  finally:
    proc.send_signal(signal.SIGINT)
    await proc.wait()

@dataclass
class RunningRClonePathmap:
  proc: asyncio.subprocess.Process
  runner: web.AppRunner

@contextlib.asynccontextmanager
async def RClonePathmap(mappings, upperdir, mountdir, *rclone_flags):
  ''' Usage:
  async with RClonePathmap({ "/a": ":s3,env_auth=True:bucket/input" }, ':s3,env_auth=True:bucket/workdir', None) as (tmpdir, *_):
    pass # do things in tmpdir
  '''
  async with _serve(mappings) as runner:
  # async with _serve(mappings) as runner:
    ((host, port), *_) = filter(lambda t: len(t)==2, runner.addresses)
    async with RCloneMount(
      f":union,upstreams='{_escape_quotes(upperdir)} :http::ro':",
      mountdir,
      f"--http-url=http://{host}:{port}",
      *rclone_flags,
    ) as proc:
      yield RunningRClonePathmap(
        proc=proc,
        runner=runner,
      )

@click.group(help=__doc__)
@click.version_option()
@click.option('-v', '--verbose', count=True, default=0, help='How verbose this should be, more -v = more verbose')
def cli(verbose=0):
  logging.basicConfig(level=30 - (verbose*10))

@cli.command()
@click.option('-c', '--config', default='-', type=click.File('r'), help='Configuration file (yaml)')
@click.option('-l', '--listen', type=str, default='localhost:8080', help='hostname:port to listen on')
def serve(config, listen):
  host, _, port = listen.partition(':')
  port = int(port)
  app = _create_app(yaml.load(config, Loader=yaml.BaseLoader))
  web.run_app(app, host=host, port=port)

async def _mount_main(mappings, upperdir, mountdir, *rclone_flags):
  async with MountTemporaryDirectory(mountdir) as mountdir:
    async with RClonePathmap(mappings, upperdir, mountdir, *rclone_flags) as rclone_pathmap:
      await rclone_pathmap.proc.wait()

@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.option('-c', '--config', default='-', type=click.File('r'), help='Configuration file (yaml) for lowerdir')
@click.argument('upperdir', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('mountdir', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('rclone_flags', nargs=-1)
def mount(upperdir, mountdir, config, rclone_flags):
  asyncio.run(_mount_main(
    yaml.load(config, Loader=yaml.BaseLoader),
    upperdir,
    mountdir,
    *rclone_flags,
  ))

if __name__ == '__main__':
  cli()
