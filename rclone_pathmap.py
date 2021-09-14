'''
This script is a wrapper around rclone for path to
rclone path mappings for the purpose of an overlayfs-style mount.

More specifically, an rclone-compatible http backend is constructed
on the fly to serve the `lowerdir` in read-only fashion, and the union
provider is used to join this with the `upperdir` for read-write access.

\b
Example usage:
  ```bash
  rclone-pathmap ':s3,env_auth=True:workbucket/workdir' mnt << END
  /input-file-1: :s3,env_auth=True:mybucket/mybigfile
  /input-file-2: :ftp,host=ftp.example.com:myftp/file
  END
  ```
'''
import sys
import json
import yaml
import click
import asyncio
import logging
import functools
import datetime
from aiohttp import web
from collections import OrderedDict
from datetime import datetime
from dateutil.parser import parse as fromTs

logger = logging.getLogger(__name__)

chunk_size = 8192

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
  proc = await asyncio.create_subprocess_exec(
    'rclone', 'cat', rclone_path,
    stdout=asyncio.subprocess.PIPE,
    stderr=sys.stderr,
  )
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
  logging.debug(f"{mappings=}")
  logging.debug(f"{listing=}")
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

async def _run_app(app):
  runner = web.AppRunner(app)
  await runner.setup()
  site = web.TCPSite(runner, 'localhost', 0)
  await site.start()
  return runner

def _escape_quotes(s):
  return s.replace(r"'", r"''")

async def _serve_and_mount(mappings, upperdir, mountdir, *rclone_flags):
  app = _create_app(mappings)
  runner = await _run_app(app)
  logger.info(runner.addresses)
  ((host, port), *_) = filter(lambda t: len(t)==2, runner.addresses)
  args = (
    'rclone', 'mount',
    f"--http-url=http://{host}:{port}",
    f":union,upstreams='{_escape_quotes(upperdir)} :http::ro':",
    mountdir,
    *rclone_flags,
  )
  logger.debug(' '.join(args))
  proc = await asyncio.create_subprocess_exec(
    *args,
    stdout=sys.stdout,
    stderr=sys.stderr,
  )
  await proc.wait()
  await runner.cleanup()

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

@cli.command(context_settings=dict(ignore_unknown_options=True))
@click.option('-c', '--config', default='-', type=click.File('r'), help='Configuration file (yaml) for lowerdir')
@click.argument('upperdir', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('mountdir', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('rclone_flags', nargs=-1)
def mount(upperdir, mountdir, config, rclone_flags):
  asyncio.run(
    _serve_and_mount(
      yaml.load(config, Loader=yaml.BaseLoader),
      upperdir,
      mountdir,
      *rclone_flags,
    )
  )

if __name__ == '__main__':
  cli()
