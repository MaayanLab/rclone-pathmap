import functools
import logging
import pathlib
import uuid
import tempfile
import asyncio
from rclone_pathmap import RCloneMount, MountTemporaryDirectory

def ensure_async(func):
  def wrapper1(args, kwargs):
    return func(*args, **kwargs)
  @functools.wraps(func)
  async def wrapper2(*args, loop=None, **kwargs):
    loop = asyncio.get_event_loop() if loop is None else loop
    ret = await loop.run_in_executor(None, wrapper1, args, kwargs)
    return ret
  return wrapper2

def assert_dict(a, b):
  assert frozenset(a.items()) == frozenset(b.items()), f"{a=} != {b=}"

def read_and_close(f):
  with f.open('r') as fr:
    return fr.read()

@ensure_async
def _test_init(ctx):
  logging.debug('init')
  # initialize
  workdir = (ctx['tmpdir'] / 'workdir')
  workdir.mkdir(parents=True, exist_ok=True)
  file_1 = workdir / str(uuid.uuid4())
  file_1_content = str(uuid.uuid4())
  with file_1.open('w') as fw:
    fw.write(file_1_content)
  #
  return dict(
    file_1=file_1,
    file_1_content=file_1_content,
    workdir=workdir,
  )

@ensure_async
def _test_00_run(ctx):
  logging.debug('00_run')
  logging.debug(list(ctx['mountdir'].glob('*')))
  assert_dict({
    f.name: read_and_close(f)
    for f in ctx['mountdir'].glob('*')
  },{
    ctx['file_1'].name: ctx['file_1_content'],
  })
  # make change and observe in backend
  file_2 = ctx['mountdir'] / str(uuid.uuid4())
  file_2_content = str(uuid.uuid4())
  with file_2.open('w') as fw:
    fw.write(file_2_content)
  #
  return dict(
    file_2=file_2,
    file_2_content=file_2_content,
  )

@ensure_async
def _test_01_run(ctx):
  logging.debug('01_run')
  assert_dict({
    f.name: read_and_close(f)
    for f in ctx['workdir'].glob('*')
  }, {
    ctx['file_1'].name: ctx['file_1_content'],
    ctx['file_2'].name: ctx['file_2_content'],
  })
  return dict()

async def _test_rclone_mount():
  ctx = {}
  with tempfile.TemporaryDirectory() as tmpdir:
    ctx['tmpdir'] = pathlib.Path(tmpdir)
    ctx.update(await _test_init(ctx))
    # mount
    async with MountTemporaryDirectory() as mountdir:
      async with RCloneMount(
        str(ctx['workdir']),
        str(mountdir),
      ):
        ctx['mountdir'] = mountdir
        _ctx = await _test_00_run(ctx)
        ctx.update(_ctx)
    _ctx = await _test_01_run(ctx)
    ctx.update(_ctx)

def test_rclone_mount():
  loop = asyncio.get_event_loop()
  loop.run_until_complete(_test_rclone_mount())
