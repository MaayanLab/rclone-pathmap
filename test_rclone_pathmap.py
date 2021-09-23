import functools
import logging
import pathlib
import uuid
import tempfile
import asyncio
from rclone_pathmap import RClonePathmap, MountTemporaryDirectory

def ensure_async(func):
  def wrapper1(args, kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
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
  backend_1 = (ctx['tmpdir'] / 'backend-1')
  backend_1.mkdir(parents=True, exist_ok=True)
  file_1 = backend_1 / str(uuid.uuid4())
  file_1_content = str(uuid.uuid4())
  with file_1.open('w') as fw:
    fw.write(file_1_content)
  backend_2 = (ctx['tmpdir'] / 'backend-2')
  backend_2.mkdir(parents=True, exist_ok=True)
  file_2 = backend_2 / str(uuid.uuid4())
  file_2_content = str(uuid.uuid4())
  with file_2.open('w') as fw:
    fw.write(file_2_content)
  workdir = (ctx['tmpdir'] / 'workdir-1')
  workdir.mkdir(parents=True, exist_ok=True)
  file_3 = workdir / str(uuid.uuid4())
  file_3_content = str(uuid.uuid4())
  with file_3.open('w') as fw:
    fw.write(file_3_content)
  #
  return dict(
    file_1=file_1,
    file_1_content=file_1_content,
    file_2=file_2,
    file_2_content=file_2_content,
    file_3=file_3,
    file_3_content=file_3_content,
    backend_1=backend_1,
    backend_2=backend_2,
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
    'file-1': ctx['file_1_content'],
    'file-2': ctx['file_2_content'],
    ctx['file_3'].name: ctx['file_3_content'],
  })
  # make change and observe in backend
  file_4 = ctx['mountdir'] / str(uuid.uuid4())
  file_4_content = str(uuid.uuid4())
  with file_4.open('w') as fw:
    fw.write(file_4_content)
  #
  return dict(
    file_4=file_4,
    file_4_content=file_4_content,
  )

@ensure_async
def _test_01_run(ctx):
  logging.debug('01_run')
  assert_dict({
    f.name: read_and_close(f)
    for f in ctx['workdir'].glob('*')
  }, {
    ctx['file_3'].name: ctx['file_3_content'],
    ctx['file_4'].name: ctx['file_4_content'],
  })
  return dict()

async def _test_rclone_pathmap():
  ctx = {}
  with tempfile.TemporaryDirectory() as tmpdir:
    ctx['tmpdir'] = pathlib.Path(tmpdir)
    ctx.update(await _test_init(ctx))
    # mount
    async with MountTemporaryDirectory() as mountdir:
      async with RClonePathmap(
        {
          '/file-1': str(ctx['file_1']),
          '/file-2': str(ctx['file_2']),
        },
        str(ctx['workdir']),
        str(mountdir),
      ):
        ctx['mountdir'] = mountdir
        ctx.update(await _test_00_run(ctx))
    ctx.update(await _test_01_run(ctx))

def test_rclone_pathmap():
  loop = asyncio.get_event_loop()
  loop.set_debug(True)
  logging.basicConfig(level=logging.DEBUG)
  loop.run_until_complete(_test_rclone_pathmap())
  logging.info('Completed')

if __name__ == '__main__':
  test_rclone_pathmap()
