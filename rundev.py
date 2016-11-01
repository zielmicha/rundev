#!/usr/bin/python3
# Helpers to manager spawned processes.
# In development, provides one stream listing all events.
# In production, forwards process managements to supervisord.
import argparse, os, tempfile, socket, json, fcntl, contextlib, subprocess, pipes, queue, atexit, threading, signal, sys, glob, pty, time, pwd

# ---------- Common -----------

def clear_env():
    env = {}
    for name in ['HOME', 'USER', 'LOGNAME']:
        if name in os.environ:
            env[name] = os.environ[name]

    env['LANG'] = 'en_US.UTF-8'
    env['PATH'] = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
    os.environ.clear()
    os.environ.update(env)

def parse_env(env):
    d = {}
    for item in (env or []):
        value = item.split('=', 1)
        if len(value) == 1:
            value = (value[0], os.environ[value[0]])
        d[value[0]] = value[1]

    return d

# --------- Devel -----------

CG = None

def setup_cg():
    global CG
    CG = '/sys/fs/cgroup/cpu'

    if os.path.exists(CG + '/lxc/') and len(os.listdir(CG)) == 1:
        # We are running inside LXC
        CG = glob.glob(CG + '/lxc/*')[0]

    CG += '/rundev'

    if not os.access(CG + '/tasks', os.O_RDWR):
        print('Creating cgroup %r for %d...'
              % (CG, os.getuid()))
        subprocess.check_call(['sudo', 'mkdir', CG])
        subprocess.check_call(['sudo', 'chown', str(os.getuid()), CG])

def add_to_cg():
    # Sometimes LXCFS (or cgroupfs?) perpetually returns 0 from os.write, hanging file.write function. We workaround this bug (?) by reopening FD.
    for i in range(30):
        with open(CG + '/tasks', 'a') as f:
            s = (str(os.getpid()) + '\n').encode()
            result = os.write(f.fileno(), s)
            if result == len(s):
                return

    raise OSError('could not add task to cgroup (returned %d)' % result)

class PtyPopen():
    # Runs children in a new PTY.
    def __init__(self, args, environ, chdir):
        self.args = args
        self.environ = environ
        self.chdir = chdir
        pid, master = pty.fork()

        if pid == 0:
            add_to_cg()
            try:
                self._child()
            except:
                traceback.print_exc()
            finally:
                os._exit(0)
        else:
            self.pid = pid
            self.stdout = os.fdopen(master, 'rb', 0)

    def _child(self):
        for k, v in self.environ:
            os.environ[k] = v
        if self.chdir:
            os.chdir(self.chdir)
        os.execvp(self.args[0], self.args)

def kill_cg():
    for i in range(5):
        try:
            tasks = open(CG + '/tasks').read().split()
        except OSError:
            return

        tasks = [ pid for pid in tasks
                  if int(pid) != os.getpid() ]

        print('[Killing tasks: %s]' % ' '.join(tasks))

        if i == 0:
            sig = 15
        else:
            sig = 9

        if not tasks: break
        subprocess.call(['sudo', 'kill', '-%d' % sig] + tasks)
        time.sleep(0.3)

class colors:
    gray = '\033[37m'
    red = '\033[31m'
    bg_red = '\033[101m'
    reset = '\033[0m'

class DevServer():
    def __init__(self):
        self.output_queue = queue.Queue(100)
        self.children = {}

    def child(self, sock):
        info = json.loads(sock.makefile().readline())
        name = info['name']
        if info['subname']:
            name = info['subname'] + '/' + name

        if name in self.children:
            sock.sendall(b'A') # confirmation
            self.output_queue.put((name, 'child already running'))
            return

        child = PtyPopen(info['command'], environ=info['env'],
                         chdir=info['chdir'])
        self.children[name] = child
        if name != '_initial':
            msg = ('started: %s\n' % ' '.join(map(str, info['command'])))
            self.output_queue.put((name, msg.encode('utf8')))

        sock.sendall(b'A') # confirmation

        while True:
            try:
                line = child.stdout.readline()
            except OSError:
                break
            if not line:
                break

            self.output_queue.put((name, line))

        _, status = os.waitpid(child.pid, 0)

        exit_info = ('exited with status %d' % status) if info['oneshot'] else (colors.bg_red + '!!! PROCESS EXITED !!!' + colors.reset)
        del self.children[name]
        self.output_queue.put((name, exit_info.encode() + b'\n'))

    def finish(self):
        kill_cg()
        os._exit(0)

    def output_handler(self):
        max_name_length = 10
        while True:
            name, line = self.output_queue.get()
            max_name_length = max(max_name_length, len(name))
            sys.stdout.buffer.write(((colors.gray + '[%s] ' + colors.reset) % (name.ljust(max_name_length))).encode())
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.write(colors.reset.encode())
            sys.stdout.buffer.flush()

            if len(self.children) == 0 and self.output_queue.qsize() == 0:
                print('No more running processes, exiting.')
                self.finish()

    def main(self, command, env):
        setup_cg()
        tmp_dir = tempfile.mkdtemp()
        socket_path = tmp_dir + '/rundev.socket'
        sock = socket.socket(socket.AF_UNIX)
        sock.bind(socket_path)
        sock.listen(5)

        clear_env()
        os.environ['RUNDEV_SOCKET'] = socket_path
        os.environ.update(parse_env(env))
        if os.environ.get('EXTPATH'):
            os.environ['PATH'] = os.environ['EXTPATH'] + ':' + os.environ['PATH']

        atexit.register(os.rmdir, tmp_dir)
        atexit.register(os.unlink, socket_path)

        threading.Thread(target=add, kwargs={
            'name': '_initial',
            'command': command,
            'oneshot': True
        }).start()

        threading.Thread(target=self.output_handler).start()

        signal.signal(signal.SIGINT, lambda *_: self.finish())

        while True:
            child, addr = sock.accept()
            threading.Thread(target=self.child, args=[child]).start()

# --------- Production -----------

runtime_dir = None

def check_runtime_dir(create=False):
    global runtime_dir
    subname = os.environ.get('RUNDEV_SUBNAME', 'rundev')
    link = os.path.expanduser('~/.config/%s' % subname)
    if not os.path.exists(os.path.dirname(link)):
        os.mkdir(os.path.dirname(link))
    if not os.path.exists(link):
        if not create:
            raise Exception('supervisord not yet running (use `rundev init`)')
        try:
            os.unlink(link)
        except OSError:
            pass
        dirname = tempfile.mkdtemp()
        os.symlink(dirname, link)

    runtime_dir = os.readlink(link)

@contextlib.contextmanager
def lock():
    with open(runtime_dir + '/lock', 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield

def which(path, program):
    fpath, fname = os.path.split(program)
    if fpath:
        return program
    else:
        for path in path.split(':'):
            path = path.strip('"')
            if not path: continue
            exe_file = os.path.join(path, program)
            if os.path.isfile(exe_file):
                return exe_file

    return program

def create_supervisor_config():
    with open(runtime_dir + '/processes.json', 'r') as f:
        info = json.loads(f.read())

    config = '''
[supervisord]
childlogdir={logdir}

[unix_http_server]
file={runtime_dir}/supervisord.sock
chmod=0700

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix://{runtime_dir}/supervisord.sock
'''.format(runtime_dir=runtime_dir, logdir=info['logdir'])

    def supervisor_quote(s):
        return pipes.quote(s).replace('%', '%%')

    for process in info['processes'].values():
        environ = dict(info['env'])
        environ.update(process['env'])

        command = list(process['command'])
        path = os.environ['PATH']
        if environ.get('EXTPATH'):
            path = environ['EXTPATH'] + ':' + path

        command[0] = which(path, command[0]) # supervisord doesn't handle PATH properly
        config += '''
[program:{name}]
command={command}
redirect_stderr=True
'''.format(name=process['name'],
           command=' '.join(map(supervisor_quote, command)))
        if process['oneshot']:
            config += 'startsecs=0\nautorestart=false\n'

        if process['chdir']:
            config += 'directory=%s\n' % supervisor_quote(process['chdir'])

        for k, v in environ.items():
            config += 'environment=%s=%s\n' % (supervisor_quote(k), supervisor_quote(v))

        if 'EXTPATH' in environ:
            config += 'environment=PATH=%s\n' % supervisor_quote(path)

        if process.get('user'):
            config += 'user=%s\n' % supervisor_quote(process['user'])
            config += 'environment=HOME=%s\n' % supervisor_quote(pwd.getpwnam(process['user']).pw_dir)

    with open(runtime_dir + '/supervisord.conf', 'w') as f:
        f.write(config)

def save_process(process_info):
    with open(runtime_dir + '/processes.json', 'r') as f:
        info = json.loads(f.read())

    info['processes'][process_info['name']] = process_info

    with open(runtime_dir + '/processes.json', 'w') as f:
        f.write(json.dumps(info, indent=2))

def start_supervisor():
    if not os.path.exists(runtime_dir + '/supervisord.sock'):
        create_supervisor_config()

        subprocess.check_call(['supervisord', '-c', runtime_dir + '/supervisord.conf'])

def add_process(info):
    check_runtime_dir(create=False)
    with lock():
        save_process(info)
        ctl = ['supervisorctl', '-c', runtime_dir + '/supervisord.conf']
        create_supervisor_config()
    subprocess.check_call(ctl + ['reread'])
    subprocess.check_call(ctl + ['update'])

def run_ctl(command):
    check_runtime_dir(create=False)
    cmd = ['supervisorctl', '-c', runtime_dir + '/supervisord.conf'] + command
    os.execvp(cmd[0], cmd)

def init_production(logdir, env):
    check_runtime_dir(create=True)
    if not logdir:
        logdir = os.path.expanduser('~/.logs')
    try:
        os.mkdir(logdir)
    except OSError:
        pass

    with lock():
        with open(runtime_dir + '/processes.json', 'w') as f:
            f.write(json.dumps({
                'processes': {},
                'env': parse_env(env),
                'logdir': logdir
            }))

        start_supervisor()

# ---------- Commands -------------

def add(name, command, env={}, user=None, oneshot=False, chdir=None):
    info = {
        'name': name,
        'command': command,
        'env': env,
        'chdir': chdir,
        'oneshot': oneshot,
        'user': user,
    }
    if 'RUNDEV_SOCKET' in os.environ:
        # development, send arguments to development console
        sock = socket.socket(socket.AF_UNIX)
        info['subname'] = os.environ.get('RUNDEV_SUBNAME')
        sock.connect(os.environ['RUNDEV_SOCKET'])
        sock.sendall((json.dumps(info) + '\n').encode())
        sock.recv(1)
        sock.close()
    else:
        add_process(info)

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')

    subparser = subparsers.add_parser(
        'dev',
        help='Runs process inside development console.')

    subparser.add_argument('command', nargs='+')
    subparser.add_argument('--env', action='append')

    subparser = subparsers.add_parser(
        'init',
        help='Initializes production environment.')
    subparser.add_argument('--logdir')
    subparser.add_argument('--env', action='append')
    subparser.add_argument('command', nargs='*')

    subparser = subparsers.add_parser(
        'ctl',
        help='Runs supervisorctl.')
    subparser.add_argument('command', nargs='*')

    subparser = subparsers.add_parser(
        'add',
        help='Adds or updates and spawns a new process.')

    subparser.add_argument('name',
                           help='Process name.')
    subparser.add_argument('command',
                           nargs='+',
                           help='Command to run')
    subparser.add_argument('--oneshot',
                           action='store_true',
                           help='Is it normal for this process to exit?')
    subparser.add_argument('--user',
                           help='Change user before executing')

    ns = parser.parse_args()
    if ns.action == 'dev':
        DevServer().main(ns.command, ns.env)
    elif ns.action == 'ctl':
        run_ctl(ns.command)
    elif ns.action == 'init':
        init_production(logdir=ns.logdir, env=ns.env)
        if ns.command:
            os.execvp(ns.command[0], ns.command)
    elif ns.action == 'add':
        add(ns.name, ns.command, oneshot=ns.oneshot, user=ns.user)
    else:
        parser.print_usage()

if __name__ == '__main__':
    main()
