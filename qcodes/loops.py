"""
Data acquisition loops.

The general scheme is:

1. create a (potentially nested) Loop, which defines the sweep setpoints and
delays

2. activate the loop (which changes it to an ActiveLoop object),
or omit this step to use the default measurement as given by the
Loop.set_measurement class method.

3. run it with the .run method, which creates a DataSet to hold the data,
and defines how and where to save the data.

Some examples:

- set default measurements for later Loop's to use

>>> Loop.set_measurement(param1, param2, param3)

- 1D sweep, using the default measurement set

>>> Loop(sweep_values, delay).run()

- 2D sweep, using the default measurement set sv1 is the outer loop, sv2 is the
  inner.

>>> Loop(sv1, delay1).loop(sv2, delay2).run()

- 1D sweep with specific measurements to take at each point

>>> Loop(sv, delay).each(param4, param5).run()

- Multidimensional sweep: 1D measurement of param6 on the outer loop, and the
  default measurements in a 2D loop

>>> Loop(sv1, delay).each(param6, Loop(sv2, delay)).run()

Supported commands to .set_measurement or .each are:

    - Parameter: anything with a .get method and .name or .names see
      parameter.py for options
    - ActiveLoop (or Loop, will be activated with default measurement)
    - Task: any callable that does not generate data
    - Wait: a delay
"""

from datetime import datetime
import multiprocessing as mp
import time

# from qcodes import config

from qcodes.station import Station
from qcodes.utils.helpers import wait_secs, full_class, tprint
from qcodes.utils.metadata import Metadatable
import zmq
from .actions import (_actions_snapshot, Task, Wait,
                      BreakIf, _QcodesBreak)

# ##### ZMQ #########
ctx = zmq.Context()
SOCKET = ctx.socket(zmq.PUB)
SOCKET.bind("tcp://*:9556")


class Loop(Metadatable):
    """
    The entry point for creating measurement loops

    sweep_values - a SweepValues or compatible object describing what
        parameter to set in the loop and over what values
    delay - a number of seconds to wait after setting a value before
        continuing. 0 (default) means no waiting and no warnings. > 0
        means to wait, potentially filling the delay time with monitoring,
        and give an error if you wait longer than expected.
    progress_interval - should progress of the loop every x seconds. Default
        is None (no output)

    After creating a Loop, you attach `action`s to it, making an `ActiveLoop`
    that you can `.run()`, or you can `.run()` a `Loop` directly, in which
    case it takes the default `action`s from the default `Station`

    `actions` are a sequence of things to do at each `Loop` step: they can be
    `Parameter`s to measure, `Task`s to do (any callable that does not yield
    data), `Wait` times, or other `ActiveLoop`s or `Loop`s to nest inside
    this one.
    """
    
    def __init__(self, sweep_values, delay=0, station=None,
                 progress_interval=None, socket=SOCKET):
        super().__init__()
        if delay < 0:
            raise ValueError('delay must be > 0, not {}'.format(repr(delay)))

        self.sweep_values = sweep_values
        self.delay = delay
        self.station = station
        self.nested_loop = None
        self.actions = None
        self.then_actions = ()
        self.progress_interval = progress_interval
        self.socket = socket


    def loop(self, sweep_values, delay=0):
        """
        Nest another loop inside this one.

        Args:
            sweep_values ():
            delay (int):

        Examples:
            >>> Loop(sv1, d1).loop(sv2, d2).each(*a)

            is equivalent to:

            >>> Loop(sv1, d1).each(Loop(sv2, d2).each(*a))

        Returns: a new Loop object - the original is untouched
        """
        out = self._copy()

        if out.nested_loop:
            # nest this new loop inside the deepest level
            out.nested_loop = out.nested_loop.loop(sweep_values, delay)
        else:
            out.nested_loop = Loop(sweep_values, delay)

        return out

    def _copy(self):
        out = Loop(self.sweep_values, self.delay,
                   progress_interval=self.progress_interval)
        out.nested_loop = self.nested_loop
        out.then_actions = self.then_actions
        out.station = self.station
        return out

    def each(self, *actions):
        """
        Perform a set of actions at each setting of this loop.
        TODO(setting vs setpoints) ? better be verbose.

        Args:
            *actions (Any): actions to perform at each setting of the loop

        Each action can be:
        - a Parameter to measure
        - a Task to execute
        - a Wait
        - another Loop or ActiveLoop
        """
        actions = list(actions)

        # check for nested Loops, and activate them with default measurement
        for i, action in enumerate(actions):
            if isinstance(action, Loop):
                default = Station.default.default_measurement
                actions[i] = action.each(*default)

        self.validate_actions(*actions)

        if self.nested_loop:
            # recurse into the innermost loop and apply these actions there
            actions = [self.nested_loop.each(*actions)]

        return ActiveLoop(self.sweep_values, self.delay, *actions,
                          then_actions=self.then_actions, station=self.station,
                          progress_interval=self.progress_interval,
                          socket=self.socket)

    @staticmethod
    def validate_actions(*actions):
        """
        Whitelist acceptable actions, so we can give nice error messages
        if an action is not recognized
        """
        for action in actions:
            if isinstance(action, (Task, Wait, BreakIf, ActiveLoop)):
                continue
            if hasattr(action, 'get') and (hasattr(action, 'name') or
                                           hasattr(action, 'names')):
                continue
            raise TypeError('Unrecognized action:', action,
                            'Allowed actions are: objects (parameters) with '
                            'a `get` method and `name` or `names` attribute, '
                            'and `Task`, `Wait`, `BreakIf`, and `ActiveLoop` '
                            'objects. `Loop` objects are OK too, except in '
                            'Station default measurements.')

    def run(self, *args, **kwargs):
        """
        shortcut to run a loop with the default measurement set
        stored by Station.set_measurement
        """
        default = Station.default.default_measurement
        return self.each(*default).run(*args, **kwargs)

    def run_temp(self, *args, **kwargs):
        """
        shortcut to run a loop in the foreground as a temporary dataset
        using the default measurement set
        """
        return self.run(*args, background=False, quiet=True,
                        data_manager=False, location=False, **kwargs)

    def then(self, *actions, overwrite=False):
        """
        Attach actions to be performed after the loop completes.

        These can only be `Task` and `Wait` actions, as they may not generate
        any data.

        returns a new Loop object - the original is untouched

        This is more naturally done to an ActiveLoop (ie after .each())
        and can also be done there, but it's allowed at this stage too so that
        you can define final actions and share them among several `Loop`s that
        have different loop actions, or attach final actions to a Loop run
        TODO: examples of this ?
        with default actions.

        *actions: `Task` and `Wait` objects to execute in order

        overwrite: (default False) whether subsequent .then() calls (including
            calls in an ActiveLoop after .then() has already been called on
            the Loop) will add to each other or overwrite the earlier ones.
        """
        return _attach_then_actions(self._copy(), actions, overwrite)

    def snapshot_base(self, update=False):
        """
        State of the loop as a JSON-compatible dict.

        Args:
            update (bool): If True, update the state by querying the underlying
             sweep_values and actions. If False, just use the latest values in
             memory.

        Returns:
            dict: base snapshot
        """
        return {
            '__class__': full_class(self),
            'sweep_values': self.sweep_values.snapshot(update=update),
            'delay': self.delay,
            'then_actions': _actions_snapshot(self.then_actions, update)
        }


def _attach_then_actions(loop, actions, overwrite):
    """Inner code for both Loop.then and ActiveLoop.then."""
    for action in actions:
        if not isinstance(action, (Task, Wait)):
            raise TypeError('Unrecognized action:', action,
                            '.then() allows only `Task` and `Wait` '
                            'actions.')

    if overwrite:
        loop.then_actions = actions
    else:
        loop.then_actions = loop.then_actions + actions

    return loop


class ActiveLoop(Metadatable):
    """

    Created by attaching actions to a `Loop`, this is the object that actually
    runs a measurement loop.

    An `ActiveLoop` can no longer be nested, only run,
    or used as an action inside another `Loop` which will run the whole thing.

    Active loop streams  data over a PUB socket, wheter or not a SUB is
    listening.

    """
    ID = 0
    def __init__(self, sweep_values, delay, *actions, then_actions=(),
                 station=None, progress_interval=None, socket=None):
        super().__init__()
        self.sweep_values = sweep_values
        self.delay = delay
        self.actions = list(actions)
        self.progress_interval = progress_interval
        self.then_actions = then_actions
        self.station = station
        self.socket = socket

        # if the first action is another loop, it changes how delays
        # happen - the outer delay happens *after* the inner var gets
        # set to its initial value
        self._nest_first = type(actions[0]) == ActiveLoop

        # for sending halt signals to the loop
        self.signal_queue = mp.Queue()

        self._monitor = None  # TODO: how to specify this?

    def then(self, *actions, overwrite=False):
        """
        Attach actions to be performed after the loop completes.

        These can only be `Task` and `Wait` actions, as they may not generate
        any data.

        returns a new ActiveLoop object - the original is untouched

        *actions: `Task` and `Wait` objects to execute in order

        overwrite: (default False) whether subsequent .then() calls (including
            calls in an ActiveLoop after .then() has already been called on
            the Loop) will add to each other or overwrite the earlier ones.
        """
        loop = ActiveLoop(self.sweep_values, self.delay, *self.actions,
                          then_actions=self.then_actions, station=self.station)
        return _attach_then_actions(loop, actions, overwrite)

    def snapshot_base(self, update=False):
        """Snapshot of this ActiveLoop's definition."""
        return {
            '__class__': full_class(self),
            'sweep_values': self.sweep_values.snapshot(update=update),
            'delay': self.delay,
            'actions': _actions_snapshot(self.actions, update),
            'then_actions': _actions_snapshot(self.then_actions, update)
        }

    def run_temp(self, **kwargs):
        """
        wrapper to run this loop in the foreground as a temporary data set,
        especially for use in composite parameters that need to run a Loop
        as part of their get method
        """
        return self.run(background=False, quiet=True,
                        data_manager=False, location=False, **kwargs)

    def run(self, quiet=False, station=None, progress_interval=False,
            *args, **kwargs):
        """
        Execute this loop.

        background: (default False) run this sweep in a separate process
            so we can have live plotting and other analysis in the main process
        use_threads: (default True): whenever there are multiple `get` calls
            back-to-back, execute them in separate threads so they run in
            parallel (as long as they don't block each other)
        quiet: (default False): set True to not print anything except errors
        data_manager: set to True to use a DataManager. Default to False.
        station: a Station instance for snapshots (omit to use a previously
            provided Station, or the default Station)
        progress_interval (default None): show progress of the loop every x
            seconds. If provided here, will override any interval provided
            with the Loop definition

        kwargs are passed along to data_set.new_data. These can only be
        provided when the `DataSet` is first created; giving these during `run`
        when `get_data_set` has already been called on its own is an error.
        The key ones are:

        location: the location of the DataSet, a string whose meaning
            depends on formatter and io, or False to only keep in memory.
            May be a callable to provide automatic locations. If omitted, will
            use the default DataSet.location_provider
        name: if location is default or another provider function, name is
            a string to add to location to make it more readable/meaningful
            to users
        formatter: knows how to read and write the file format
            default can be set in DataSet.default_formatter
        io: knows how to connect to the storage (disk vs cloud etc)
        write_period: how often to save to storage during the loop.
            default 5 sec, use None to write only at the end


        returns:
            a DataSet object that we can use to plot
        """
        if progress_interval is not False:
            self.progress_interval = progress_interval

        station = station or self.station or Station.default
        # if station:
            # data_set.add_metadata({'station': station.snapshot()})

        # information about the loop definition is in its snapshot
        # TODO: this is supposedly json so just send it
        # data_set.add_metadata({'loop': self.snapshot()})
        # then add information about how and when it was run
        # ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # data_set.add_metadata({'loop': {
            # 'ts_start': ts,
            # 'background': background,
            # 'use_threads': use_threads,
            # 'use_data_manager': (data_manager is not False)
        # }})

        self._run_wrapper()
        self.socket.send_string("DONE")

    def _run_wrapper(self, *args, **kwargs):
        try:
            self._run_loop(*args, **kwargs)
        except _QuietInterrupt:
            pass
        finally:
            if hasattr(self, 'data_set'):
                # somehow this does not show up in the data_set returned by
                # run(), but it is saved to the metadata
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.data_set.add_metadata({'loop': {'ts_end': ts}})
                self.data_set.finalize()

    def _run_loop(self, first_delay=0, action_indices=(),
                  loop_indices=(), current_values=(),
                  **ignore_kwargs):
        """
        the routine that actually executes the loop, and can be called
        from one loop to execute a nested loop

        first_delay: any delay carried over from an outer loop
        action_indices: where we are in any outer loop action arrays
        loop_indices: setpoint indices in any outer loops
        current_values: setpoint values in any outer loops
        signal_queue: queue to communicate with main process directly
        ignore_kwargs: for compatibility with other loop tasks
        """
        # new active loop
        ActiveLoop.ID += 1
        ID = ActiveLoop.ID 

        # at the beginning of the loop, the time to wait after setting
        # the loop parameter may be increased if an outer loop requested longer
        delay = max(self.delay, first_delay)

        t0 = time.time()
        imax = len(self.sweep_values)
        for i, value in enumerate(self.sweep_values):

            self.sweep_values.set(value)

            if not self._nest_first:
                # only wait the delay time if an inner loop will not inherit it
                self._wait(delay)

            try:
                for index, f in enumerate(self.actions):
                    if type(f) == ActiveLoop:
                        f._run_loop()
                    else:
                        val = f()
                        name = f.name
                        self.socket.send_string("{}/{}/{}/{}/{}".format(ID,
                                                                        i,
                                                                        value,
                                                                        index,
                                                                        name,
                                                                        val))
                    # after the first action, no delay is inherited
                    delay = 0
            except _QcodesBreak:
                break

            # after the first setpoint, delay reverts to the loop delay
            delay = self.delay

        if self.progress_interval is not None:
            # final progress note: set dt=-1 so it *always* prints
            tprint('loop %s DONE: %d/%d (%.1f [s])' % (
                   self.sweep_values.name, i + 1, imax, time.time() - t0),
                   dt=-1, tag='outerloop')

        # the loop is finished - run the .then actions
        for f in self.then_actions:
            f()

    def _wait(self, delay):
        if delay:
            finish_clock = time.perf_counter() + delay
            while True:
                self._check_signal()
                t = wait_secs(finish_clock)
                time.sleep(min(t, self.signal_period))
                if t <= self.signal_period:
                    break

class _QuietInterrupt(Exception):
    pass


class _DebugInterrupt(Exception):
    pass
