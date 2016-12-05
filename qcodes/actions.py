"""Actions, mainly to be executed in measurement Loops."""
import time

from qcodes.utils.deferred_operations import is_function
from qcodes.utils.threading import thread_map


_NO_SNAPSHOT = {'type': None, 'description': 'Action without snapshot'}


def _actions_snapshot(actions, update):
    """Make a list of snapshots from a list of actions."""
    snapshot = []
    for action in actions:
        if hasattr(action, 'snapshot'):
            snapshot.append(action.snapshot(update=update))
        else:
            snapshot.append(_NO_SNAPSHOT)
    return snapshot


class Task:
    """
    A predefined task to be executed within a measurement Loop.

    The first argument should be a callable, to which any subsequent
    args and kwargs (which are evaluated before the loop starts) are passed.

    The args and kwargs are first evaluated if they are found to be callable.

    Keyword Args passed when the Task is called are ignored,
    but are accepted for compatibility with other things happening in a Loop.

    Args:
        func (callable): Function to executed
        *args: pass to func, after evaluation if callable
        **kwargs: pass to func, after evaluation if callable

    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, **ignore_kwargs):
        # If any of the arguments are callable, evaluate them first
        eval_args = [arg() if callable(arg) else arg for arg in self.args]
        eval_kwargs = {k: (v() if callable(v) else v) for k, v in self.kwargs.items()}

        self.func(*eval_args, **eval_kwargs)

    def snapshot(self, update=False):
        """
        Snapshots  task
        Args:
            update (bool): TODO not in use

        Returns:
            dict: snapshot
        """
        return {'type': 'Task', 'func': repr(self.func)}


class Wait:
    """
    A simple class to tell a Loop to wait <delay> seconds.

    This is transformed into a Task within the Loop, such that
    it can do other things (monitor, check for halt) during the delay.

    But for use outside of a Loop, it is also callable (then it just sleeps)

    Args:
        delay: seconds to delay

    Raises:
        ValueError: if delay is negative
    """
    def __init__(self, delay):
        if not delay >= 0:
            raise ValueError('delay must be > 0, not {}'.format(repr(delay)))
        self.delay = delay

    def __call__(self):
        if self.delay:
            time.sleep(self.delay)

    def snapshot(self, update=False):
        """
        Snapshots  delay
        Args:
            update (bool): TODO not in use

        Returns:
            dict: snapshot
        """
        return {'type': 'Wait', 'delay': self.delay}


class _Nest:

    """
    Wrapper to make a callable nested ActiveLoop.

    This should not be constructed manually, only by an ActiveLoop.
    """

    def __init__(self, inner_loop, action_indices):
        self.inner_loop = inner_loop
        self.action_indices = action_indices
        self.name = "nested loop {}".format(action_indices)

    def __call__(self, **kwargs):
        print("{}".format(self.action_indices))
        self.inner_loop._run_loop(action_indices=self.action_indices, **kwargs)
        return "inner_loop"


class BreakIf:

    """
    Loop action that breaks out of the loop if a condition is truthy.
    
    Args:
        condition (callable): a callable taking no arguments.
            Can be a simple function that returns truthy when it's time to quit
            May also be constructed by deferred operations on `Parameter`.
    Raises:
        TypeError: if condition is not a callable with no aguments.

    Examples:
            >>> BreakIf(gates.chan1 >= 3)
            >>> BreakIf(abs(source.I * source.V) >= source.power_limit.get_latest)
    """

    def __init__(self, condition):
        if not is_function(condition, 0):
            raise TypeError('BreakIf condition must be a callable with '
                            'no arguments')
        self.condition = condition

    def __call__(self, **ignore_kwargs):
        if self.condition():
            raise _QcodesBreak

    def snapshot(self, update=False):
        # TODO: make nice reprs for DeferredOperations
        """
        Snapshots breakif action
        Args:
            update (bool): TODO not in use

        Returns:
            dict: snapshot

        """
        return {'type': 'BreakIf', 'condition': repr(self.condition)}


class _QcodesBreak(Exception):
    pass
