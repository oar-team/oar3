# -*- coding: utf-8 -*-
import sys
import click


class CommandReturns(object):
    PRINT = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    TAG2STR = {PRINT: '', INFO: '', WARNING: '#WARNING: ', ERROR: '#ERROR: '}

    def __init__(self, cli=True):
        self.cli = cli
        self.buffering = not cli
        self.buffer = []
        self.exit_values = []
        self.final_exit = 0

    def _print(self, msg_typed_value):
        tag, objs, error = msg_typed_value
        if tag == CommandReturns.PRINT:
            print(objs)
        else:
            #    msg = '{} {} {}'.format(CommandReturns.TAG2STR[tag], objs, error)
            print('{}{}'.format(CommandReturns.TAG2STR[tag], objs), file=sys.stderr)

    def to_str(self):
        str_out = ''
        for msg_typed_value in self.buffer:
            tag, objs, error = msg_typed_value
            str_out += CommandReturns.TAG2STR[tag] + objs
            if error != 0:
                str_out += ' ' + str(error)
            str_out += '\n'
        return str_out

    def get_exit_value(self):
        if self.final_exit == 0:
            prev_ev = 0
            for ev in self.exit_values:
                if ev != 0:
                    if (prev_ev != 0) and (prev_ev != ev):
                        # Multiple errors return error 15
                        return 15
                    else:
                        prev_ev = ev
                        self.final_exit = ev

        return self.final_exit

    def print_or_push(self, tag, msg, error, exit_value):
        self.exit_values.append(exit_value)
        if self.buffering:
            self.buffer.append((tag, msg, error))
        else:
            self._print((tag, msg, error))

    def print_(self, objs):
        self.print_or_push(CommandReturns.PRINT, objs, 0, 0)

    def info(self, objs, error=0, exit_value=0):
        self.print_or_push(CommandReturns.INFO, objs, error, exit_value)

    def warning(self, objs, error=0, exit_value=0):
        self.print_or_push(CommandReturns.WARNING, objs, error, exit_value)

    def error(self, objs, error=0, exit_value=0):
        self.print_or_push(CommandReturns.ERROR, objs, error, exit_value)

    def usage(self, exit_value):
        '''Print usage message.'''
        if self.cli:
            ctx = click.get_current_context()
            click.echo(ctx.get_help())
        self.final_exit = exit_value

    def exit(self, error=None):
        if error:
            exit(error)
        else:
            exit(self.get_exit_value())
