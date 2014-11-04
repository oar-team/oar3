import os
import os.path as op
import click

HERE = op.dirname(__file__)

@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name', help='The person to greet.')
def kao(count, name):
    """Simple program that greets NAME for a total of COUNT times."""
    for x in range(count):
        click.echo('Hello %s!' % name)                                        

if __name__ == '__main__':
    kao()
