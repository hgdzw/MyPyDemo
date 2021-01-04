# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import click

# main方法会走这个 如果指定方法的话 要将他们分组
@click.group()
def cli():

    print('main方法执行了...')

# 使函数 runDemo 成为命令行接口
@cli.command(name="runDemo")
# 第一个参数指定了命令行选项的名称，可以看到，count 的默认值是 1；
# @click.option('--count',default=10)
def runDemo():
    print("run方法运行了....")

@cli.command(name="server")
def server():
    print("server....")

if __name__ == '__main__':
    cli()
