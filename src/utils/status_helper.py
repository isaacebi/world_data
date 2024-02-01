import os

def is_run_from_cli():
    return __name__ == '__main__'

def displayText(texts, shown_on_terminal=True):
    if shown_on_terminal:
        cmd = 'echo {}'.format(texts)
        os.system(cmd)

    else:
        print(texts)
