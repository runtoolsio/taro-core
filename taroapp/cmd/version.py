from pathlib import Path


def run(args):
    f = open(Path.cwd() / "setup.py", 'r')
    text = f.read()
    sub = "version=\""
    version = text[text.index(sub) + len(sub):text.index("\",",text.index(sub))]
    print(version)
    f.close()