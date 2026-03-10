import py_compile


def test_main_compiles():
    py_compile.compile("main.py", doraise=True)


def test_bot_compiles():
    py_compile.compile("bot.py", doraise=True)


def test_package_main_compiles():
    py_compile.compile("src/bithumb_bot/__main__.py", doraise=True)
