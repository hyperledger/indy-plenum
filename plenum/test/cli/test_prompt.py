def checkPrompt(cli, checkWith: str):
    promptTokens = cli.cli.application.layout.children[1].children[
        0].content.content.get_tokens("")
    promptStr = ''.join([s for t, s in promptTokens]).strip()
    assert "{}>".format(checkWith) == promptStr


def setPrompt(cli, prompt):
    cli.enterCmd("prompt {}".format(prompt))


def setAndCheckPrompt(cli, prompt):
    setPrompt(cli, prompt)
    checkPrompt(cli, prompt)


def testPrompt(cli):
    # Check if prompts can be changed multiple times.
    setAndCheckPrompt(cli, "Alice")
    setAndCheckPrompt(cli, "Bob")

    # Check whether prompt changes work after some other commands too
    cli.enterCmd("help")
    setAndCheckPrompt(cli, "Jason")
