import typing


def given(condition: bool, yes: typing.Any, no: typing.Any) -> typing.Any:
    """Get value based on the first conditional argument."""
    return yes if condition else no
