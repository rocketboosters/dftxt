import re
import typing

_START_FENCE_PATTERN = re.compile(
    r"(^|\n)(?P<fence>(```|~~~))(?P<type>(dftxt|df))[ \t]*(?P<args>[^\n]*)\n"
)
_END_FENCE_PATTERN = re.compile(r"\n(```|~~~)")


def _parse_args(args: str) -> typing.Dict[str, str]:
    """Parse the arguments of a dftxt fence."""
    exploded = [a.strip() for a in re.split(r"\s+", args)]
    if not exploded:
        return {"name": "", "action": "append"}

    if exploded[0] == "...":
        return {"name": "", "action": "wrap"}

    name = exploded[0]
    if name.endswith("..."):
        name = name[:-3]
        action = "wrap"
    else:
        action = "append"
    return {"name": name, "action": action}


def _combine_frame_sections(frame_sections: typing.Dict[str, typing.List[str]]) -> str:
    """Combine the extracted frame sections into a dftxt file."""
    keys = list(frame_sections.keys())
    if not keys:
        return ""

    if len(keys) == 1 and keys[0] == "":
        return "{}\n".format("\n".join(frame_sections[""]).rstrip())

    frames: typing.List[str] = []
    for key in keys:
        header = "{}---".format("" if not frames else "\n")
        if key:
            header += f" {key} ---"
        frames.append(
            "{}\n\n{}".format(header, "\n".join(frame_sections[key]).rstrip("\n"))
        )

    return "{}\n".format("\n".join(frames).rstrip())


def extract(markdown: str) -> str:
    """Extract dftxt from a markdown file."""
    cleaned = markdown.replace("\r", "")
    offset = 0
    frame_sections: typing.Dict[str, typing.List[str]] = {}
    while offset < len(cleaned):
        opening_match = _START_FENCE_PATTERN.search(cleaned, offset)
        if opening_match is None:
            offset = len(cleaned)
            break

        ending_match = _END_FENCE_PATTERN.search(cleaned, opening_match.end())
        if ending_match is None:
            offset = len(cleaned)
            break

        offset = ending_match.end()

        args = _parse_args(opening_match.group("args"))
        section = cleaned[opening_match.end() : ending_match.start()]
        if args["action"] == "wrap":
            section = f"\n\n{section.strip()}"
        if args["name"] not in frame_sections:
            frame_sections[args["name"]] = []
        frame_sections[args["name"]].append(section)

    return _combine_frame_sections(frame_sections)
