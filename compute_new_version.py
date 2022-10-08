import getopt
import re
from dataclasses import dataclass

import sys

VERSION_RE = re.compile(r"^v?(?P<major>\d+)(\.(?P<minor>\d+))?(\.(?P<patch>\d+))?$")


@dataclass
class Version:
    major: int
    minor: int
    patch: int

    def __str__(self):
        return f'{self.major}.{self.minor}.{self.patch}'


def parse_version(version_str: str) -> Version:
    version_str = version_str.strip()
    matches = VERSION_RE.match(version_str)
    if not matches:
        raise Exception(f'version {version_str} is not a legal version string')

    parts = matches.groupdict()
    major = int(parts['major'])
    minor = int(parts.get('minor') or 0)
    patch = int(parts.get('patch') or 0)

    return Version(major=major, minor=minor, patch=patch)


def compute_new_version(repository_version: Version, minimal_required_version: Version) -> Version:
    if minimal_required_version.major < repository_version.major:
        raise Exception(
            f'Repository version is {repository_version} but required version is {minimal_required_version}. '
            f'Major part is incompatible')
    if minimal_required_version.major > repository_version.major:
        return minimal_required_version

    # from here on, minimal.major == repository.major

    if minimal_required_version.minor < repository_version.minor:
        raise Exception(
            f'Repository version is {repository_version} but required version is {minimal_required_version}. '
            f'Minor part is incompatible')

    if minimal_required_version.minor > repository_version.minor:
        return minimal_required_version

    # from here on, minimal.major and minor == repository.major and minor

    if minimal_required_version.patch > repository_version.patch:
        return minimal_required_version

    return Version(major=repository_version.major,
                   minor=repository_version.minor,
                   patch=repository_version.patch + 1)


def main(script_name: str, argv):
    try:
        opts, args = getopt.getopt(argv, "r:m:")
    except getopt.GetoptError:
        print(f'Usage: {script_name} -r <repository_version> -m <minimal_version>', file=sys.stderr)
        sys.exit(2)
    rep_version = None
    min_version = None
    try:
        for opt, val in opts:
            if opt == '-r':
                rep_version = parse_version(val)
            elif opt == '-m':
                min_version = parse_version(val)

        if rep_version is None or min_version is None:
            print(f'Usage: {script_name} -r <repository_version> -m <minimal_version>', file=sys.stderr)
            sys.exit(2)

        print(compute_new_version(repository_version=rep_version, minimal_required_version=min_version)),
    except Exception as ex:
        print(f'ERROR: {str(ex)}', file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[0], sys.argv[1:])
