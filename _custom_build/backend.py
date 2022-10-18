from pathlib import Path

# noinspection PyUnresolvedReferences
from setuptools.build_meta import *

from make_all_extra_requirements import generate_all_requirements

package_dir = Path(__file__).parent.parent
generate_all_requirements(path=package_dir)
