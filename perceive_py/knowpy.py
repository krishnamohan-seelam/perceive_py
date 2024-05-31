from importlib import util as imlib_util
from importlib import metadata as imlib_meta
from itertools import takewhile
import sys


def gen_module_loc(module_names):

    for serial_no, module_name in enumerate(module_names):

        if spec := imlib_util.find_spec(module_name):
            yield serial_no, spec


def gen_dist_info(distributions):
    for serial_no, distribution in enumerate(
        sorted(distributions, key=lambda d: d.name)
    ):
        yield serial_no, distribution.name, distribution.version


def main():
    print("\nmodule names\n")
    for module_serial_no, module_spec in takewhile(
        lambda x: x[0] < 10, gen_module_loc(sys.stdlib_module_names)
    ):
        print(f"{module_serial_no+1}. {module_spec.name:30} {module_spec.origin}")

    print("\ndistributions names\n")
    for dist_serial_no, dist_name, dist_version in takewhile(
        lambda x: x[0] < 10, gen_dist_info(imlib_meta.distributions())
    ):
        print(f"{dist_serial_no+1}. {dist_name:30} {dist_version}")


if __name__ == "__main__":
    main()
