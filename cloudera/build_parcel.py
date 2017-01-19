#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run like this:
# build_parcel.py --input-directory ../dist/build_output --output-directory\
# ../dist/repo_output/parcels --release-version 1\
# --spark2-version 2.0.0.cloudera1-SNAPSHOT --cdh-version 5.7.0 --build-number\
# 106597 --patch-number 0 --verbose --force-clean

from __future__ import with_statement
from __future__ import print_function
import argparse
from collections import defaultdict, OrderedDict
import distutils.spawn
import getpass
import glob
import hashlib
import json
import logging
import multiprocessing
import os
import re
import shlex
import shutil
import subprocess
import sys
import time

import util
import cauldron.osinfo as osinfo

# This script requires Python 2.7 or later, so does the make_manifest.py script
# in the CM repo being called. Also, this hasn't been tested with Python3.

LOG = None
SUPPORTED_OSES=[]

TRACE_TIMES = []
def timer_decorate(func):
    """Decorator function used to print timing information."""
    def func_wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        end = time.time()
        delta = end - start
        delta_ms = delta * 1000
        LOG.debug("Function %s executed in %d ms", func.__name__, delta_ms)
        TRACE_TIMES.append((func.__name__, delta_ms))
        return ret
    return func_wrapper

def log_timer_traces():
    LOG.info("Collected trace timings:")
    for t in TRACE_TIMES:
        LOG.info("\t%s ran in %d ms", t[0], t[1])

class Extractor(object):
    """Deep copies contents of the build from input directory to output
    directory and performs some manual fixups.
    This includes things like relativizing symlinks, mangling shell scripts,
    and relocating files."""

    def __init__(self, in_dir, out_dir):
        self._in_dir = in_dir
        self._out_dir = out_dir
        self._meta_dir = None

    @timer_decorate
    def _deploy_bits(self):
        """Deploy the bits. Copies over the contents from input directory,
        assuming the packages are pre-extracted.
        """
        # Setup output directory
        LOG.info("Copying bits from %s to %s", self._in_dir, self._out_dir)
        shutil.copytree(self._in_dir, self._out_dir, symlinks=True)

    @timer_decorate
    def _precheck(self):
        # Check that no tools.jar or jdk.tools*.jar is present, it has an incompatible license
        # See: CDH-21987 and related.
        patterns = [re.compile(p) for p in [r'tools.jar', r'jdk\.tools.*\.jar']]
        for root, dirs, files in os.walk(self._out_dir):
            for f in files:
                for p in patterns:
                    if p.match(f) is not None:
                        raise Exception("Found JDK tools jar with incompatible license: " +\
                                        os.path.join(root, f))

    @timer_decorate
    def _fixup_special_cases(self):
        self._fixup_special_case_spark2()
        self._fixup_global()
        LOG.info("Finished package-specific and global parcel fixups")

    def _fixup_special_case_spark2(self):
        # Insert some fixed up environment variables for Spark in wrapper script
        scripts = ["pyspark2", "spark2-shell", "spark2-submit"]
        lines = """export HADOOP_HOME=$CDH_LIB_DIR/hadoop\n"""
        util.insert_lines_head(self._out_dir, scripts, lines)

    @timer_decorate
    def _fixup_global(self):
        # Fix bin scripts to point to ../lib rather than /usr/lib
        bin_dir = os.path.join(self._out_dir, "usr/bin/")
        scripts = os.listdir(bin_dir)

        # Transform into absolute paths
        scripts = [os.path.join(bin_dir, x) for x in scripts]
        # Filter out not-files
        scripts = [x for x in scripts if os.path.isfile(x)]
        # Filter out ELF binaries
        scripts = [x for x in scripts if not util.is_elf_binary(x)]
        # Filter out bigtop-detect-javahome
        scripts = [x for x in scripts if not x.endswith("/bigtop-detect-javahome")]

        # Process scripts
        # Insert lines to set the correct LIB_DIR
        lines = \
"""  # Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
  SOURCE="${BASH_SOURCE[0]}"
  BIN_DIR="$( dirname "$SOURCE" )"
  while [ -h "$SOURCE" ]
  do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
  done
  BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  CDH_LIB_DIR=$BIN_DIR/../../CDH/lib
  LIB_DIR=$BIN_DIR/../lib
"""
        util.insert_lines_head(self._out_dir, scripts, lines)

        # Do regex find and replace to rewrite references to absolute paths
        replacements = [["/usr/lib/bigtop-utils", "$CDH_LIB_DIR/bigtop-utils"],
                   ["/usr/libexec/", "$BIN_DIR/../libexec/"],
                   ["/usr/lib/", "$LIB_DIR/"],
                   ["/etc/default", "$BIN_DIR/../etc/default"],
                   ["#!/bin/sh", "#!/bin/bash"]]
        util.find_and_replace(scripts, replacements)

        # Wipe out the debug packages
        debug_path = os.path.join(self._out_dir, "usr/lib/debug")
        util.rmtree(debug_path)
        # Clean out man pages
        util.rmtree(os.path.join(self._out_dir, "usr/share/man"))
        # Clean out docs
        util.rmtree(os.path.join(self._out_dir, "usr/share/doc"))

    @timer_decorate
    def _check_symlinks(self):
        num_relative = 0
        num_absolute = 0
        broken_absolute = []
        broken_relative = []
        for root, dirs, files in os.walk(self._out_dir):
            for f in files:
                link = os.path.join(root, f)
                if not os.path.islink(link):
                    continue
                target = os.readlink(link)
                if os.path.isabs(target):
                    # Don't verify all absolute links.
                    # Locations like /etc/ and /var are okay
                    if target.startswith("/etc") or target.startswith("/var"):
                        num_absolute += 1
                    else:
                        broken_absolute.append((link, target))
                else:
                    # Don't verify relative links that do not point within the out_dir
                    abs_target = os.path.normpath(os.path.join(root, target))
                    if not os.path.exists(abs_target):
                        broken_relative.append((link, target))
                    num_relative += 1
        LOG.info("Skipped checking %d absolute symlinks", num_absolute)
        LOG.info("Found %d resolvable relative symlinks", num_relative)

        if len(broken_absolute) > 0 or len(broken_relative) > 0:
            LOG.error("Found %d invalid absolute symlinks" % len(broken_absolute))
            for link, target in broken_absolute:
                LOG.error("Bad absolute link from %s to %s", link, target)
            LOG.error("Found %d dangling relative symlinks" % len(broken_relative))
            for link, target in broken_relative:
                LOG.error("Dangling link from %s to %s", link, target)
            raise Exception("Found dangling or invalid symlinks")

    @timer_decorate
    def extract(self):
        self._deploy_bits()
        self._precheck()
        self._fixup_special_cases()
        self._check_symlinks()

class Component(object):
    """Component-level metadata. Not all packages are their own component."""
    def __init__(self, path):
        """Read in the spark2_version.properties in the cloudera folder of a component"""
        assert os.path.split(os.path.dirname(path))[1] == "cloudera", "Properties file %s is not in a cloudera directory" % path
        assert os.path.basename(path) == "spark2_version.properties", "Properties file %s is not named spark2_version.properties" % path
        self.properties = {}
        with open(path, "r") as infile:
            for line in infile:
                try:
                    # Trim off comment
                    line = line.split("#")[0]
                    # Ignore empty and whitespace-only lines
                    if line == "" or line.isspace():
                        continue
                    key, value = line.split("=")
                    self.properties[key] = value
                except Exception as e:
                    LOG.error("Error while parsing %s line %s" % (path, line))
                    raise e

class Metadata(object):
    """Write parcel metadata. These are json files stored in a meta/ folder.

    * parcel.json specifies what functionality provided by the parcel, as well as
            environment variables about each component (e.g. homedir, shell, groups)
    * permissions.json specifies non-standard permissions for certain files,
            e.g. if an executable needs suid or sticky bit.
    * alternatives.json specifies alternatives (i.e. /usr/sbin/alternatives) for
            different commands or config dirs

    All of this is documented in greater detail on the CM github wiki:
        https://github.com/cloudera/cm_ext/wiki/The-parcel-format
        https://github.com/cloudera/cm_ext/wiki/The-parcel.json-file
        etc.
    """

    def __init__(self, out_dir, version):
        self._out_dir = out_dir
        self._meta_dir = os.path.join(self._out_dir, "meta")
        self._version = version
        self._components = self._scan_components(out_dir)


    @staticmethod
    def _scan_components(out_dir):
        """Look for cdh_version.properties files in the output directory, parse them,
        and return a list of representative Component objects."""
        paths = glob.glob(os.path.join(out_dir, "usr/lib/*/cloudera/spark2_version.properties"))
        LOG.info("Found %d spark2_version.properties files", len(paths))
        assert paths > 0, "Did not find any spark2_version.properties files!"
        components = []
        for path in paths:
            components.append(Component(path))
        return components

    def _write_parcel_json(self):
        parcel = {
            "schema_version": 1,
            "name": "SPARK2",
            "version": self._version.version(),
            "extraVersionInfo": {
                "baseVersion": self._version.base_version(),
                "patchCount": str(self._version.patch_count()),
            },
            "replaces": "SPARK",
            "depends" : "CDH (>= 5.7), CDH (<< 5.12)",
            "setActiveSymlink": True,
            "scripts": {
                "defines": "spark2_env.sh",
            },
            "provides": [
                "spark2",
                "cdh-plugin",
            ],
            "users": {
                "spark": {
                    "longname"    : "Spark",
                    "home"        : "/var/lib/spark",
                    "shell"       : "/sbin/nologin",
                    "extra_groups": [ ]
                }
            },

            "groups": [
                "spark"
            ]
        }
        # Packages
        parcel["packages"] = []

        # Components
        parcel["components"] = []
        for c in self._components:
            parcel["components"].append({
                "name": c.properties["cloudera.pkg.name"],
                "version": c.properties["version"],
                "pkg_version": c.properties["cloudera.pkg.version"],
                "pkg_release": c.properties["cloudera.pkg.release"],
            })

        out_file = os.path.join(self._meta_dir, "parcel.json")
        with open(out_file, "wt") as o:
            json.dump(parcel, o, sort_keys=True, indent=4, separators=(',', ': '))

    def _write_env_sh(self):
        env_sh = """\
#!/bin/bash
CDH_DIRNAME=${PARCEL_DIRNAME:-"SPARK2-%s"}
export CDH_SPARK2_HOME=$PARCELS_ROOT/$CDH_DIRNAME/lib/spark2
""" % self._version.version()

        out_file = os.path.join(self._meta_dir, "spark2_env.sh")
        with open(out_file, "wt") as o:
            o.write(env_sh)

    def _write_alternatives_json(self):
        # Adding a note just as a reminder - Originally BINARIES referred to various
        # binary wrappers (scripts from original components that needed to have
        # bigtop-detect-javahome called for them to work properly in a parcel
        # environment) but we subsequently added bigtop-detect-javahome to this list.
        # Just making a point of calling this out in case we change the semantics of
        # the following block somehow in the future in which case we may need to
        # handle this particular file differently
        binaries = ["pyspark2", "spark2-shell", "spark2-submit"]
        conf_dirs = ["spark2"]

        alternatives = {}
        for b in binaries:
            alternatives[b] = {
                "destination": "/usr/bin/%s" % b,
                "source": "bin/%s" % b,
                "priority": 10,
                "isDirectory": False
            }
        for c in conf_dirs:
            alternatives[c+"-conf"] = {
                "destination": "/etc/%s/conf" % c,
                "source": "etc/%s/conf.dist" % c,
                "priority": 10,
                "isDirectory": True
            }
        out_file = os.path.join(self._meta_dir, "alternatives.json")
        with open(out_file, "wt") as o:
            json.dump(alternatives, o, sort_keys=True, indent=4, separators=(',', ': '))

    def _create_release_notes(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if self._version._patch_number == 0:
            if os.path.isfile(os.path.join(script_dir, 'release-notes.txt')):
              with open(os.path.join(script_dir, 'release-notes.txt'), 'r') as release_notes_file_for_normal_releases:
                  release_notes_txt_normal=release_notes_file_for_normal_releases.read()
                  self._write_release_notes(release_notes_txt_normal)
        else:
            if os.path.isfile(os.path.join(script_dir, 'patch-release-notes.txt')):
                with open(os.path.join(script_dir, 'patch-release-notes.txt'), 'r') as release_notes_file_for_patch:
                    release_notes_txt_patch=release_notes_file_for_patch.read()
                    # The following line makes sure that patch-release-notes.txt file is not empty and it doesn't contain only spaces.
                    if (not release_notes_txt_patch.isspace()) and (release_notes_txt_patch):
                        self._write_release_notes(release_notes_txt_patch)
                    else:
                        raise Exception("""Release notes are required for patch builds. Please write the release notes in $SPARK_HOME/cloudera/patch-release-notes.txt""")
            # The following line will raise an exception if the release notes files for patches was not provided.
            else:
                raise Exception("""Release notes are required for patch builds. Please provide patch-release-notes.txt in $SPARK_HOME/cloudera folder""")

    def _write_release_notes(self, release_notes_content):
        release_notes_file = os.path.join(self._meta_dir, "release-notes.txt")
        with open(release_notes_file, 'w') as release_notes:
            release_notes.write(release_notes_content)

    @timer_decorate
    def write_metadata(self):
        util.mkdirs_recursive(self._meta_dir)

        self._write_parcel_json()
        self._write_env_sh()
        self._write_alternatives_json()
        self._create_release_notes()

class Version(object):

    def __init__(self, release_version, spark2_version, cdh_version,
                 patch_number, build_number, distro):
        self._release_version = release_version
        # Sometimes versions being sent have SNAPSHOT at their end, if so, let's get rid of
        # it because we don't want that showing up in parcel names, etc.
        self._spark2_version = spark2_version.replace("-SNAPSHOT", "")
        self._cdh_version = cdh_version.replace("-SNAPSHOT", "")
        self._patch_number = patch_number
        self._build_number = build_number
        self._distro = distro
        self._validate()

    @staticmethod
    def _validate_int(i, arg_name):
        assert i >= 0, "Argument %s is negative!" % arg_name

    def _validate(self):
        Version._validate_int(self._release_version, "release-version")

        # Version is expected to be something like "2.0.0.cloudera[something here]".
        version_re = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+\.cloudera.+")
        assert version_re.match(self._spark2_version), """Expected dotted version vector
          (upstream.3digit.number.cloudera*), got %s""" % self._spark2_version

        # Now let's validate cdh_version
        splitted_cdh_version = self._cdh_version.split(".")
        assert len(splitted_cdh_version) >= 2, \
                """Expected dotted version vector (major.minor.maintenance) or
                (major.minor) in cdh-version, got %s""" % self._cdh_version
        for v in splitted_cdh_version:
            assert v == "x" or re.match("^\d+$", v), "%s is not a valid version component" % (v,)

        Version._validate_int(self._patch_number, "patch-number")
        Version._validate_int(self._build_number, "build-number")
        if self._distro:
            for dist in self._distro[0::]:
                assert dist in SUPPORTED_OSES, "Unknown distro %s" % self._distro
        # Let's create one parcel, then we will rename and copy it.
        if not self._distro:
            self._distro = SUPPORTED_OSES

    def patch_count(self):
        return self._patch_number

    def base_version(self):
        # Ex. "cdh5.7.0"
        return "cdh" + self._cdh_version

    def version(self):
        # Ex. "2.0.0.cloudera1-1.cdh5.7.0.p593.106617"
        fmt = "%s-%d.%s.p%d.%d"
        return fmt % (self._spark2_version, self._release_version, self.base_version(), self._patch_number, self._build_number)

    def full_version(self):
        # Ex. "2.0.0.cloudera1-1.cdh5.7.0.p593.106617-el6"
        fmt = "%s-%s"
        distro = osinfo.parse_name(self._distro[0]).parcel_label()
        return fmt % (self.version(), distro)

    @staticmethod
    def add_arguments(parser):
        # Version arguments
        parser.add_argument('--release-version',
                            dest="release_version",
                            required=True,
                            type=int,
                            help="Release version. This is almost always 1.")
        parser.add_argument('--spark2-version',
                            dest="spark2_version",
                            required=True,
                            help="Spark2 version as a dotted version string e.g. 2.0.0.cloudera1")
        parser.add_argument('--cdh-version',
                            dest="cdh_version",
                            required=True,
                            help="CDH version that this version of spark2 was built with " + \
                            "(without 'cdh'), e.g. 5.7.0")
        parser.add_argument("--patch-number",
                            dest="patch_number",
                            type=int,
                            required=True,
                            help="Patch number. Specify the PATCH JIRA #." + \
                                " For a normal release, this should be set to 0.")
        parser.add_argument("--build-number",
                            dest="build_number",
                            type=int,
                            required=True,
                            help="Global build number." + \
                                " This is the unique Global Build Number (GBN) for this build.")
        parser.add_argument('--distro',
                            action='append',
                            dest="distro",
                            required=False,
                            help="Target distribution, e.g. RedHat6, Ubuntu1204, etc." +\
                                " If none specified, it's assumed that the bits contain no native content" +\
                                " and all supported parcels are generated as renamed copies of each other.")

    @staticmethod
    def build_from_args(args):
        return Version(args.release_version, args.spark2_version, args.cdh_version,
                       args.patch_number, args.build_number, args.distro)

class Archiver(object):
    def __init__(self, out_dir, build_dir, version, skip_archive, distro):
        self._out_dir = out_dir
        self._build_dir = build_dir
        self._version = version
        self._meta_dir = None
        self._parcel_dir = os.path.join(self._out_dir, "SPARK2-" + self._version.version())
        self._skip_archive = skip_archive
        self._distro = distro

    @staticmethod
    def add_arguments(parser):
        # Archiver arguments
        parser.add_argument('--skip-archive',
                            action='store_true',
                            dest="skip_archive",
                            help="Skip the final archive step, leaving the parcel as a directory.")

    def _stitch_parcel(self):
        # Do a rename/swap to stitch things into a proper parcel layout with the right directory name
        # We only take a subset of all the directories, and also move things in "usr" up one level
        os.mkdir(self._parcel_dir)
        subdirs = ["usr/lib", "usr/bin", "meta", "etc", "usr/share"]
        for d in subdirs:
            src_path = os.path.join(self._build_dir, d)
            dst_path = os.path.join(self._parcel_dir, os.path.basename(src_path))
            if os.path.isdir(src_path):
                os.rename(src_path, dst_path)
        LOG.info("Staged parcel contents to %s", self._parcel_dir)

    @timer_decorate
    def _archive_parcel(self):
        # If pigz (parallel gz) is present, use it. Good speedup.
        # See: http://askubuntu.com/questions/62607/whats-the-best-way-to-use-parallel-bzip2-and-gzip-by-default
        cmd = "tar -czf %s %s"
        pigz_path = distutils.spawn.find_executable("pigz")
        if pigz_path is not None:
            cmd = "tar -c --use-compress-program=" + pigz_path + " -f %s %s"
        else:
            LOG.warn("No pigz binary found, falling back to serial gzip implementation.")

        # Create parcel archive
        parcel_archive = "SPARK2-" + self._version.full_version() + ".parcel"
        subprocess.check_call(shlex.split(cmd % (parcel_archive, os.path.basename(self._parcel_dir))), cwd=self._out_dir)

    @timer_decorate
    def _copy_if_necessary(self):
        """ If no particular distro was specified, this makes copies of the parcels for each of the supported OSs"""
        if not self._distro:
            self._parcel_copy(SUPPORTED_OSES)
        else:
            self._parcel_copy(self._distro)

    @timer_decorate
    def _parcel_copy(self,distributions):
        src_archive = os.path.join(self._out_dir, "SPARK2-" + self._version.full_version() + ".parcel")
        for dist in distributions[1::]:
            dest_version = "%s-%s" % (self._version.version(), osinfo.parse_name(dist).parcel_label())
            dest_archive = os.path.join(self._out_dir, "SPARK2-" + dest_version + ".parcel")
            # TODO: Is this the right copy function to use? There are wayyyyyy to many to chose from.
            shutil.copy(src_archive, dest_archive)

    @timer_decorate
    def _generate_sha1_files(self):
        """ Generate sha1 files for parcels, placed right beside them."""
        # At this point, all the parcels have been generated. So we simply iterate through the parcels
        # generate corresponding sha1sum files and place the .sha1 files right beside the parcels.
        files = os.listdir(self._out_dir)
        for f in files:
            if not f.endswith('.parcel'):
                continue

            LOG.info("Generating sha1sum file for %s", f)
            parcel_path = os.path.join(self._out_dir, f)
            with open(parcel_path, 'rb') as fp:
                with open(parcel_path + ".sha1", 'w') as sha1file:
                    print(hashlib.sha1(fp.read()).hexdigest(), file=sha1file)

    @timer_decorate
    def _cleanup(self):
        # Cleanup
        util.rmtree(self._build_dir)
        if not self._skip_archive:
            util.rmtree(self._parcel_dir)

    @timer_decorate
    def archive(self):
        self._stitch_parcel()
        if not self._skip_archive:
            self._archive_parcel()
            self._copy_if_necessary()
            self._generate_sha1_files()
        self._cleanup()

def parse_args():
    parser = argparse.ArgumentParser(
        description="Builds parcels.")

    # Global argument
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help="Whether to print verbose output for debugging.")
    # Input and output dirs
    parser.add_argument('--input-directory',
                        dest="input_directory",
                        required=True,
                        help="Directory containing input files.")
    parser.add_argument('--output-directory',
                        dest="output_directory",
                        required=True,
                        help="Where to store output and intermediate files." +
                        " Will be created if it does not exist.")
    parser.add_argument('--force-clean',
                        action='store_true',
                        dest='force_clean',
                        help="Whether to delete contents of the output directory beforehand.")
    parser.add_argument('--extract-packages',
                        action='store_true',
                        help="If specified, parcel will be built by extracting packages present in the input directory."+\
                        " Otherwise, bits are assumed to have already been extracted under input_directory. Currently," +
                        " package extraction is not supported. So, please only specify false.")

    # Version arguments
    Version.add_arguments(parser)

    # Archiver arguments
    Archiver.add_arguments(parser)

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(sys.argv[1:])
    return args

@timer_decorate
def main():
    global LOG
    LOG = logging.getLogger(__name__)
    util.configure_logger(LOG)

    global SUPPORTED_OSES
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, 'supported_oses.txt'), 'r') as f:
      for line in f:
        result = line.rstrip('\n')
        SUPPORTED_OSES.append(result)


    args = parse_args()

    if args.verbose:
        util.configure_debug_logger(LOG)

    version = Version.build_from_args(args)

    in_dir = os.path.abspath(os.path.expandvars(args.input_directory))
    out_dir = os.path.abspath(os.path.expandvars(args.output_directory))

    if not os.path.exists(in_dir):
        LOG.error("Input path %s does not exist", in_dir)
        sys.exit(1)

    if not os.path.isdir(in_dir):
        LOG.error("Input path %s is not a directory", in_dir)
        sys.exit(1)

    if args.force_clean:
        LOG.info("--force-clean specified, deleting output directory (if it exists) %s", out_dir)
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)

    if not os.path.isdir(out_dir):
        if not os.path.exists(out_dir):
            LOG.info("Creating output directory %s", out_dir)
            util.mkdirs_recursive(out_dir)
        else:
            LOG.error("Output path %s is not a directory", out_dir)
            sys.exit(1)

    if len(os.listdir(out_dir)) > 0:
        LOG.error("Output directory %s is not empty! Invoke with --force-clean to wipe output directory.", out_dir)
        sys.exit(1)

    LOG.info("Using input directory %s", in_dir)
    LOG.info("Using output directory %s", out_dir)

    build_dir = os.path.join(out_dir, "build")
    e = Extractor(in_dir, build_dir)
    e.extract()
    m = Metadata(build_dir, version)
    m.write_metadata()
    a = Archiver(out_dir, build_dir, version, args.skip_archive, args.distro)
    a.archive()

if __name__ == "__main__":
    main()
    log_timer_traces()
