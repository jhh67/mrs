#!/usr/bin/env python2.7

import sys
import os
import subprocess
import shutil
import glob

if sys.version_info[0:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

class Options(object):
    pass

class MrsTest(unittest.TestCase):

    def setUp(self):
        try:
            shutil.rmtree('output')
        except:
            pass

    def tearDown(self):
        global tmpdir
        try:
            #shutil.rmtree('output')
            shutil.rmtree(tmpdir)
        except:
            pass

    def mr(self, inputs, mappers, reducers, expected, output = None, force = False, jobs = None,
            splits = None, partitions = None, merger = None, args = None):
        global options
        global tmpdir
        if args is None:
            args = []
        cmd = ['../mrs', '-d']
        for input in inputs:
            cmd += ['-i', input]
        for mapper in mappers:
            cmd += ['-m', mapper]
        for reducer in reducers:
            cmd += ['-r', reducer]
        if jobs is not None:
            cmd += ['-j', str(jobs)]
        if splits is not None:
            cmd += ['-s', str(splits)]
        if partitions is not None:
            cmd += ['-p', str(partitions)]
        if merger is not None:
            cmd += ['-M', merger]
        if output is not None:
            cmd += ['-o', output]
        if force is not False:
            cmd += ['-f']
        cmd += args
        if options.verbosity > 2:
            print ' '.join(cmd)
        try:
            result = subprocess.check_output(cmd)
        except subprocess.CalledProcessError, e:
            print "MRS failed with exit code: %s" % e.returncode
            print "Output: %s" % e.output
            raise e
        for line in result.split('\n'):
            if line.startswith('tmpdir:'):
                tmpdir = line.split()[1]

        if output is None:
            output = "output"
        status = subprocess.call(['diff', expected, output])
        self.assertEqual(status, 0)

        if splits is None:
            splits = 4

        if partitions is None:
            partitions = 4

        if jobs is None:
            jobs = 1

        for step in xrange(0, len(inputs)):

            files = glob.glob(tmpdir + "/step.%d/split.*" % step)
            self.assertEqual(len(files), splits)

            files = glob.glob(tmpdir + "/step.%d/tmp.*" % step)
            self.assertEqual(len(files), splits)

            files = glob.glob(tmpdir + "/step.%d/part.*" % step)
            self.assertEqual(len(files), partitions * splits)


    @unittest.skip('XXX')
    def test_001_default(self):
        # run with defaults
        self.mr(['input00'], ['./wordSplitter.py'], ['aggregate'], 'output00')

    @unittest.skip('XXX')
    def test_002_merger(self):
        # reducer outputs merged
        self.mr(['input00'], ['./wordSplitter.py'], ['aggregate'], 'output01', merger='sort')

    @unittest.skip('XXX')
    def test_003_single_reducer(self):
        # single reducer
        self.mr(['input00'], ['./wordSplitter.py'], ['aggregate'], 'output01', merger='sort', partitions=1)

    @unittest.skip('XXX')
    def test_004_specify_splits(self):
        # 3 splits
        self.mr(['input00'], ['./wordSplitter.py'], ['aggregate'], 'output00', splits=3)

    @unittest.skip('XXX')
    def test_005_multiple_jobs(self):
        # 5 jobs
        self.mr(['input00'], ['./wordSplitter.py'], ['aggregate'], 'output00', jobs=5)

    def test_005_multiple_steps(self):
        # 5 jobs
        self.mr(['input00'], ['./wordSplitter.py', './topten.py'], ['aggregate', './topten.py'], 'output02',merger='./topten.py')


def main():
    global options
    options = Options()
    options.verbosity = 3
    options.failfast=True

    tests = sys.argv

    unittest.main(argv=tests, verbosity=options.verbosity, failfast=options.failfast)

if __name__ == '__main__': 
    if "-n" in sys.argv:
        dry = True
    main()
