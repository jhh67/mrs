MRS is a MapReduce simulator implemented as a tcsh script. It supports a
streaming interface to the mappers and reducers similar to Hadoop and runs all
tasks locally. It supports running multiple tasks concurrently and MapReduce
jobs that consist of multiple steps. In debug mode it leaves all intermediate
files intact to help with debugging your MapReduce job. MRS makes use of gawk
and python 2.7 so you'll need those installed. Copy the 'mrs' executable to
somewhere in your path, and put mrs.1 somewhere in your manpath.

See the man page for details on how to use it.

-- John H. Hartman
