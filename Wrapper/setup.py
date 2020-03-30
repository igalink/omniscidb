from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize
import os
import sys
from codecs import open
from os import path
from shutil import copy
from sys import platform
from setuptools import Extension, setup
import pyarrow
import numpy

ext_modules = [Extension("dbe",
                     ["dbe.pyx"],
                     language='c++',
                     extra_compile_args=["-std=c++17", "-fPIC", "-Wno-strict-prototypes", "-pie", "-lc"],
#                     extra_link_args=['/usr/local/mapd-deps/lib/libarrow.so.13.0.0', '/usr/local/mapd-deps/lib/libarrow_python.so.13.0.0'],
                     include_dirs=['../', './', '/usr/local/mapd-deps/include'],
                     library_dirs=['./', '../build/Wrapper', '/usr/local/mapd-deps/lib', '/lib/x86_64-linux-gnu', '/usr/lib/x86_64-linux-gnu'],
                     libraries=['DBEngine'],
                     )]

class DBEBuildExt(build_ext):

    # list of libraries that will be bundled with python connector,
    # this list should be carefully examined when pyarrow lib is
    # upgraded
    arrow_libs_to_copy = {
        'linux': ['libarrow.so.13',
                  'libarrow_python.so.13']
#                  'libarrow_flight.so.13',
#                  'libarrow_boost_filesystem.so.1.68.0',
#                  'libarrow_boost_system.so.1.68.0',
#                  'libarrow_boost_regex.so.1.68.0'],
#        'darwin': ['libarrow.16.dylib',
#                   'libarrow_python.16.dylib',
#                   'libarrow_boost_filesystem.dylib',
#                   'libarrow_boost_regex.dylib',
#                   'libarrow_boost_system.dylib'],
#        'win32': ['arrow.dll',
#                  'arrow_python.dll',
#                  'zlib.dll']
    }

    arrow_libs_to_link = {
        'linux': ['libarrow.so.13',
                  'libarrow_python.so.13'],
        'darwin': ['libarrow.16.dylib',
                   'libarrow_python.16.dylib'],
        'win32': ['arrow.lib',
                  'arrow_python.lib']
    }

    def build_extension(self, ext):
        current_dir = os.getcwd()

        ext.extra_compile_args.append('-isystem' + pyarrow.get_include())
        ext.extra_compile_args.append('-isystem' + numpy.get_include())
#        ext.extra_compile_args.append('-std=c++11')
#        ext.extra_compile_args.append('-D_GLIBCXX_USE_CXX11_ABI=0')

        ext.library_dirs.append(os.path.join(current_dir, self.build_lib))
        ext.extra_link_args += self._get_arrow_lib_as_linker_input()
        ext.extra_link_args += ['-Wl,-rpath,$ORIGIN']

        build_ext.build_extension(self, ext)

    def _get_arrow_lib_dir(self):
        return pyarrow.get_library_dirs()[0]

    def _get_arrow_lib_as_linker_input(self):
        link_lib = self.arrow_libs_to_link[sys.platform]
        ret = []
        for lib in link_lib:
            source = '{}/{}'.format(self._get_arrow_lib_dir(), lib)
#            assert path.exists(source)
            ret.append(source)
            print(source)
        return ret

cmd_class = {
    "build_ext": DBEBuildExt
}

setup(
  name = 'dbe',
#  cmdclass = {'build_ext': build_ext},
  cmdclass = cmd_class,
  ext_modules = cythonize(ext_modules, compiler_directives={"c_string_type": "str", "c_string_encoding": "utf8", "language_level": "3"})
)