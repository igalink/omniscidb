from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

# "-DBOOST_VARIANT_USE_RELAXED_GET_BY_DEFAULT"

ext_modules = [Extension("dbe",
                         ["dbe.pyx"],
                         language='c++',
                         extra_compile_args=[
                             "-std=c++17", "-fPIC", "-pie", "-lc"],
                         #                     extra_link_args=['/usr/local/mapd-deps/lib/libarrow.so.13.0.0', '/usr/local/mapd-deps/lib/libarrow_python.so.13.0.0'],
                         include_dirs=['../', './',
                                       '/usr/local/mapd-deps/include'],
                         library_dirs=['./', '../build/Wrapper', '/usr/local/mapd-deps/lib',
                                       '/lib/x86_64-linux-gnu', '/usr/lib/x86_64-linux-gnu'],
                         libraries=['DBEngine'],
                         )]

setup(
    name='dbe',
    cmdclass={'build_ext': build_ext},
    ext_modules=cythonize(ext_modules, compiler_directives={
                          "c_string_type": "str", "c_string_encoding": "utf8", "language_level": "3"})
)
