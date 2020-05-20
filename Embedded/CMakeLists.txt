add_library(DBEngine SHARED DBEngine.cpp)

# TBB
set(TBB_LIBS "")
find_package(TBB)
if(TBB_FOUND)
  message(STATUS "Building with TBB support")
  add_definitions("-DHAVE_TBB")
  list(APPEND TBB_LIBS ${TBB_LIBRARIES})
endif()

target_link_libraries(DBEngine tinfo ${TBB_LIBS} ${LLVM_LINKER_FLAGS} ${Arrow_LIBRARIES} QueryRunner DataMgr ${TBB_LIBS})

find_program(PYTHON "python3")
#make these dependent on all klampt module source files, Klampt, and Klampt's dependency libraries
#FILE(GLOB DEPS klampt/src/*.h klampt/src/*.cpp klampt/*.py)
#SET(DEPS ${DEPS} ${KLAMPT_LIBRARIES} Klampt)
MESSAGE(STATUS "PYTHON=@PYTHON")
IF(PYTHON)
MESSAGE(STATUS "add_custom_command")
  add_custom_command(
    OUTPUT ${CMAKE_SOURCE_DIR}/Embedded/python_timestamp
#    COMMAND rm -rf build
    COMMAND ${PYTHON} setup.py build_ext
#    COMMAND ${CMAKE_COMMAND} -E touch ${TEMPOUTPUT}/python_timestamp
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/Embedded
#    DEPENDS ${DEPS}
  )
#  add_custom_target(python3 ALL DEPENDS ${OUTPUT}/python3_timestamp)
ENDIF(PYTHON)

#add_custom_command(
#        OUTPUT
#            ${CMAKE_CURRENT_BINARY_DIR}/__header.h
#            ${CMAKE_CURRENT_BINARY_DIR}/header.h
#        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/OriginalHeader.h
#        COMMAND python setup.py build_ext --inplace
#)

