cmake_minimum_required(VERSION 3.10)

set(VERSION 0.7.0)
project(raft VERSION ${VERSION})

option(FIXTURE_ENABLED "enable test fixture" On)
option(UV_ENABLED "enable libuv" On)
option(EXAMPLE_ENABLED "enable example server" On)
option(BENCHMARK_ENABLED "enable benchmark test" On)
option(DEBUG_ENABLED "enable debug" On)
option(SANITIZE_ENABLED "enable address sanitizer" On)
option(CODE_COVERAGE_ENABLED "enable code coverage" On)
option(RAFTSTATIC "settofalse.doesntwork" Off)

#todo: limit the msvc options to only windows
# also check warn opts at https://docs.microsoft.com/en-us/cpp/c-runtime-library/compatibility?view=msvc-160
set(AM_CFLAGS ${AM_CFLAGS} ${CODE_COVERAGE_FLAGS})
set(AM_CFLAGS ${AM_CFLAGS} /D_CRT_DECLARE_NONSTDC_NAMES=1)

#todo: these aren't used in cmake build yet
set (include_HEADERS include/raft.h)
set (raftincludedir) # ${includedir)/raft)
set (raftinclude_HEADERS)

#todo harmonise withe makefile
set(RAFT_LIBRARIES)

set(libraft_la_CFLAGS, ${AM_CFLAGS} -fvisibility=hidden)
set(VERSION 0.7.0)
set(SOVERSION 0.7) # ??

set(libraft_la_SOURCES
    src/byte.c 
    src/client.c    
    src/configuration.c    
    src/convert.c    
    src/election.c    
    src/entry.c
    src/err.c    
    src/fixture.c    
    src/heap.c    
    src/log.c    
    src/membership.c    
    src/progress.c
    src/raft.c
    src/recv_append_entries.c    
    src/recv_append_entries_result.c    
    src/recv.c    
    src/recv_install_snapshot.c
    src/recv_request_vote.c    
    src/recv_request_vote_result.c    
    src/recv_timeout_now.c
    src/replication.c    
    src/snapshot.c    
    src/start.c    
    src/state.c    
    src/syscall.c
    src/tick.c    
    src/tracing.c
    )

# todo: not sure why we need to set these for raft in cmake
set(libraft_la_HEADERS
    src/array.h
    src/assert.h
    src/byte.h
    src/configuration.h
    src/convert.h
    src/election.h
    src/entry.h
    src/err.h
    src/heap.h
    src/log.h
    src/membership.h
    src/progress.h
    src/queue.h
    src/recv_append_entries.h
    src/recv_append_entries_result.h
    src/recv.h
    src/recv_install_snapshot.h
    src/recv_request_vote.h
    src/recv_request_vote_result.h
    src/recv_timeout_now.h
    src/replication.h
    src/request.h
    src/snapshot.h
    src/syscall.h
    src/tick.h
    src/tracing.h
    )
    
#set(bin_PROGRAMS)

#todo: this is set and added to in cmake but not sure what to do with it
set(check_PROGRAMS 
  test_unit_core
)

#set(TESTS ${check_PROGRAMS})
#check_LTLIBRARIES = libtest.la

#todo: add msvc/clang translation of -Wno-unused-result -Wno-conversion
set(libtest_la_CFLAGS ${AM_CFLAGS} -DMUNIT_TEST_NAME_LEN=60 -D_CRT_SECURE_NO_WARNINGS=1 -D_WINSOCK_DEPRECATED_NO_WARNINGS=1)
set(libtest_la_SOURCES
    test/lib/fault.c 
    test/lib/fsm.c 
    test/lib/heap.c 
    test/lib/munit.c
    test/lib/tracer.c     
    test/lib/tcp.c
    )

set(test_unit_core_SOURCES 
  src/byte.c 
  src/configuration.c 
  src/err.c 
  src/heap.c 
  src/log.c 
  test/unit/main_core.c 
  test/unit/test_byte.c 
  test/unit/test_configuration.c 
  test/unit/test_err.c 
  test/unit/test_log.c 
  test/unit/test_queue.c)
#todo: add msvc/clang translation of -Wno-conversion
set(test_unit_core_CFLAGS ${AM_CFLAGS})
set(test_unit_core_LDADD libtest)

if (FIXTURE_ENABLED)
    set(libraft_la_SOURCES ${libraft_la_SOURCES} src/fixture.c)

    set(raftinclude_HEADERS ${raftinclude_HEADERS} include/raft/fixture.h)

    set(check_PROGRAMS ${check_PROGRAMS}
        test/integration/core 
        test/fuzzy/core
        )

    set(libtest_la_SOURCES ${libtest_la_SOURCES} test/lib/cluster.c)

    set(test_integration_core_SOURCES
        test/integration/main_core.c 
        test/integration/test_apply.c 
        test/integration/test_assign.c 
        test/integration/test_barrier.c 
        test/integration/test_bootstrap.c 
        test/integration/test_digest.c 
        test/integration/test_election.c 
        test/integration/test_fixture.c 
        test/integration/test_heap.c 
        test/integration/test_membership.c 
        test/integration/test_recover.c 
        test/integration/test_replication.c 
        test/integration/test_snapshot.c 
        test/integration/test_strerror.c 
        test/integration/test_tick.c 
        test/integration/test_transfer.c 
        test/integration/test_start.c
        )
    set(test_integration_core_CFLAGS ${AM_CFLAGS} "-Wno-conversion")
    # todo: don't think install applies to cmake??
    #set(test_integration_core_LDFLAGS "-no-install")
    set(test_integration_core_LDADD libtest libraft)

    set(test_fuzzy_core_SOURCES
        test/fuzzy/main_core.c 
        test/fuzzy/test_election.c 
        test/fuzzy/test_liveness.c 
        test/fuzzy/test_membership.c 
        test/fuzzy/test_replication.c
        )

    set(test_fuzzy_core_CFLAGS ${AM_CFLAGS} "-Wno-conversion")
    # todo: don't think install applies to cmake??
    #set(test_fuzzy_core_LDFLAGS "-no-install")
    set(test_fuzzy_core_LDADD libtest libraft)

endif()

#todo: this works for vcpkg with Visual stdio (with msvc or clang)
set(have_uv false)
message("looking for libuv")
message("target triplet ${VCPKG_TARGET_TRIPLET}")
message("toolchain  ${CMAKE_TOOLCHAIN_FILE}")
message("^^ make sure these are set")
find_package(unofficial-libuv QUIET)
if(${unofficial-libuv_FOUND})
    message("unofficial-libuv was found")
    if (UV_ENABLED)    
        set(have_uv true)
        set(libraft_la_SOURCES
            ${libraft_la_SOURCES}
            src/uv_append.c
            src/uv.c
            src/uv_encoding.c
            src/uv_finalize.c
            src/uv_fs.c
            src/uv_ip.c
            src/uv_list.c
            src/uv_metadata.c
            src/uv_os.c
            src/uv_prepare.c
            src/uv_recv.c
            src/uv_segment.c
            src/uv_send.c
            src/uv_snapshot.c
            src/uv_tcp.c
            src/uv_tcp_connect.c
            src/uv_tcp_listen.c
            src/uv_truncate.c
            src/uv_writer.c
            )
        set(libraft_la_HEADERS
            ${libraft_la_HEADERS}
            src/uv_encoding.h
            src/uv_fs.h
            src/uv.h
            src/uv_ip.h
            src/uv_os.h
            src/uv_tcp.h
            src/uv_writer.h
            )
        #todo: this part probably needs reworked for other environments/ package managers
        message("trying complicated stuff")
        include(FindPackageHandleStandardArgs)
        find_package_handle_standard_args(unofficial-libuv  DEFAULT_MSG
                                          unofficial-libuv_LIBRARY unofficial-libuv_INCLUDE_DIR)
        #set variables LIBUV_INCLUDE_DIR and LIBUV_LIBRARY 
        FIND_PATH(LIBUV_INCLUDE_DIR NAMES uv.h HINTS ${LIBUV_INCLUDE_DIR} ${LIBUV_DIR})
        FIND_LIBRARY(LIBUV_LIBRARY NAMES uv libuv HINTS ${LIBUV_LIBRARY_DIR} ${LIBUV_DIR})
        message("libuv include dir ${LIBUV_INCLUDE_DIR}, library ${LIBUV_LIBRARY}")
        include_directories(${LIBUV_INCLUDE_DIR})

        set(UV_LIBS ${LIBUV_LIBRARY})

        set(libraft_la_LDFLAGS ${libraft_la_LDFLAGS} ${UV_LIBS})

        set(raftinclude_HEADERS ${raftinclude_HEADERS} include/raft/uv.h)

        set(check_PROGRAMS ${check_PROGRAMS}
            test_unit_uv 
            test_integration_uv
            )

        set(libtest_la_SOURCES ${libtest_la_SOURCES}
            test/lib/aio.c 
            test/lib/dir.c
            test/lib/tcp.c 
            test/lib/loop.c
            )

        set(test_unit_uv_SOURCES  
            src/err.c 
            src/heap.c 
            src/syscall.c 
            src/tracing.c 
            src/uv_fs.c 
            src/uv_os.c 
            src/uv_writer.c 
            test/unit/main_uv.c 
            test/unit/test_uv_fs.c 
            test/unit/test_uv_writer.c
            )
        #todo: unused, added via LDADD for cmake
        #set(test_unit_uv_LDFLAGS ${UV_LIBS})
        set(test_unit_uv_CFLAGS ${AM_CFLAGS} -Wno-conversion)
        set(test_unit_uv_LDADD ${UV_LIBS} libtest)

        set(test_integration_uv_SOURCES
            test/integration/main_uv.c 
            test/integration/test_uv_init.c 
            test/integration/test_uv_append.c 
            test/integration/test_uv_bootstrap.c 
            test/integration/test_uv_load.c 
            test/integration/test_uv_recover.c 
            test/integration/test_uv_recv.c 
            test/integration/test_uv_send.c 
            test/integration/test_uv_set_term.c 
            test/integration/test_uv_tcp_connect.c 
            test/integration/test_uv_tcp_listen.c 
            test/integration/test_uv_snapshot_put.c 
            test/integration/test_uv_truncate.c
            )
        set(test_integration_uv_CFLAGS ${AM_CFLAGS} -Wno-type-limits -Wno-conversion)
        #todo: unused, added via LDADD for cmake
        #set(test_integration_uv_LDFLAGS -no-install ${UV_LIBS})
        #todo: winsock lib only for win32?
        set(test_integration_uv_LDADD ${UV_LIBS} libtest libraft ws2_32)
    
        #todo: is this right
        set(AM_CFLAGS ${AM_CFLAGS} ${UV_CFLAGS})
    endif()
endif()

## target configuration

if (RAFTSTATIC)
  add_library(libraft STATIC ${libraft_la_SOURCES} ${libraft_la_HEADERS})
else()
  add_library(libraft SHARED ${libraft_la_SOURCES} ${libraft_la_HEADERS})
endif()

set_property(TARGET libraft PROPERTY VERSION ${VERSION})
set_property(TARGET libraft PROPERTY SOVERSION ${SOVERSION})

if (RAFTSTATIC)
  #clang dont like it
  #target_compile_options(raft PRIVATE RAFT_STATIC)
elseif()
  target_compile_options(libraft PRIVATE RAFT_SHARED_BUILD)
endif()
target_compile_options(libraft PRIVATE ${libraft_la_CFLAGS} )
target_link_libraries(libraft ${libraft_la_LDFLAGS})
#target_link_libraries(libraft unofficial-libuv::libuv)

#EXAMPLES

if (EXAMPLE_ENABLED)
    if (UV_ENABLED)
        #todo: dll build works, static build doesn't link
        add_executable(example_server example/server.c)
        #target_compile_options(example_server PRIVATE RAFT_STATIC)
        target_compile_options(example_server PRIVATE ${AM_CFLAGS} )
        target_link_libraries(example_server LINK_PUBLIC libraft)
        target_link_libraries(example_server LINK_PUBLIC ${UV_LIBS})   
    endif()

    #add_executable(example_cluster example/cluster.c)
    #target_compile_options(example_cluster PRIVATE RAFT_STATIC)
    #target_compile_options(example_cluster PRIVATE ${AM_CFLAGS} )
    #target_link_libraries(example_cluster LINK_PUBLIC libraft )
endif()

# tests
add_library(libtest STATIC ${libtest_la_SOURCES})
target_compile_options(libtest PRIVATE ${libtest_la_CFLAGS} )

function(build_test_exe testname)
  add_executable(${testname} ${${testname}_SOURCES})
  target_compile_options(${testname} PRIVATE ${${testname}_CFLAGS} )
  target_link_libraries(${testname} LINK_PUBLIC ${${testname}_LDADD})
endfunction()

build_test_exe(test_unit_core)

if (FIXTURE_ENABLED)
    build_test_exe(test_integration_core)
    build_test_exe(test_fuzzy_core)
endif()

if (have_uv)
    build_test_exe(test_unit_uv)
    build_test_exe(test_integration_uv)
endif()