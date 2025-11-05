# - Find IcfsFS
# Find the Linux Trace Toolkit - next generation with associated includes path.
# See http://icfs.org/
#
# This module accepts the following optional variables:
#    ICFS_PREFIX   = A hint on ICFSFS install path.
#
# This module defines the following variables:
#    ICFSFS_FOUND       = Was IcfsFS found or not?
#    ICFSFS_LIBRARIES   = The list of libraries to link to when using IcfsFS
#    ICFSFS_INCLUDE_DIR = The path to IcfsFS include directory
#
# On can set ICFS_PREFIX before using find_package(IcfsFS) and the
# module with use the PATH as a hint to find IcfsFS.
#
# The hint can be given on the command line too:
#   cmake -DICFS_PREFIX=/DATA/ERIC/IcfsFS /path/to/source

if(ICFS_PREFIX)
  message(STATUS "FindIcfsFS: using PATH HINT: ${ICFS_PREFIX}")
  # Try to make the prefix override the normal paths
  find_path(ICFSFS_INCLUDE_DIR
    NAMES icfsfs/libicfsfs.h
    PATHS ${ICFS_PREFIX}
    PATH_SUFFIXES include
    NO_DEFAULT_PATH
    DOC "The IcfsFS include headers")

  find_path(ICFSFS_LIBRARY_DIR
    NAMES libicfsfs.so
    PATHS ${ICFS_PREFIX}
    PATH_SUFFIXES lib/${CMAKE_LIBRARY_ARCHITECTURE} lib lib64
    NO_DEFAULT_PATH
    DOC "The IcfsFS libraries")
endif(ICFS_PREFIX)

if (NOT ICFSFS_INCLUDE_DIR)
  find_path(ICFSFS_INCLUDE_DIR
    NAMES icfsfs/libicfsfs.h
    PATHS ${ICFS_PREFIX}
    PATH_SUFFIXES include
    DOC "The IcfsFS include headers")
endif (NOT ICFSFS_INCLUDE_DIR)

if (NOT ICFSFS_LIBRARY_DIR)
  find_path(ICFSFS_LIBRARY_DIR
    NAMES libicfsfs.so
    PATHS ${ICFS_PREFIX}
    PATH_SUFFIXES lib/${CMAKE_LIBRARY_ARCHITECTURE} lib lib64
    DOC "The IcfsFS libraries")
endif (NOT ICFSFS_LIBRARY_DIR)

find_library(ICFSFS_LIBRARY icfsfs PATHS ${ICFSFS_LIBRARY_DIR} NO_DEFAULT_PATH)
check_library_exists(icfsfs icfs_ll_lookup ${ICFSFS_LIBRARY_DIR} ICFS_FS)
if (NOT ICFS_FS)
  unset(ICFSFS_LIBRARY_DIR CACHE)
  unset(ICFSFS_INCLUDE_DIR CACHE)
else (NOT ICFS_FS)
  check_library_exists(icfsfs icfs_ll_mknod ${ICFSFS_LIBRARY_DIR} ICFS_FS_MKNOD)
  if(NOT ICFS_FS_MKNOD)
    message("Cannot find icfs_ll_mknod.  Disabling ICFS fsal mknod method")
    set(USE_FSAL_ICFS_MKNOD OFF)
  else(ICFS_FS_MKNOD)
    set(USE_FSAL_ICFS_MKNOD ON)
  endif(NOT ICFS_FS_MKNOD)
  check_library_exists(icfsfs icfs_ll_setlk ${ICFSFS_LIBRARY_DIR} ICFS_FS_SETLK)
  if(NOT ICFS_FS_SETLK)
    message("Cannot find icfs_ll_setlk.  Disabling ICFS fsal lock2 method")
    set(USE_FSAL_ICFS_SETLK OFF)
  else(ICFS_FS_SETLK)
    set(USE_FSAL_ICFS_SETLK ON)
  endif(NOT ICFS_FS_SETLK)
  check_library_exists(icfsfs icfs_ll_lookup_root ${ICFSFS_LIBRARY_DIR} ICFS_FS_LOOKUP_ROOT)
  if(NOT ICFS_FS_LOOKUP_ROOT)
    message("Cannot find icfs_ll_lookup_root. Working around it...")
    set(USE_FSAL_ICFS_LL_LOOKUP_ROOT OFF)
  else(NOT ICFS_FS_LOOKUP_ROOT)
    set(USE_FSAL_ICFS_LL_LOOKUP_ROOT ON)
  endif(NOT ICFS_FS_LOOKUP_ROOT)

  check_library_exists(icfsfs icfs_ll_delegation ${ICFSFS_LIBRARY_DIR} ICFS_FS_DELEGATION)
  if(NOT ICFS_FS_DELEGATION)
    message("Cannot find icfs_ll_delegation. Disabling support for delegations.")
    set(USE_FSAL_ICFS_LL_DELEGATION OFF)
  else(NOT ICFS_FS_DELEGATION)
    set(USE_FSAL_ICFS_LL_DELEGATION ON)
  endif(NOT ICFS_FS_DELEGATION)

  check_library_exists(icfsfs icfs_ll_sync_inode ${ICFSFS_LIBRARY_DIR} ICFS_FS_SYNC_INODE)
  if(NOT ICFS_FS_SYNC_INODE)
    message("Cannot find icfs_ll_sync_inode. SETATTR requests may be cached!")
    set(USE_FSAL_ICFS_LL_SYNC_INODE OFF)
  else(NOT ICFS_FS_SYNC_INODE)
    set(USE_FSAL_ICFS_LL_SYNC_INODE ON)
  endif(NOT ICFS_FS_SYNC_INODE)

  check_library_exists(icfsfs icfs_ll_fallocate ${ICFSFS_LIBRARY_DIR} ICFS_FALLOCATE)
  if(NOT ICFS_FALLOCATE)
    message("Cannot find icfs_ll_fallocate. No ALLOCATE or DEALLOCATE support!")
    set(USE_ICFS_FALLOCATE OFF)
  else(NOT ICFS_FALLOCATE)
    set(USE_ICFS_LL_FALLOCATE ON)
  endif(NOT ICFS_FALLOCATE)

  check_library_exists(icfsfs icfs_abort_conn ${ICFSFS_LIBRARY_DIR} ICFS_FS_ABORT_CONN)
  if(NOT ICFS_FS_ABORT_CONN)
	  message("Cannot find icfs_abort_conn. FSAL_ICFS will not leave session intact on clean shutdown.")
	  set(USE_FSAL_ICFS_ABORT_CONN OFF)
  else(NOT ICFS_FS_ABORT_CONN)
	  set(USE_FSAL_ICFS_ABORT_CONN ON)
  endif(NOT ICFS_FS_ABORT_CONN)

  check_library_exists(icfsfs icfs_start_reclaim ${ICFSFS_LIBRARY_DIR} ICFS_FS_RECLAIM_RESET)
  if(NOT ICFS_FS_RECLAIM_RESET)
	  message("Cannot find icfs_start_reclaim. FSAL_ICFS will not kill off old sessions.")
	  set(USE_FSAL_ICFS_RECLAIM_RESET OFF)
  else(NOT ICFS_FS_RECLAIM_RESET)
	  set(USE_FSAL_ICFS_RECLAIM_RESET ON)
  endif(NOT ICFS_FS_RECLAIM_RESET)

  check_library_exists(icfsfs icfs_select_filesystem ${ICFSFS_LIBRARY_DIR} ICFS_FS_GET_FS_CID)
  if(NOT ICFS_FS_GET_FS_CID)
	  message("Cannot find icfs_set_filesystem. FSAL_ICFS will only mount the default filesystem.")
	  set(USE_FSAL_ICFS_GET_FS_CID OFF)
  else(NOT ICFS_FS_GET_FS_CID)
	  set(USE_FSAL_ICFS_GET_FS_CID ON)
  endif(NOT ICFS_FS_GET_FS_CID)

  check_library_exists(icfsfs icfs_ll_register_callbacks ${ICFSFS_LIBRARY_DIR} ICFS_FS_REGISTER_CALLBACKS)
  if(NOT ICFS_FS_REGISTER_CALLBACKS)
	  message("Cannot find icfs_ll_register_callbacks. FSAL_ICFS will not respond to cache pressure requests from the MDS.")
	  set(USE_FSAL_ICFS_REGISTER_CALLBACKS OFF)
  else(NOT ICFS_FS_REGISTER_CALLBACKS)
	  set(USE_FSAL_ICFS_REGISTER_CALLBACKS ON)
  endif(NOT ICFS_FS_REGISTER_CALLBACKS)

  check_library_exists(icfsfs icfs_ll_lookup_vino ${ICFSFS_LIBRARY_DIR} ICFS_FS_LOOKUP_VINO)
  if(NOT ICFS_FS_LOOKUP_VINO)
	  message("Cannot find icfs_ll_lookup_vino. FSAL_ICFS will not be able to reliably look up snap inodes by handle.")
	  set(USE_FSAL_ICFS_LOOKUP_VINO OFF)
  else(NOT ICFS_FS_LOOKUP_VINO)
	  set(USE_FSAL_ICFS_LOOKUP_VINO ON)
  endif(NOT ICFS_FS_LOOKUP_VINO)

  set(CMAKE_REQUIRED_INCLUDES ${ICFSFS_INCLUDE_DIR})
  if (CMAKE_MAJOR_VERSION VERSION_EQUAL 3 AND CMAKE_MINOR_VERSION VERSION_GREATER 14)
    include(CheckSymbolExists)
  endif(CMAKE_MAJOR_VERSION VERSION_EQUAL 3 AND CMAKE_MINOR_VERSION VERSION_GREATER 14)
  check_symbol_exists(ICFS_STATX_INO "icfsfs/libicfsfs.h" ICFS_FS_ICFS_STATX)
  if(NOT ICFS_FS_ICFS_STATX)
    message("Cannot find ICFS_STATX_INO. Enabling backward compatibility for pre-icfs_statx APIs.")
    set(USE_FSAL_ICFS_STATX OFF)
  else(NOT ICFS_FS_ICFS_STATX)
    set(USE_FSAL_ICFS_STATX ON)
  endif(NOT ICFS_FS_ICFS_STATX)
  check_prototype_definition(icfs_ll_writev
    #"int64_t icfs_ll_writev(struct icfs_mount_info *cmount, struct Fh *fh,
    #                   const struct iovec *iov, int iovcnt, int64_t off);"
    "int64_t icfs_ll_writev(struct icfs_mount_info *cmount, struct Fh *fh,
                       struct iovec_t *iov, int64_t off);"
    "NULL"
    "unistd.h;icfsfs/libicfsfs.h"
    ICFS_FS_ICFS_ZEROCPY_WRITE)
  if(NOT ICFS_FS_ICFS_ZEROCPY_WRITE)
    message("Disable ICFS write zero copy APIs.")
    set(USE_FSAL_ICFS_ZEROCPY_WRITE OFF)
  else(NOT ICFS_FS_ICFS_ZEROCPY_WRITE)
    set(USE_FSAL_ICFS_ZEROCPY_WRITE ON)
  endif(NOT ICFS_FS_ICFS_ZEROCPY_WRITE)
  check_prototype_definition(icfs_ll_readv
    "int64_t icfs_ll_readv(struct icfs_mount_info *cmount, struct Fh *fh,
                      struct iovec_t *iov, int64_t off);"
    "NULL"
    "unistd.h;icfsfs/libicfsfs.h"
    ICFS_FS_ICFS_ZEROCPY_READ)
  if(NOT ICFS_FS_ICFS_ZEROCPY_READ)
    message("Disable ICFS read zero copy APIs.")
    set(USE_FSAL_ICFS_ZEROCPY_READ OFF)
  else(NOT ICFS_FS_ICFS_ZEROCPY_READ)
    set(USE_FSAL_ICFS_ZEROCPY_READ ON)
  endif(NOT ICFS_FS_ICFS_ZEROCPY_READ)
endif (NOT ICFS_FS)

set(ICFSFS_LIBRARIES ${ICFSFS_LIBRARY})
message(STATUS "Found icfsfs libraries: ${ICFSFS_LIBRARIES}")

# handle the QUIETLY and REQUIRED arguments and set PRELUDE_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ICFSFS
  REQUIRED_VARS ICFSFS_INCLUDE_DIR ICFSFS_LIBRARY_DIR)
# VERSION FPHSA options not handled by CMake version < 2.8.2)
#                                  VERSION_VAR)
mark_as_advanced(ICFSFS_INCLUDE_DIR)
mark_as_advanced(ICFSFS_LIBRARY_DIR)
mark_as_advanced(USE_FSAL_ICFS_MKNOD)
mark_as_advanced(USE_FSAL_ICFS_SETLK)
mark_as_advanced(USE_FSAL_ICFS_LL_LOOKUP_ROOT)
mark_as_advanced(USE_FSAL_ICFS_STATX)
