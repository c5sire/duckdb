if(${BUILD_ICU_EXTENSION})
  add_subdirectory(icu)
endif()

if(${BUILD_PARQUET_EXTENSION})
  add_subdirectory(parquet)
endif()

if(${BUILD_TPCH_EXTENSION})
  add_subdirectory(tpch)
endif()

if(${BUILD_FTS_EXTENSION})
  add_subdirectory(fts)
endif()

if(${BUILD_HTTPFS_EXTENSION})
  add_subdirectory(httpfs)
endif()
