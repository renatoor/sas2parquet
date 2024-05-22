find_package(Iconv)

add_library(readstat
  CKHashTable.c
  CMakeLists.txt
  readstat_bits.c
  readstat_convert.c
  readstat_error.c
  readstat_io_unistd.c
  readstat_malloc.c
  readstat_metadata.c
  readstat_parser.c
  readstat_value.c
  readstat_variable.c
  readstat_writer.c
  sas/readstat_sas.c
  sas/readstat_sas7bdat_read.c
  sas/readstat_sas_rle.c)

target_link_libraries(readstat INTERFACE Iconv::Iconv)
