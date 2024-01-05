#ifndef rs_car_lib_h
#define rs_car_lib_h

/* Warning, this file is autogenerated by cbindgen. Don't modify this manually. */

#include <stdint.h>

bool extract_all_car(const char *car_path, const char *output_path);

bool extract_file_car(const char *car_path,
                      const char *filter_file_pattern,
                      const char *output_path);

bool extract_verified_by_cid_from_car(const char *car_path,
                                      const char *cid,
                                      const char *output_path);

#endif /* rs_car_lib_h */