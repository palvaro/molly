package edu.berkeley.cs.boom.molly.wrappers;


import jnr.ffi.Pointer;

public interface C4 {
  void c4_initialize();

  Pointer c4_make(Pointer p, int port);

  int c4_install_file(Pointer c4, String file);

  int c4_install_str(Pointer c4, String str);

  String c4_dump_table(Pointer c4, String table);

  void c4_destroy(Pointer c4);

  void c4_terminate();
}
