# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  CCLINK?= -ldl -lnsl -lsocket -lm -lpthread
  DEBUG?= -g -ggdb 
else
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  CCLINK?= -lm -pthread
  DEBUG?= -g -rdynamic -ggdb 
endif

ifeq ($(USE_TCMALLOC),yes)
  CCLINK+= -ltcmalloc
  CFLAGS+= -DUSE_TCMALLOC
endif
CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)

PREFIX= /usr/local
INSTALL_BIN= $(PREFIX)/bin
INSTALL= cp -p

OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o ziplist.o release.o networking.o util.o object.o db.o replication.o rdb.o t_string.o t_list.o t_set.o t_zset.o t_hash.o config.o aof.o vm.o pubsub.o multi.o debug.o sort.o intset.o syncio.o meshin.o
BENCHOBJ = ae.o anet.o redis-benchmark.o sds.o adlist.o zmalloc.o
CLIOBJ = anet.o sds.o adlist.o redis-cli.o zmalloc.o release.o
CHECKDUMPOBJ = redis-check-dump.o lzf_c.o lzf_d.o
CHECKAOFOBJ = redis-check-aof.o

PRGNAME = redis-server
BENCHPRGNAME = redis-benchmark
CLIPRGNAME = redis-cli
CHECKDUMPPRGNAME = redis-check-dump
CHECKAOFPRGNAME = redis-check-aof

all: dep redis-benchmark redis-cli redis-check-dump redis-check-aof redis-server
	@echo ""
	@echo "Hint: To run 'make test' is a good idea ;)"
	@echo ""


dependencies:
	cd ../deps/hiredis && $(MAKE) static ARCH="$(ARCH)"
	cd ../deps/linenoise && $(MAKE) ARCH="$(ARCH)"

redis-server: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ)

redis-benchmark: dependencies $(BENCHOBJ)
	cd ../deps/hiredis && $(MAKE) static
	$(CC) -o $(BENCHPRGNAME) $(CCOPT) $(DEBUG) $(BENCHOBJ) ../deps/hiredis/libhiredis.a

redis-benchmark.o:
	$(CC) -c $(CFLAGS) -I../deps/hiredis $(DEBUG) $(COMPILE_TIME) $<

redis-cli: dependencies $(CLIOBJ)
	$(CC) -o $(CLIPRGNAME) $(CCOPT) $(DEBUG) $(CLIOBJ) ../deps/hiredis/libhiredis.a ../deps/linenoise/linenoise.o

redis-cli.o:
	$(CC) -c $(CFLAGS) -I../deps/hiredis -I../deps/linenoise $(DEBUG) $(COMPILE_TIME) $<

redis-check-dump: $(CHECKDUMPOBJ)
	$(CC) -o $(CHECKDUMPPRGNAME) $(CCOPT) $(DEBUG) $(CHECKDUMPOBJ)

redis-check-aof: $(CHECKAOFOBJ)
	$(CC) -o $(CHECKAOFPRGNAME) $(CCOPT) $(DEBUG) $(CHECKAOFOBJ)

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(PRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) $(CHECKDUMPPRGNAME) $(CHECKAOFPRGNAME) *.o *.gcda *.gcno *.gcov

.depend dep:
	$(CC) -MM -I ../deps/hiredis -I ../deps/linenoise *.c > .depend

test: redis-server
	(cd ..; tclsh8.5 tests/test_helper.tcl --tags "${TAGS}" --file "${FILE}")

bench:
	./redis-benchmark

log:
	git log '--pretty=format:%ad %s (%cn)' --date=short > ../Changelog

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) ARCH="-m32"

gprof:
	$(MAKE) PROF="-pg"

gcov:
	$(MAKE) PROF="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION=""

32bitgprof:
	$(MAKE) PROF="-pg" ARCH="-arch i386"

install: all
	mkdir -p $(INSTALL_BIN)
	$(INSTALL) $(PRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(BENCHPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CLIPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CHECKDUMPPRGNAME) $(INSTALL_BIN)
	$(INSTALL) $(CHECKAOFPRGNAME) $(INSTALL_BIN)

# Deps (use make dep to generate this)
include .depend
