.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o
STATIC_LIB:=$(LIBS)libbpt.a

# Include more files if you write another source file.
SRCS_FOR_LIB:= \
	$(SRCDIR)disk_bpt.c \
	$(SRCDIR)file.c \
	$(SRCDIR)interface.c

OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(STATIC_LIB)
	$(CC) $(CFLAGS) $< -o $@ -L $(LIBS) -lbpt

%.o: %.c
	$(CC) $(CFLAGS) $^ -c -o $@

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

$(STATIC_LIB): $(OBJS_FOR_LIB)
	ar cr $@ $^

#library:
#	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

#static_library:
#	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
