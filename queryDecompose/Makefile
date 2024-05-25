#help for make
#http://www.cnblogs.com/wang_yb/p/3990952.html
#https://segmentfault.com/a/1190000000349917
#http://blog.csdn.net/cuiyifang/article/details/7910268

#to use gprof to analyse efficience of the program:
#http://blog.chinaunix.net/uid-25194149-id-3215487.html
#to use doxygen+graphviz+htmlhelp to generate document from source code:
#http://www.doxygen.nl/
#(also include good comments norm)
#http://blog.csdn.net/u010740725/article/details/51387810

#TODO:the dependences are not complete!

#compile parameters

CC = g++
MPICC = g++

#the optimazition level of gcc/g++
#http://blog.csdn.net/hit_090420216/article/details/44900215
#NOTICE: -O2 is recommended, while -O3 is dangerous
#when developing, not use -O because it will disturb the normal 
#routine. use it for test and release.
CFLAGS = -c -Wall -g #-pg #-O2
EXEFLAG = -g #-pg #-O2

# paths

objdir = .objs/

exedir = bin/

lib_antlr = lib/libantlr.a

api_cpp = api/cpp/lib/libgstoreconnector.a

api_java = api/java/lib/GstoreJavaAPI.jar

# objects

utilobj = $(objdir)Util.o 

queryobj = $(objdir)Varset.o $(objdir)QueryTree.o  $(objdir)GeneralEvaluation.o

parserobj = $(objdir)SparqlParser.o \
			$(objdir)SparqlLexer.o $(objdir)QueryParser.o


objfile = $(parserobj) $(utilobj) $(queryobj) 


inc = -I./tools/libantlr3c-3.4/ -I./tools/libantlr3c-3.4/include 

#add -lreadline -ltermcap if using readline or objs contain readline
library = -ltermcap -lreadline -L./lib -lantlr 
def64IO = -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE

#gtest

all: $(exedir)gqueryDis

test_index: test_index.cpp
	$(CC) $(EXEFLAG) -o test_index test_index.cpp $(objfile) $(library)

#executables begin

$(exedir)gqueryDis: $(lib_antlr) $(objdir)gqueryDis.o $(objfile)
	$(MPICC) $(EXEFLAG) -o $(exedir)gqueryDis $(objdir)gqueryDis.o $(objfile) $(library)

#executables end


#objects in Main/ begin

$(objdir)gqueryDis.o: Main/gqueryDis.cpp Util/Util.h
	$(MPICC) $(CFLAGS) Main/gqueryDis.cpp $(inc) -o $(objdir)gqueryDis.o  #-DREADLINE_ON
	#add -DREADLINE_ON if using readline


#objects in Main/ end



#objects in Query/ begin

$(objdir)Varset.o: Query/Varset.cpp Query/Varset.h
	$(CC) $(CFLAGS) Query/Varset.cpp $(inc) -o $(objdir)Varset.o

$(objdir)QueryTree.o: Query/QueryTree.cpp Query/QueryTree.h $(objdir)Varset.o
	$(CC) $(CFLAGS) Query/QueryTree.cpp $(inc) -o $(objdir)QueryTree.o

#no more using $(objdir)Database.o
$(objdir)GeneralEvaluation.o: Query/GeneralEvaluation.cpp Query/GeneralEvaluation.h $(objdir)QueryParser.o $(objdir)QueryTree.o \
	$(objdir)Varset.o 
	$(CC) $(CFLAGS) Query/GeneralEvaluation.cpp $(inc) -o $(objdir)GeneralEvaluation.o

#objects in Query/ end



#objects in Util/ begin

$(objdir)Util.o:  Util/Util.cpp Util/Util.h
	$(CC) $(CFLAGS) Util/Util.cpp -o $(objdir)Util.o

#objects in util/ end


#objects in Parser/ begin

$(objdir)SparqlParser.o: Parser/SparqlParser.c Parser/SparqlParser.h
	gcc  $(CFLAGS) Parser/SparqlParser.c $(inc) -o $(objdir)SparqlParser.o

$(objdir)SparqlLexer.o: Parser/SparqlLexer.c Parser/SparqlLexer.h
	gcc  $(CFLAGS) Parser/SparqlLexer.c $(inc) -o $(objdir)SparqlLexer.o

$(objdir)QueryParser.o: Parser/QueryParser.cpp Parser/QueryParser.h $(objdir)SparqlParser.o $(objdir)SparqlLexer.o $(objdir)QueryTree.o
	$(CC) $(CFLAGS) Parser/QueryParser.cpp $(inc) -o $(objdir)QueryParser.o

#objects in Parser/ end



$(lib_antlr):
	rm -rf tools/libantlr3c-3.4/
	cd tools; tar -xzvf libantlr3c-3.4.tar.gz;
	cd tools; cd libantlr3c-3.4/; ./configure -enable-64bit; make;
	rm -rf lib/libantlr.a
	ar -crv lib/libantlr.a tools/libantlr3c-3.4/*.o 
	#NOTICE: update the sparql.tar.gz if Sparql* in Parser are changed manually
	rm -rf Parser/Sparql*
	cd tools; tar -xzvf sparql.tar.gz; mv Sparql* ../Parser/;

$(api_cpp): $(objdir)Socket.o
	$(MAKE) -C api/cpp/src 

$(api_java):
	$(MAKE) -C api/java/src

.PHONY: clean dist tarball api_example gtest sumlines

clean:
	rm -rf $(exedir)g* $(objdir)*.o
	#rm -rf .project .cproject .settings   just for eclipse
	#rm -rf cscope* just for vim

dist: clean
	rm -rf *.nt *.n3 .debug/*.log .tmp/*.dat *.txt *.db
	rm -rf tools/libantlr3c-3.4 lib/libantlr.a Parser/Sparql*
	rm -rf cscope* .cproject .settings tags
