CXX = g++
CXXFLAGS = -O2 -std=c++17 -I./mystl

all: lab3/tokenizer.exe lab4/stemming.exe lab7/boolindex.exe lab8/boolsearch.exe

lab3/tokenizer.exe: lab3/tokenizer.cpp mystl/vector.hpp mystl/hashmap.hpp
	$(CXX) $(CXXFLAGS) $< -o $@

lab4/stemming.exe: lab4/stemming.cpp mystl/vector.hpp mystl/hashmap.hpp
	$(CXX) $(CXXFLAGS) $< -o $@

lab7/boolindex.exe: lab7/boolindex.cpp mystl/vector.hpp mystl/hashmap.hpp
	$(CXX) $(CXXFLAGS) $< -o $@

lab8/boolsearch.exe: lab8/boolsearch.cpp mystl/vector.hpp mystl/hashmap.hpp
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f lab3/tokenizer.exe lab4/stemming.exe lab7/boolindex.exe lab8/boolsearch.exe
