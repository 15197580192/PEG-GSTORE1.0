cmake_minimum_required(VERSION 3.6.2)
set (Tutorial_VERSION_MAJOR 1)
set (Tutorial_VERSION_MINOR 0)

project (PEG)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra")

# boost
# set(BOOST_ROOT /specific/boost/path)
find_package(Boost COMPONENTS system thread chrono date_time atomic REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

#crow and nt partition
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/partition)
#rapidjson
include_directories(${PROJECT_SOURCE_DIR}/rapidjson)

# antlr3
include_directories(${PROJECT_SOURCE_DIR}/queryDecompose/tools/libantlr3c-3.4)
include_directories(${PROJECT_SOURCE_DIR}/queryDecompose/tools/libantlr3c-3.4/include)
link_directories(${PROJECT_SOURCE_DIR}/queryDecompose/lib)


aux_source_directory(gStoreAPI gstoreapiSRC)
aux_source_directory(queryDecompose/Parser parserSRC)
aux_source_directory(queryDecompose/Query querySRC)
aux_source_directory(queryDecompose/Util utilSRC)
aux_source_directory(joiner joinerSRC)

# server
add_executable(PEG_Server PEG_Server.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Server pthread)
target_link_libraries(PEG_Server ${Boost_LIBRARIES})
target_link_libraries(PEG_Server curl)
target_link_libraries(PEG_Server libantlr.a)

# load
add_executable(PEG_Load PEG_Load.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Load pthread)
target_link_libraries(PEG_Load ${Boost_LIBRARIES})
target_link_libraries(PEG_Load curl)
target_link_libraries(PEG_Load libantlr.a)

# query
add_executable(PEG_Query PEG_Query.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Query pthread)
target_link_libraries(PEG_Query ${Boost_LIBRARIES})
target_link_libraries(PEG_Query curl)
target_link_libraries(PEG_Query libantlr.a)

# addEdge
add_executable(PEG_AddEdge PEG_AddEdge.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_AddEdge pthread)
target_link_libraries(PEG_AddEdge ${Boost_LIBRARIES})
target_link_libraries(PEG_AddEdge curl)
target_link_libraries(PEG_AddEdge libantlr.a)

# removeEdge
add_executable(PEG_RemoveEdge PEG_RemoveEdge.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_RemoveEdge pthread)
target_link_libraries(PEG_RemoveEdge ${Boost_LIBRARIES})
target_link_libraries(PEG_RemoveEdge curl)
target_link_libraries(PEG_RemoveEdge libantlr.a)

# queryNeighbor
add_executable(PEG_Neighbor PEG_Neighbor.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Neighbor pthread)
target_link_libraries(PEG_Neighbor ${Boost_LIBRARIES})
target_link_libraries(PEG_Neighbor curl)
target_link_libraries(PEG_Neighbor libantlr.a)

# khop
add_executable(PEG_Khop PEG_Khop.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Khop pthread)
target_link_libraries(PEG_Khop ${Boost_LIBRARIES})
target_link_libraries(PEG_Khop curl)
target_link_libraries(PEG_Khop libantlr.a)

# closenesscentrality
add_executable(PEG_ClosenessCentrality PEG_ClosenessCentrality.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_ClosenessCentrality pthread)
target_link_libraries(PEG_ClosenessCentrality ${Boost_LIBRARIES})
target_link_libraries(PEG_ClosenessCentrality curl)
target_link_libraries(PEG_ClosenessCentrality libantlr.a)

# shortestpath
add_executable(PEG_ShortestPath PEG_ShortestPath.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_ShortestPath pthread)
target_link_libraries(PEG_ShortestPath ${Boost_LIBRARIES})
target_link_libraries(PEG_ShortestPath curl)
target_link_libraries(PEG_ShortestPath libantlr.a)

# triangle
add_executable(PEG_Triangle PEG_Triangle.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Triangle pthread)
target_link_libraries(PEG_Triangle ${Boost_LIBRARIES})
target_link_libraries(PEG_Triangle curl)
target_link_libraries(PEG_Triangle libantlr.a)

# triangle1
add_executable(PEG_Triangle1 PEG_Triangle1.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Triangle1 pthread)
target_link_libraries(PEG_Triangle1 ${Boost_LIBRARIES})
target_link_libraries(PEG_Triangle1 curl)
target_link_libraries(PEG_Triangle1 libantlr.a)

# triangle2
add_executable(PEG_Triangle2 PEG_Triangle2.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Triangle2 pthread)
target_link_libraries(PEG_Triangle2 ${Boost_LIBRARIES})
target_link_libraries(PEG_Triangle2 curl)
target_link_libraries(PEG_Triangle2 libantlr.a)

# ssppr
add_executable(PEG_SSPPR PEG_SSPPR.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_SSPPR pthread)
target_link_libraries(PEG_SSPPR ${Boost_LIBRARIES})
target_link_libraries(PEG_SSPPR curl)
target_link_libraries(PEG_SSPPR libantlr.a)

# allentity
add_executable(PEG_AllEntity PEG_AllEntity.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_AllEntity pthread)
target_link_libraries(PEG_AllEntity ${Boost_LIBRARIES})
target_link_libraries(PEG_AllEntity curl)
target_link_libraries(PEG_AllEntity libantlr.a)

# delete
add_executable(PEG_Delete PEG_Delete.cpp ${gstoreapiSRC} ${parserSRC} ${querySRC} ${utilSRC} ${joinerSRC})
target_link_libraries(PEG_Delete pthread)
target_link_libraries(PEG_Delete ${Boost_LIBRARIES})
target_link_libraries(PEG_Delete curl)
target_link_libraries(PEG_Delete libantlr.a)

add_executable(testjson testjson.cpp)
