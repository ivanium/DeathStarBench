add_executable(
    Client
    Client.cpp
    ${THRIFT_GEN_CPP_DIR}/media_service_types.cpp
    ${THRIFT_GEN_CPP_DIR}/UserService.cpp
    ${THRIFT_GEN_CPP_DIR}/MovieIdService.cpp
    ${THRIFT_GEN_CPP_DIR}/UniqueIdService.cpp
    ${THRIFT_GEN_CPP_DIR}/TextService.cpp
    ${THRIFT_GEN_CPP_DIR}/PageService.cpp
)

target_include_directories(
    Client PRIVATE
    /usr/local/include/jaegertracing
)

target_link_libraries(
    Client
    nlohmann_json::nlohmann_json
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

install(TARGETS Client DESTINATION ./)