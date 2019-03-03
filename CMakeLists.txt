cmake_minimum_required(VERSION 3.10)
project(cs222_fall18_team_18)

set(CMAKE_CXX_STANDARD 11)

include_directories(rbf)



add_executable(cs222_fall18_team_18
        rbf/pfm.cc
        rbf/pfm.h
        rbf/rbfm.cc
        rbf/rbfm.h
        rbf/rbftest1.cc
        rbf/rbftest10.cc
        rbf/rbftest11.cc
        rbf/rbftest12.cc
        rbf/rbftest2.cc
        rbf/rbftest3.cc
        rbf/rbftest4.cc
        rbf/rbftest5.cc
        rbf/rbftest6.cc
        rbf/rbftest7.cc
        rbf/rbftest8.cc
        rbf/rbftest8b.cc
        rbf/rbftest9.cc
        rbf/rbftest_p0.cc
        rbf/rbftest_p1.cc
        rbf/rbftest_p1b.cc
        rbf/rbftest_p2.cc
        rbf/rbftest_p2b.cc
        rbf/rbftest_p3.cc
        rbf/rbftest_p4.cc
        rbf/rbftest_p5.cc
        rbf/rbftest_update.cc
        rbf/rbftest_delete.cc
        rbf/test_util.h
        rm/rm.h
        rm/rm.cc
        rm/rmtest_00.cc
        rm/rmtest_01.cc
        rm/rmtest_02.cc
        rm/rmtest_03.cc
        rm/rmtest_04.cc
        rm/rmtest_05.cc
        rm/rmtest_06.cc
        rm/rmtest_07.cc
        rm/rmtest_08.cc
        rm/rmtest_09.cc
        rm/rmtest_10.cc
        rm/rmtest_11.cc
        rm/rmtest_12.cc
        rm/rmtest_13.cc
        rm/rmtest_13b.cc
        rm/rmtest_14.cc
        rm/rmtest_15.cc
        rm/rmtest_create_tables.cc
        rm/rmtest_delete_tables.cc
        rm/rmtest_extra_1.cc
        rm/rmtest_extra_2.cc
)
