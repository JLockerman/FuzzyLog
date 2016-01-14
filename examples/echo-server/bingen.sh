#! /bin/sh

#rte common
#~/bindgen -msse4.2 -mavx -I /usr/lib/clang/3.5.1/include -I $RTE_SDK/$RTE_TARGET/include -include $RTE_SDK/$RTE_TARGET/include/rte_config.h -match $filename.h $RTE_SDK/$RTE_TARGET/include/$1.h
 
~/bindgen -msse4.2 -mavx -I /usr/lib/clang/3.5.1/include -I $RTE_SDK/$RTE_TARGET/include -include $RTE_SDK/$RTE_TARGET/include/rte_config.h -match $filename.h $RTE_SDK/$RTE_TARGET/include/$1.h