#!/bin/bash
set -ev

export GMX_DOUBLE=OFF
export GMX_MPI=${GMX_MPI:-OFF}
export GMX_THREAD_MPI=ON

export GMX_SRC_DIR=gromacs-$BRANCH

ccache -s

pushd $HOME
 [ -d $GMX_SRC_DIR ] || \
    git clone \
        --depth=1 \
        -b $BRANCH \
        https://gitlab.com/gromacs/gromacs.git \
        ${GMX_SRC_DIR}
 pushd ${GMX_SRC_DIR}
  pwd
  rm -rf build
  mkdir build
  pushd build
   which cmake
   cmake --version
   cmake -G Ninja \
         -DCMAKE_CXX_COMPILER=$CXX \
         -DGMX_ENABLE_CCACHE=ON \
         -DGMX_GIT_VERSION_INFO=OFF \
         -DCMAKE_C_COMPILER=$CC \
         -DGMX_DOUBLE=$GMX_DOUBLE \
         -DGMX_MPI=$GMX_MPI \
         -DGMX_THREAD_MPI=$GMX_THREAD_MPI \
         -DGMX_INSTALL_LEGACY_API=ON \
         -DCMAKE_INSTALL_PREFIX=$HOME/install/gromacs-$BRANCH \
         ..
   cmake --build . --target install
  popd
 popd
popd
ccache -s
